import Foundation
import CouchbaseLiteSwift

/// Manages continuous replication to Couchbase Capella App Services.
class ReplicatorManager: ObservableObject {
    static let shared = ReplicatorManager()

    private let appServicesWSS = "wss://lcqfknrvnr1vpm5x.apps.cloud.couchbase.com:4984/brewsync"
    private let appServicesHTTPS = "https://lcqfknrvnr1vpm5x.apps.cloud.couchbase.com:4984/brewsync"
    private let oidcProviderName = "django"

    private var replicator: Replicator?
    private var listenerToken: ListenerToken?
    private var docListenerToken: ListenerToken?
    /// Tracks whether the currently running replicator was started with a
    /// session (authenticated) vs as guest. The per-doc listener uses this
    /// to decide whether a push rejection is a legitimate orphan (purge) or
    /// an auth-misconfiguration (keep, log loudly).
    private var startedAuthenticated: Bool = false

    @Published var lastPushError: String?

    @Published var status: ReplicatorStatus = .stopped
    @Published var isConnected: Bool = false

    enum ReplicatorStatus: String {
        case stopped = "Stopped"
        case connecting = "Connecting..."
        case connected = "Connected"
        case offline = "Offline"
        case error = "Error"
    }

    private init() {}

    /// Get the OIDC login URL from App Services (for use in ASWebAuthenticationSession)
    var oidcLoginURL: URL? {
        URL(string: "\(appServicesHTTPS)/_oidc?provider=\(oidcProviderName)&offline=true")
    }

    /// Start replication.
    /// - Pass an `idToken` (Django OIDC JWT) to sync as an authenticated user
    ///   (push + pull). The token is sent via `Authorization: Bearer <jwt>`
    ///   on the WebSocket upgrade. App Services validates the JWT against
    ///   Django's JWKS on every connection — no `_session` exchange needed.
    /// - Pass `nil` to sync as the GUEST user (pull-only, anonymous browsing).
    func start(idToken: String? = nil) {
        // Stop any existing replicator before starting a new one so we can
        // cleanly switch between guest and authenticated modes.
        stop()

        guard DatabaseManager.shared.database != nil else {
            print("[Replicator] Database not initialized")
            return
        }

        guard let url = URL(string: appServicesWSS) else { return }

        let token = idToken
        let hasToken = !(token ?? "").isEmpty

        let endpoint = URLEndpoint(url: url)
        var collections: [Collection] = []
        if let c = DatabaseManager.shared.beerCollection { collections.append(c) }
        if let c = DatabaseManager.shared.breweryCollection { collections.append(c) }
        if let c = DatabaseManager.shared.ratingCollection { collections.append(c) }
        if let c = DatabaseManager.shared.blogPageCollection { collections.append(c) }
        print("[Replicator] Syncing \(collections.count) collections")

        var config = ReplicatorConfiguration(target: endpoint)
        // Guest can only pull; authenticated users push + pull.
        config.replicatorType = hasToken ? .pushAndPull : .pull
        config.continuous = true
        config.addCollections(collections)
        startedAuthenticated = hasToken
        if hasToken, let token = token {
            // Bearer JWT auth on the WebSocket upgrade. App Services validates
            // it against Django's JWKS. This is the *same* mechanism every
            // REST endpoint accepts — see the iOS test writes earlier.
            config.headers = ["Authorization": "Bearer \(token)"]
            print("[Replicator] >>> START mode=PUSH+PULL (authenticated), idToken=\(token.prefix(12))...\(token.suffix(8)) len=\(token.count)")
        } else {
            print("[Replicator] >>> START mode=PULL-ONLY (guest, no token)")
        }
        print("[Replicator] Collections: \(collections.map { $0.name })")

        replicator = Replicator(config: config)

        listenerToken = replicator?.addChangeListener { [weak self] change in
            DispatchQueue.main.async {
                switch change.status.activity {
                case .stopped:
                    self?.status = .stopped
                    self?.isConnected = false
                case .idle:
                    self?.status = .connected
                    self?.isConnected = true
                    let beers = DatabaseManager.shared.getAllBeers().count
                    let breweries = DatabaseManager.shared.getAllBreweries().count
                    let blogs = DatabaseManager.shared.getAllBlogPosts().count
                    // Count wagtailcore_page docs
                    var pageCount = 0
                    if let col = DatabaseManager.shared.wagtailPageCollection {
                        let q = QueryBuilder.select(SelectResult.expression(Meta.id)).from(DataSource.collection(col))
                        pageCount = (try? q.execute().allResults().count) ?? 0
                    }
                    print("[Replicator] Idle — synced \(change.status.progress.completed) docs | Local: \(beers) beers, \(breweries) breweries, \(blogs) blogs, \(pageCount) pages")
                case .busy:
                    self?.status = .connected
                    self?.isConnected = true
                    print("[Replicator] Busy — \(change.status.progress.completed)/\(change.status.progress.total)")
                case .connecting:
                    self?.status = .connecting
                    self?.isConnected = false
                case .offline:
                    self?.status = .offline
                    self?.isConnected = false
                @unknown default:
                    break
                }

                if let error = change.status.error {
                    print("[Replicator] Error: \(error)")
                    self?.status = .error
                }
            }
        }

        // Per-document push-failure handler. Only purges in cases where the
        // doc is genuinely an orphan that the current auth context can never
        // push. If we're authenticated and the server still rejects, that's
        // an auth bug — DO NOT purge (we'd lose the user's rating). Surface
        // it loudly so we notice instead of silently retrying forever.
        docListenerToken = replicator?.addDocumentReplicationListener { [weak self] replication in
            guard let self = self, replication.isPush else { return }
            for doc in replication.documents {
                guard let error = doc.error as NSError? else { continue }
                let msg = error.localizedDescription
                let isReadOnly = msg.contains("read-only") || msg.contains("read only")
                let isWrongUser = msg.contains("wrong user") || msg.contains("missing role")

                if !self.startedAuthenticated && isReadOnly {
                    // Guest mode replicator picked up a stray local write.
                    // Safe to purge — guest can never push anything.
                    print("[Replicator] Orphan in guest mode, purging \(doc.id): \(msg)")
                    self.purgeLocalDoc(id: doc.id, scope: doc.scope, collectionName: doc.collection)
                } else if self.startedAuthenticated && isWrongUser {
                    // Authenticated, but the doc belongs to a different user.
                    // The current user can never push it. Purge.
                    print("[Replicator] Cross-user orphan, purging \(doc.id): \(msg)")
                    self.purgeLocalDoc(id: doc.id, scope: doc.scope, collectionName: doc.collection)
                } else if self.startedAuthenticated && isReadOnly {
                    // We THINK we're authed but the server says we're not.
                    // This is the bug we need to surface — the session cookie
                    // isn't being honored. DO NOT purge; the user's rating is
                    // legitimate. Tell whoever is listening.
                    print("[Replicator] ⚠️ AUTH GLITCH: replicator started authenticated but server returned read-only for \(doc.id). The session cookie is stale or malformed. Doc kept locally.")
                    DispatchQueue.main.async { self.lastPushError = "Session expired — please log out and back in" }
                } else {
                    // Some other push failure. Log but don't purge.
                    print("[Replicator] Push failed for \(doc.id) (kept locally): \(msg)")
                    DispatchQueue.main.async { self.lastPushError = msg }
                }
            }
        }

        replicator?.start()
        status = .connecting
    }

    /// Purge a single doc from its local collection by id. Used by the per-doc
    /// replication listener when the server permanently rejects a push, so we
    /// don't accumulate orphan local writes that retry forever.
    private func purgeLocalDoc(id: String, scope: String, collectionName: String) {
        guard let db = DatabaseManager.shared.database else { return }
        do {
            guard let collection = try db.collection(name: collectionName, scope: scope) else { return }
            if let doc = try collection.document(id: id) {
                try collection.purge(document: doc)
            }
        } catch {
            print("[Replicator] Failed to purge \(id): \(error)")
        }
    }

    func stop() {
        if let token = listenerToken {
            token.remove()
            listenerToken = nil
        }
        if let token = docListenerToken {
            token.remove()
            docListenerToken = nil
        }
        replicator?.stop()
        replicator = nil
        status = .stopped
        isConnected = false
    }
}
