import SwiftUI
import CouchbaseLiteSwift

struct ContentView: View {
    @ObservedObject var auth = AuthManager.shared
    @ObservedObject var replicator = ReplicatorManager.shared
    @State private var dbReady = false

    var body: some View {
        ZStack {
            // Browsing is available to all users — no auth gate
            if dbReady {
                TabView {
                    BeerListView()
                        .tabItem {
                            Label("Beers", systemImage: "mug.fill")
                        }
                    BreweryListView()
                        .tabItem {
                            Label("Breweries", systemImage: "building.2")
                        }
                    BlogView()
                        .tabItem {
                            Label("Blog", systemImage: "doc.richtext")
                        }
                }
                .tint(Theme.accent)
            }

            // Show sync status while connecting (both guest and authenticated)
            if !replicator.isConnected && replicator.status != .stopped {
                VStack {
                    HStack(spacing: 6) {
                        Image(systemName: "wifi.slash")
                            .font(.caption2)
                        Text(replicator.status.rawValue)
                            .font(.caption2)
                    }
                    .padding(.horizontal, 12)
                    .padding(.vertical, 6)
                    .background(Theme.card)
                    .foregroundColor(Theme.textMuted)
                    .cornerRadius(20)
                    .overlay(RoundedRectangle(cornerRadius: 20).stroke(Theme.border, lineWidth: 1))
                    Spacer()
                }
                .padding(.top, 4)
            }
        }
        .preferredColorScheme(.dark)
        .onAppear {
            Database.log.console.domains = .all
            Database.log.console.level = .warning
            try? DatabaseManager.shared.initialize()
            dbReady = true
        }
        .task {
            // Always sync via App Services — guest (pull-only) when logged
            // out, authenticated (push+pull) once the user signs in.
            await startReplicator()
        }
        .onChange(of: auth.isAuthenticated) { _ in
            // Restart the replicator with the new auth context. The replicator's
            // per-doc listener (in ReplicatorManager) will purge any local doc
            // the new context can't push, so we don't need to wipe the DB.
            Task { await startReplicator() }
        }
    }

    private func startReplicator() async {
        if auth.isAuthenticated {
            if let token = await AuthManager.shared.currentIdToken() {
                ReplicatorManager.shared.start(idToken: token)
                return
            }
            print("[App] No usable id_token, falling back to guest sync")
            AuthManager.shared.logout()
        }
        ReplicatorManager.shared.start(idToken: nil)
    }
}
