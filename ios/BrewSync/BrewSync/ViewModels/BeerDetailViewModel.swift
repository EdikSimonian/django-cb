import Foundation

/// Manages beer detail data and user rating.
@MainActor
class BeerDetailViewModel: ObservableObject {
    @Published var beer: Beer
    @Published var brewery: Brewery?
    @Published var userRating: Int = 0
    @Published var ratings: [Rating] = []

    private let auth = AuthManager.shared
    private var refreshTimer: Timer?

    init(beer: Beer) {
        self.beer = beer
    }

    func startRefreshing() {
        load()
        refreshTimer = Timer.scheduledTimer(withTimeInterval: 3, repeats: true) { [weak self] _ in
            DispatchQueue.main.async { self?.load() }
        }
    }

    func stopRefreshing() {
        refreshTimer?.invalidate()
        refreshTimer = nil
    }

    func load() {
        // Reload beer from local DB (may have been updated by sync)
        if let updated = DatabaseManager.shared.getBeer(id: beer.id) {
            beer = updated
        }

        // Load brewery
        if let breweryId = beer.breweryId {
            brewery = DatabaseManager.shared.getBrewery(id: breweryId)
        }

        // Load ratings
        ratings = DatabaseManager.shared.getRatings(forBeer: beer.id)

        // Always recompute avg/count from the local ratings we actually have,
        // ignoring whatever stale value is on the beer doc. The server-side
        // avg_rating is only updated by a backend recompute job, so trusting
        // it here would clobber the user's optimistic update on every refresh
        // tick — causing the visible "flash back to old value" race.
        if !ratings.isEmpty {
            let total = ratings.reduce(0) { $0 + $1.score }
            beer.avgRating = Double(total) / Double(ratings.count)
            beer.ratingCount = ratings.count
        } else {
            beer.avgRating = 0
            beer.ratingCount = 0
        }

        // Load current user's rating
        if auth.isAuthenticated, !auth.username.isEmpty {
            if let existing = DatabaseManager.shared.getUserRating(
                beerId: beer.id, username: auth.username
            ) {
                userRating = existing.score
            }
        }
    }

    func submitRating(score: Int) {
        guard auth.isAuthenticated, !auth.username.isEmpty else { return }

        let rating = Rating(
            id: Rating.documentId(beerId: beer.id, username: auth.username),
            beerId: beer.id,
            userId: 0,
            username: auth.username,
            score: score
        )

        do {
            try DatabaseManager.shared.saveRating(rating)
            userRating = score

            // Recompute local avg for display only (don't save beer doc —
            // non-admin users can't write to beers_beer collection)
            let allRatings = DatabaseManager.shared.getRatings(forBeer: beer.id)
            let total = allRatings.reduce(0) { $0 + $1.score }
            let count = allRatings.count
            beer.avgRating = count > 0 ? Double(total) / Double(count) : 0
            beer.ratingCount = count
            ratings = allRatings
        } catch {
            print("[Rating] Save error: \(error)")
        }
    }

    func deleteBeer() throws {
        try DatabaseManager.shared.deleteBeer(id: beer.id)
    }
}
