import SwiftUI

struct UserMenuView: View {
    @ObservedObject var auth = AuthManager.shared
    @State private var showDeleteConfirmation = false
    @State private var showLogin = false

    var body: some View {
        Group {
            content
        }
        // Lives in both branches so the auth flip always fires here, even
        // when the unauthenticated branch's Button isn't in the view tree.
        .onChange(of: auth.isAuthenticated) { _ in
            showLogin = false
        }
    }

    @ViewBuilder
    private var content: some View {
        if auth.isAuthenticated {
            Menu {
                Text("Signed in as \(auth.displayName.isEmpty ? auth.username : auth.displayName)")
                if auth.isAdmin {
                    Label("Admin", systemImage: "shield.checkered")
                }
                Divider()
                Button("Reset Sync Data") {
                    DatabaseManager.shared.deleteAndReset()
                    try? DatabaseManager.shared.initialize()
                    Task {
                        ReplicatorManager.shared.stop()
                        let token = await auth.currentIdToken()
                        ReplicatorManager.shared.start(idToken: token)
                    }
                }
                Button("Sign Out", role: .destructive) {
                    auth.logout()
                }
                Divider()
                Button("Delete Account", role: .destructive) {
                    showDeleteConfirmation = true
                }
            } label: {
                Image(systemName: "person.circle")
                    .font(.title3)
                    .foregroundColor(Theme.textMuted)
            }
            .alert("Delete Account?", isPresented: $showDeleteConfirmation) {
                Button("Cancel", role: .cancel) { }
                Button("Delete", role: .destructive) {
                    Task {
                        // Order matters: tear down the replicator and reset the
                        // local DB BEFORE flipping auth. If we logged out first,
                        // ContentView's isAuthenticated onChange would restart
                        // the guest replicator on the DB we're about to wipe.
                        ReplicatorManager.shared.stop()
                        do {
                            try await auth.deleteAccount()
                        } catch {
                            print("[Auth] Delete account error: \(error)")
                        }
                        DatabaseManager.shared.deleteAndReset()
                        try? DatabaseManager.shared.initialize()
                        auth.logout()
                    }
                }
            } message: {
                Text("This will permanently delete your account, ratings, and all associated data. This cannot be undone.")
            }
        } else {
            // Unauthenticated: show sign-in entry point
            Button {
                showLogin = true
            } label: {
                Image(systemName: "person.circle")
                    .font(.title3)
                    .foregroundColor(Theme.textMuted)
            }
            .sheet(isPresented: $showLogin) {
                LoginView()
            }
        }
    }
}
