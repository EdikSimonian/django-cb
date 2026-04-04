import SwiftUI

struct UserMenuView: View {
    @ObservedObject var auth = AuthManager.shared

    var body: some View {
        Menu {
            Text("Signed in as \(auth.username)")
            if auth.isAdmin {
                Label("Admin", systemImage: "shield.checkered")
            }
            Divider()
            Button("Reset Sync Data") {
                DatabaseManager.shared.deleteAndReset()
                try? DatabaseManager.shared.initialize()
                Task {
                    ReplicatorManager.shared.stop()
                    if let s = await auth.refreshSession() {
                        ReplicatorManager.shared.start(sessionID: s)
                    }
                }
            }
            Button("Sign Out", role: .destructive) {
                auth.logout()
            }
        } label: {
            Image(systemName: "person.circle")
                .font(.title3)
                .foregroundColor(Theme.textMuted)
        }
    }
}
