import Foundation
import SwiftUI

/// Manages authentication state. The actual OIDC flow is handled by OIDCWebView.
@MainActor
class AuthManager: ObservableObject {
    static let shared = AuthManager()

    private let djangoURL = "https://django-couchbase-orm-production.up.railway.app"

    @Published var isAuthenticated = false
    @Published var username: String = ""
    @Published var userId: Int = 0
    @Published var isAdmin: Bool = false
    @Published var error: String?

    init() {
        // Restore session from keychain
        if KeychainHelper.load(key: "sync_session") != nil,
           let name = KeychainHelper.load(key: "username"), !name.isEmpty {
            self.isAuthenticated = true
            self.username = name
            self.userId = Int(KeychainHelper.load(key: "user_id") ?? "0") ?? 0
            self.isAdmin = KeychainHelper.load(key: "groups")?.contains("admin") ?? false
        }
    }

    // MARK: - Handle session from OIDCWebView

    func handleSession(sessionID: String, username: String) {
        KeychainHelper.save(key: "sync_session", value: sessionID)
        KeychainHelper.save(key: "username", value: username)
        self.username = username
        self.isAuthenticated = true
        self.error = nil
    }

    // MARK: - Parse ID token for admin status

    func parseIdToken(_ token: String) {
        let parts = token.split(separator: ".")
        guard parts.count >= 2 else { return }
        var payload = String(parts[1])
        while payload.count % 4 != 0 { payload += "=" }
        let base64 = payload
            .replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
        guard let data = Data(base64Encoded: base64),
              let claims = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else { return }

        if let groups = claims["groups"] as? [String] {
            isAdmin = groups.contains("admin")
            KeychainHelper.save(key: "groups", value: groups.joined(separator: ","))
        }
        if let preferredUsername = claims["preferred_username"] as? String {
            username = preferredUsername
            KeychainHelper.save(key: "username", value: preferredUsername)
        }
    }

    // MARK: - Registration

    func register(username: String, email: String, password: String) async throws {
        let url = URL(string: "\(djangoURL)/api/auth/register/")!
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode([
            "username": username,
            "email": email,
            "password": password,
        ])

        let (data, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode == 201 else {
            let body = String(data: data, encoding: .utf8) ?? "Registration failed"
            throw NSError(domain: "Auth", code: 0, userInfo: [NSLocalizedDescriptionKey: body])
        }
    }

    // MARK: - Logout

    func logout() {
        KeychainHelper.clearAll()
        KeychainHelper.delete(key: "sync_session")
        isAuthenticated = false
        username = ""
        userId = 0
        isAdmin = false
    }
}
