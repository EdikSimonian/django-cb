import AuthenticationServices
import Foundation

/// Handles Google Sign-In using ASWebAuthenticationSession (no Google SDK dependency).
/// Uses the iOS OAuth client ID (public client, no secret needed).
@MainActor
class GoogleSignInHelper: NSObject, ASWebAuthenticationPresentationContextProviding {

    private static let clientId = "247166506991-9lht6504qb2vva7knscevtp6gb21pe1s.apps.googleusercontent.com"
    private static let googleAuthURL = "https://accounts.google.com/o/oauth2/v2/auth"
    private static let googleTokenURL = "https://oauth2.googleapis.com/token"
    // Redirect URI: reversed client ID as scheme (standard Google convention for iOS)
    private static let redirectScheme = "com.googleusercontent.apps.247166506991-9lht6504qb2vva7knscevtp6gb21pe1s"
    private static let redirectURI = "\(redirectScheme):/oauth2redirect"

    func signIn() async throws -> String {
        let codeVerifier = PKCE.generateCodeVerifier()
        let codeChallenge = PKCE.generateCodeChallenge(from: codeVerifier)

        var components = URLComponents(string: Self.googleAuthURL)!
        components.queryItems = [
            URLQueryItem(name: "client_id", value: Self.clientId),
            URLQueryItem(name: "redirect_uri", value: Self.redirectURI),
            URLQueryItem(name: "response_type", value: "code"),
            URLQueryItem(name: "scope", value: "openid email profile"),
            URLQueryItem(name: "code_challenge", value: codeChallenge),
            URLQueryItem(name: "code_challenge_method", value: "S256"),
        ]

        guard let authURL = components.url else {
            throw AuthError.networkError
        }

        let callbackURL = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<URL, Error>) in
            let session = ASWebAuthenticationSession(
                url: authURL,
                callbackURLScheme: Self.redirectScheme
            ) { url, error in
                if let error = error {
                    continuation.resume(throwing: error)
                } else if let url = url {
                    continuation.resume(returning: url)
                } else {
                    continuation.resume(throwing: AuthError.cancelled)
                }
            }
            session.prefersEphemeralWebBrowserSession = false
            session.presentationContextProvider = self
            session.start()
        }

        guard let code = URLComponents(url: callbackURL, resolvingAgainstBaseURL: false)?
            .queryItems?.first(where: { $0.name == "code" })?.value else {
            throw AuthError.noCode
        }

        let idToken = try await exchangeGoogleCode(code: code, codeVerifier: codeVerifier)
        return idToken
    }

    private func exchangeGoogleCode(code: String, codeVerifier: String) async throws -> String {
        let url = URL(string: Self.googleTokenURL)!
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")

        // iOS client is a public client — no client_secret needed
        let body = [
            "code=\(code)",
            "client_id=\(Self.clientId)",
            "redirect_uri=\(Self.redirectURI)",
            "grant_type=authorization_code",
            "code_verifier=\(codeVerifier)",
        ].joined(separator: "&")
        request.httpBody = body.data(using: .utf8)

        let (data, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode == 200 else {
            let responseBody = String(data: data, encoding: .utf8) ?? ""
            print("[Google] Token exchange failed: \(responseBody)")
            throw AuthError.tokenExchangeFailed
        }

        guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let idToken = json["id_token"] as? String else {
            throw AuthError.tokenExchangeFailed
        }

        return idToken
    }

    // MARK: - ASWebAuthenticationPresentationContextProviding

    func presentationAnchor(for session: ASWebAuthenticationSession) -> ASPresentationAnchor {
        ASPresentationAnchor()
    }
}
