import AuthenticationServices
import Foundation
import UIKit

/// Handles Sign in with Apple credential flow and delegates back to AuthManager.
class AppleSignInCoordinator: NSObject, ASAuthorizationControllerDelegate, ASAuthorizationControllerPresentationContextProviding {

    private var continuation: CheckedContinuation<(idToken: String, fullName: String?, authorizationCode: String?), Error>?

    func signIn() async throws -> (idToken: String, fullName: String?, authorizationCode: String?) {
        return try await withCheckedThrowingContinuation { continuation in
            self.continuation = continuation

            let provider = ASAuthorizationAppleIDProvider()
            let request = provider.createRequest()
            request.requestedScopes = [.fullName, .email]

            let controller = ASAuthorizationController(authorizationRequests: [request])
            controller.delegate = self
            controller.presentationContextProvider = self
            controller.performRequests()
        }
    }

    // MARK: - ASAuthorizationControllerDelegate

    func authorizationController(controller: ASAuthorizationController, didCompleteWithAuthorization authorization: ASAuthorization) {
        guard let credential = authorization.credential as? ASAuthorizationAppleIDCredential,
              let tokenData = credential.identityToken,
              let idToken = String(data: tokenData, encoding: .utf8) else {
            continuation?.resume(throwing: AuthError.tokenExchangeFailed)
            continuation = nil
            return
        }

        var fullName: String?
        if let nameComponents = credential.fullName {
            let parts = [nameComponents.givenName, nameComponents.familyName].compactMap { $0 }
            if !parts.isEmpty {
                fullName = parts.joined(separator: " ")
            }
        }

        // authorizationCode is needed server-side to exchange for Apple's refresh token,
        // which we must keep so we can revoke it on account deletion (App Store req).
        var authCode: String?
        if let codeData = credential.authorizationCode {
            authCode = String(data: codeData, encoding: .utf8)
        }

        print("[Apple] Got ID token, user: \(credential.user.prefix(10))..., name: \(fullName ?? "nil"), code: \(authCode != nil ? "yes" : "no")")
        continuation?.resume(returning: (idToken: idToken, fullName: fullName, authorizationCode: authCode))
        continuation = nil
    }

    func authorizationController(controller: ASAuthorizationController, didCompleteWithError error: Error) {
        if let asError = error as? ASAuthorizationError, asError.code == .canceled {
            continuation?.resume(throwing: AuthError.cancelled)
        } else {
            continuation?.resume(throwing: error)
        }
        continuation = nil
    }

    // MARK: - ASAuthorizationControllerPresentationContextProviding

    func presentationAnchor(for controller: ASAuthorizationController) -> ASPresentationAnchor {
        // On iPad, the authorization controller needs the real key window,
        // not a fresh empty ASPresentationAnchor. Returning an unattached
        // window causes the request to fail silently on iPadOS.
        UIApplication.shared.connectedScenes
            .compactMap { $0 as? UIWindowScene }
            .flatMap { $0.windows }
            .first(where: { $0.isKeyWindow }) ?? ASPresentationAnchor()
    }
}
