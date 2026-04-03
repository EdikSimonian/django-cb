import SwiftUI
import WebKit

/// WKWebView that handles the full App Services OIDC flow.
/// Opens _oidc → Django login → _oidc_callback → captures session JSON.
struct OIDCWebView: UIViewRepresentable {
    let url: URL
    let onSession: (String, String) -> Void  // (sessionID, username)
    let onError: (String) -> Void

    func makeCoordinator() -> Coordinator {
        Coordinator(onSession: onSession, onError: onError)
    }

    func makeUIView(context: Context) -> WKWebView {
        let config = WKWebViewConfiguration()
        config.websiteDataStore = .default()  // Share cookies
        let webView = WKWebView(frame: .zero, configuration: config)
        webView.navigationDelegate = context.coordinator
        webView.isOpaque = false
        webView.backgroundColor = UIColor(Theme.bg)
        webView.scrollView.backgroundColor = UIColor(Theme.bg)
        webView.load(URLRequest(url: url))
        return webView
    }

    func updateUIView(_ uiView: WKWebView, context: Context) {}

    class Coordinator: NSObject, WKNavigationDelegate {
        let onSession: (String, String) -> Void
        let onError: (String) -> Void

        init(onSession: @escaping (String, String) -> Void, onError: @escaping (String) -> Void) {
            self.onSession = onSession
            self.onError = onError
        }

        func webView(_ webView: WKWebView, decidePolicyFor navigationResponse: WKNavigationResponse, decisionHandler: @escaping (WKNavigationResponsePolicy) -> Void) {
            // Check if this is the _oidc_callback response
            if let url = navigationResponse.response.url,
               url.path.contains("_oidc_callback") {
                // Let it load, we'll read the content in didFinish
                decisionHandler(.allow)
                return
            }
            decisionHandler(.allow)
        }

        func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
            guard let url = webView.url else { return }

            // Check if we're on the _oidc_callback page
            if url.path.contains("_oidc_callback") || url.absoluteString.contains("_oidc_callback") {
                // Read the page content (JSON response)
                webView.evaluateJavaScript("document.body.innerText") { [weak self] result, error in
                    guard let body = result as? String, !body.isEmpty else {
                        // Try getting full page HTML
                        webView.evaluateJavaScript("document.documentElement.outerHTML") { result, _ in
                            if let html = result as? String {
                                self?.parseResponse(html)
                            }
                        }
                        return
                    }
                    self?.parseResponse(body)
                }
            }

            // Also check if the current page has JSON with session_id
            // (some responses don't have _oidc_callback in the URL)
            if url.host?.contains("apps.cloud.couchbase.com") == true {
                webView.evaluateJavaScript("document.body.innerText") { [weak self] result, _ in
                    if let body = result as? String, body.contains("session_id") {
                        self?.parseResponse(body)
                    }
                }
            }
        }

        func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
            print("[OIDCWebView] Navigation failed: \(error)")
            onError(error.localizedDescription)
        }

        private func parseResponse(_ body: String) {
            print("[OIDCWebView] Parsing response: \(body.prefix(200))")

            // Try to extract JSON from the body
            guard let data = body.data(using: .utf8),
                  let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                // Maybe it's wrapped in HTML tags
                if let jsonStart = body.range(of: "{"),
                   let jsonEnd = body.range(of: "}", options: .backwards) {
                    let jsonString = String(body[jsonStart.lowerBound...jsonEnd.upperBound])
                    if let data = jsonString.data(using: .utf8),
                       let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                        handleJSON(json)
                        return
                    }
                }

                if body.contains("error") {
                    onError(body)
                }
                return
            }

            handleJSON(json)
        }

        private func handleJSON(_ json: [String: Any]) {
            if let sessionID = json["session_id"] as? String {
                let name = json["name"] as? String ?? ""
                print("[OIDCWebView] Got session! user=\(name)")
                onSession(sessionID, name)
            } else if let error = json["error"] as? String {
                let reason = json["reason"] as? String ?? error
                print("[OIDCWebView] Error: \(reason)")
                onError(reason)
            }
        }
    }
}
