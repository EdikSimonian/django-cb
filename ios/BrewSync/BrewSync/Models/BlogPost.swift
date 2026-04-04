import Foundation

struct BlogPost: Identifiable {
    let id: Int
    var title: String
    var slug: String
    var date: String
    var intro: String
    var body: String
}
