import SwiftUI

struct BreweryFormView: View {
    enum Mode {
        case add
        case edit(Brewery)
    }

    let mode: Mode
    @Environment(\.dismiss) private var dismiss

    @State private var name = ""
    @State private var city = ""
    @State private var state = ""
    @State private var country = ""
    @State private var description = ""
    @State private var website = ""
    @State private var error: String?

    var isEditing: Bool {
        if case .edit = mode { return true }
        return false
    }

    var body: some View {
        NavigationStack {
            ZStack {
                Theme.bg.ignoresSafeArea()

                ScrollView {
                    VStack(spacing: 16) {
                        TextField("Brewery Name", text: $name)
                            .textFieldStyle(BrewTextFieldStyle())

                        HStack(spacing: 12) {
                            TextField("City", text: $city)
                                .textFieldStyle(BrewTextFieldStyle())
                            TextField("State", text: $state)
                                .textFieldStyle(BrewTextFieldStyle())
                        }

                        TextField("Country", text: $country)
                            .textFieldStyle(BrewTextFieldStyle())

                        TextField("Website", text: $website)
                            .textFieldStyle(BrewTextFieldStyle())
                            .keyboardType(.URL)
                            .autocapitalization(.none)

                        TextField("Description", text: $description, axis: .vertical)
                            .textFieldStyle(BrewTextFieldStyle())
                            .lineLimit(3...6)

                        if let error = error {
                            Text(error)
                                .font(.caption)
                                .foregroundColor(Theme.danger)
                        }

                        Button {
                            save()
                        } label: {
                            Text(isEditing ? "Save Changes" : "Add Brewery")
                                .fontWeight(.semibold)
                                .frame(maxWidth: .infinity)
                                .padding(.vertical, 16)
                                .background(name.isEmpty ? Theme.border : Theme.accent)
                                .foregroundColor(name.isEmpty ? Theme.textMuted : .black)
                                .cornerRadius(12)
                        }
                        .disabled(name.isEmpty)
                    }
                    .padding(20)
                }
            }
            .navigationTitle(isEditing ? "Edit Brewery" : "Add Brewery")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                        .foregroundColor(Theme.accent)
                }
            }
            .onAppear {
                if case .edit(let brewery) = mode {
                    name = brewery.name
                    city = brewery.city
                    state = brewery.state
                    country = brewery.country
                    description = brewery.description
                    website = brewery.website
                }
            }
        }
    }

    private func save() {
        var brewery: Brewery
        if case .edit(let existing) = mode {
            brewery = existing
        } else {
            let tempId = Int(Date().timeIntervalSince1970)
            brewery = Brewery(id: tempId, name: "", city: "", state: "", country: "", description: "", website: "")
        }

        brewery.name = name
        brewery.city = city
        brewery.state = state
        brewery.country = country
        brewery.description = description
        brewery.website = website

        do {
            try DatabaseManager.shared.saveBrewery(brewery)
            dismiss()
        } catch {
            self.error = error.localizedDescription
        }
    }
}
