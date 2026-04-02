"""Document lifecycle signals using django.dispatch."""

from django.dispatch import Signal

# Sent before a document is saved (created or updated).
# sender: the Document class
# instance: the Document instance
# created: bool — True if this is a new document
pre_save = Signal()

# Sent after a document is saved.
# sender: the Document class
# instance: the Document instance
# created: bool — True if this was a new document
post_save = Signal()

# Sent before a document is deleted.
# sender: the Document class
# instance: the Document instance
pre_delete = Signal()

# Sent after a document is deleted.
# sender: the Document class
# instance: the Document instance
post_delete = Signal()
