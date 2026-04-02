import uuid


def generate_id() -> str:
    """Generate a unique document ID."""
    return str(uuid.uuid4())
