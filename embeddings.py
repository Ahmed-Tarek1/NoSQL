from sentence_transformers import SentenceTransformer

# Lightweight example embedding provider using sentence-transformers.
# Model: all-MiniLM-L6-v2 (small, fast).
_model = None

def _get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer('all-MiniLM-L6-v2')
    return _model

def compute_embedding(value, key=None):
    """Return a float list embedding for `value`.

    This function is intentionally simple and synchronous. For production
    use you may want an async wrapper, batching, or a persistent process.
    """
    model = _get_model()
    text = str(value)
    emb = model.encode(text)
    try:
        return emb.tolist()
    except Exception:
        return list(map(float, emb))
