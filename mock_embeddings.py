"""CI-friendly mock embedding provider.

Provides a deterministic embedding for a given value (and optional key)
without heavy ML dependencies. Useful for CI and fast unit tests.
"""
import random

def compute_embedding(value, key=None, dim=128):
    s = str(value)
    # incorporate key to make per-key deterministic variations
    seed = hash((key, s)) & 0xFFFFFFFF
    rnd = random.Random(seed)
    return [rnd.random() for _ in range(dim)]
