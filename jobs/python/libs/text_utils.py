"""Tiny helper module shipped with --py-files (see 04_pyfiles_demo.py).

Edit this file, then rebuild the zip:
    cd jobs/python/libs && zip -j text_utils.zip text_utils.py
and run `make upload`.
"""
import re

VERSION = "1.0.0"

STOP_WORDS = {
    "the", "and", "for", "that", "with", "this", "are", "was", "from",
    "you", "your", "but", "not", "have", "has", "its", "into", "than",
    "then", "they", "them", "can", "will", "all", "one", "out", "who",
}


def normalize_words(line):
    """Lowercase, split on non-letters, drop short words and stop words."""
    words = re.split(r"[^a-z0-9']+", line.lower())
    return [w for w in words if len(w) > 2 and w not in STOP_WORDS]
