"""
File cache implementation for backend server.
"""

import os
import subprocess
import tempfile


def add_to_cache(request: dict, get_existing: bool) -> bool:
    """
    Get new entry for cache.
    """
