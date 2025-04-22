"""
File access and cache implementation for backend server.
"""

import os
import subprocess
import tempfile

from minio_access import get_minio_command


def add_file_cache(
    config: dict,
    minio_path: str,
    get_existing: bool,
    need_upload: bool,
) -> dict:
    """
    Get new entry for cache, return this as dictionary.
    """

    td = tempfile.TemporaryDirectory(prefix=config["file_cache_prefix"])
    fname = f"{td}/data.bin"

    if get_existing:
        process = subprocess.Popen(
            get_minio_command(config, "cp", [minio_path, fname]),
            stdout=subprocess.DEVNULL,
        )
        cache_file = None
        is_ready = False
    else:
        # Simple case, just create brand new file.
        cache_file = open(fname, "+wb")
        process = None
        is_ready = True

    return {
        "minio_path": minio_path,
        "cache_dir": td,
        "cache_path": fname,
        "cache_file": cache_file,
        "is_ready": is_ready,
        "upload": need_upload,
        "delete_cache": False,
        "process": process,
    }


def check_ready_file_cache(cache_entry: dict) -> bool:
    """
    Check if file cache entry is ready.
    Returns bool indicating ready status.
    """

    # Simple case: already in ready state.
    if cache_entry["is_ready"]:
        return True

    # Otherwise, finished process may make the entry ready.
    if cache_entry.process.poll():
        r = cache_entry["process"].returncode
        if r != 0:
            cache_entry["delete_cache"] = True
            raise OSError(f"Command failed with return code: {r}")

        # Successfully obtained file.
        cache_entry["cache_file"] = open(cache_entry["cache_path"], "+wb")
        cache_entry["is_ready"] = True
    else:
        return False


def read_file_cache(cache_entry: dict, size: int, offset: int) -> bytes:
    """
    Read data from cache entry.
    Raises exception if error occurs.
    """

    if not cache_entry["is_ready"]:
        raise ValueError("Only call read_file_cache if is_ready flag set!")

    cache_entry["cache_file"].seek(offset, os.SEEK_SET)
    return cache_entry["cache_file"].read(size)


def write_file_cache(cache_entry: dict, data: bytes, offset: int):
    """
    Write data to cache entry.
    Raises exception if error occurs.
    """

    if not cache_entry["is_ready"]:
        raise ValueError("Only call write_file_cache if is_ready flag set!")

    cache_entry["cache_file"].seek(offset, os.SEEK_SET)
    cache_entry["cache_file"].write(data)
    cache_entry["need_upload"] = True


def upload_minio_file_cache(config: dict, cache_entry: dict):
    """
    Upload cached file to MinIO storage.
    """

    if not cache_entry["is_ready"]:
        raise ValueError("Only call read_file_cache if is_ready flag set!")

    cache_entry["process"] = subprocess.Popen(
        get_minio_command(
            config,
            "cp",
            [cache_entry["cache_path"], cache_entry["minio_path"]],
        ),
        stdout=subprocess.DEVNULL,
    )

    cache_entry["is_ready"] = False
    cache_entry["upload"] = False
