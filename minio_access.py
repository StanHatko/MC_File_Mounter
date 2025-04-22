"""
MinIO access for backend server.
"""

import json
import subprocess
import tempfile


def get_minio_command(config: dict, cmd: str, params: list):
    """
    Get subprocess list to run specified MinIO mc client command.
    """
    r = [config["mc_bin_path"], cmd]
    for p in params:
        r.append(p)
    return r


def get_file_from_minio(config: dict, minio_source: str, temp_dir: str):
    """
    Copy file from MinIO to local directory.
    """
    # TODO implement
