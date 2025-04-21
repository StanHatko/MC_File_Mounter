#!/usr/bin/env python3
"""
Backend server for handling MinIO MC-based file-system requests.
"""

import glob
import json
import os
import re
import subprocess
import time


def get_file_str(filename: str) -> str | None:
    """
    Get contents of file as string, or None if fails.
    """
    try:
        with open(filename, "r", encoding="UTF8") as f:
            return f.read()
    except (OSError, UnicodeError) as e:
        print(f"Could not read string from file, with exception: {e}")
        return None


class FileSysRequest:
    """
    Request to file system.
    """

    def __init__(self, request_num: int, config: dict):
        self.request_num = request_num
        self.config = config
        self.is_active = False
        self.is_done = False
        self.operation = None

        comm_path = self.config["comm_path"]
        self.comm_base = f"{comm_path}_{self.request_num}_data."

    def check_if_available(self) -> bool:
        """
        Check if request is available.
        """
        if self.is_active:
            return True
        ready_path = f"{self.comm_base}.start"
        return os.path.exists(ready_path)

    def start_handle_request(self):
        """
        Start the actual handling of the request.
        """

        assert not self.is_active
        self.operation = get_file_str(f"{self.comm_base}.op")

        if self.operation is None:
            print("Unable to obtain operation to perform.")
            return

        if self.operation == "read":
            TODO
        elif self.operation == "write":
            TODO


def get_config_var(var_name: str) -> str:
    """
    Gets specified configuration variable (from environment variables).
    """
    v = os.getenv(var_name)
    if v is None:
        raise ValueError(f"Environment variable {var_name} must be set!")
    print(f"Configuration variable {var_name} has value: {v}")
    return v


def handle_requests(comm_path: str, requests: dict) -> bool:
    """
    Handle all active requests and detect new requests to handle.
    Returns bool indicating if any updates have occurred.
    """

    print("Handle all requests with start file present...")
    sfs = glob.glob(f"{comm_path}*.start")
    nfs = len(sfs)
    print(f"Found {nfs} start indicator files.")
    new_activity = False

    for sf in sfs:
        base = re.sub(r"\.start$", "", sf)

        if base not in requests:
            print("Found new request:", base)
            # TODO: add handling of new request
            new_activity = True
        else:
            # TODO: add handling of ongoing or done request
            TODO

    return new_activity


def delete_old_comm_files(comm_path: str, requests: dict) -> int:
    """
    Delete old communication files that are no longer needed.
    Returns number of deleted files.
    """

    print("Delete old communication files for tasks with delete file created...")
    dfs = glob.glob(f"{comm_path}*.del")
    nfs = len(dfs)
    print(f"Found {nfs} delete indicator files.")

    nc = 0
    for df in dfs:
        base = re.sub(r"\.del$", "", df)
        print("Delete files starting with:", base)

        # Remove from requests (if not present, OK).
        requests.pop(base, None)

        for fn in glob.glob(f"{base}*"):
            print("Delete file:", fn)
            # If cannot delete, undefined behavior will occur if ignore error.
            # So make fatal error that crashes program.
            os.unlink(fn)
            nc += 1

    print(f"Deleted total of {nc} files.")
    return nc


def main():
    """
    Main function invoked when running program.
    Initializes and handles requests in loop.
    """

    print("Initialize back-end for MinIO-MC based file system...")
    config = {
        "mc_bin_path": get_config_var("mc_bin_path"),
        "comm_path": get_config_var("mc_comm_prefix"),
        "remote_prefix": get_config_var("mc_path_prefix"),
    }
    comm_path = config["comm_path"]

    filesys_metadata = {}
    requests = {}

    print("Handle requests with infinite loop...")
    while True:
        # Detect requests that should be handled.
        handle_requests(comm_path, requests)

        # Detect old request files to be deleted.
        delete_old_comm_files(comm_path, requests)

        # TODO: sleep between requests, duration depends on activity


if __name__ == "__main__":
    main()
