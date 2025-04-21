#!/usr/bin/env python3
"""
Backend server for handling MinIO MC-based file-system requests.
"""

import json
import os
import subprocess
import queue
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

    requests = queue.Queue()
    filesys_metadata = {}
    active_files = {}
    cur_request = 0

    requests.put(FileSysRequest(cur_request, config))

    while True:
        cr = requests.get()


if __name__ == "__main__":
    main()
