#!/usr/bin/env python3
"""
Backend server for handling MinIO MC-based file-system requests.
"""

import json
import os
import subprocess
import queue
import time


class FileSysRequest:
    """
    Request to file system.
    """

    def __init__(self, request_num: int, config: dict):
        self.request_num = request_num
        self.config = config
        self.is_active = False

    def check_if_activated(self):
        """
        Check if request is available.
        """
        if self.is_active:
            return True
        comm_path = self.config["comm_path"]
        ready_path = f"{comm_path}_{self.request_num}_data.start"
        return os.path.exists(ready_path)


def get_config_var(var_name: str):
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
    cur_request = 0

    requests.put(FileSysRequest(cur_request, config))

    while True:
        cr = requests.get()


if __name__ == "__main__":
    main()
