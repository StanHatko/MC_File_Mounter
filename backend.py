#!/usr/bin/env python3
"""
Backend server for handling MinIO MC-based file-system requests.
"""

import glob
import json
import os
import re
import subprocess
import tempfile
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


class FileObject:
    """
    File object for remote file system.
    """

    def __init__(self, file_name: str, config: dict):
        self.request_id = None
        self.filename = file_name
        self.write_out = False
        self.cache_file = tempfile.NamedTemporaryFile(mode="+wb")
        self.process = None
        self.cache_file = None

    def get_from_remote(self, request_cur: dict):
        pass

    def send_to_remote(self, request_cur: dict):
        pass

    def write(self, request_cur: dict):
        """
        Write data to on-disk cache, in specified location.
        """
        print("Write data to file:", self.filename)
        rb = request_cur["base"]

        # Seek to required offset.
        with open(f"{rb}.offset", "r", encoding="UTF8") as f:
            offset = int(f.read())
        self.cache_file.seek(offset, os.SEEK_SET)

        # Get the required buffer.
        with open(f"{rb}.buffer", "rb") as f:
            data = f.read()

        # Do the write itself.
        print(f"Write {len(data)} bytes at offset {offset}.")
        self.cache_file.write(data)
        self.write_out = True
        print("Successfully completed the write operation.")

    def read(self, request_cur: dict):
        """
        Read data from on-disk cache, in specified location.
        """

    def truncate(self, request_cur: dict):
        """
        Truncate file to specified size.
        """

        print("Truncate file:", self.filename)
        rb = request_cur["base"]

        with open(f"{rb}.length", "r", encoding="UTF8") as f:
            length = int(f.read())
        print("Truncate to length:", length)
        os.truncate(self.cache_file.name, length)
        print("Successfully truncated the file.")


class DirObject:
    """
    Directory or file metadata object for remote file system.
    """

    def __init__(self, dir_name: str, config: dict):
        self.request_id = None
        self.dir_name = dir_name
        self.time_created = time.time()
        self.process = None
        self.results = None
        self.mc_bin_path = config["mc_bin_path"]

    def is_available(self) -> bool:
        """
        Returns True if can run new request now, False if not.
        """
        return self.request_id is None

    def get_dir_list(self, request_cur: dict) -> bool:
        """
        Get list of contents for the directory or file.
        Returns True if done, False if not done.
        Asynchronous, run as needed until gets result.
        Updates the self.results object when done.
        """

        # Initialize
        self.request_id = request_cur["id"]
        self.results = None

        # Run the mc ls process
        if self.process is not None:
            # Get metadata.
            print("Start process to get metadata for directory:", self.dir_name)
            self.process = subprocess.Popen(
                [
                    self.mc_bin_path,
                    "ls",
                    "--json",
                    self.dir_name,
                ],
                stdout=subprocess.PIPE,
            )

        # Check if done.
        if self.process.poll():
            print("Process metadata for directory:", self.dir_name)

            # If failed, indicate this.
            if self.process.returncode != 0:
                print("Get metadata has failed with return code:", self.returncode)
                self.results = {"failed": True}
                return True

            # Parse the output JSON.
            t = self.process.stdout.split("\n")
            r = []
            for line in t:
                line = line.strip()
                if line == "":
                    continue
                try:
                    r.append(json.loads(line))
                except (json.JSONDecodeError, UnicodeError) as e:
                    print("Encountered JSON decode error:", e)

            # Indicate done.
            print("Number of entries detected:", len(r))
            self.process = None
            self.request_id = None
            return True

        # Not yet done.
        return False


def get_config_var(var_name: str) -> str:
    """
    Gets specified configuration variable (from environment variables).
    """
    v = os.getenv(var_name)
    if v is None:
        raise ValueError(f"Environment variable {var_name} must be set!")
    print(f"Configuration variable {var_name} has value: {v}")
    return v


def do_unlink(request: dict, config: dict, files_cache: dict, metadata_cache: dict):
    """
    Deletes file from cache and MinIO storage.
    """

    if not request["init"]:
        # Init process to delete from MinIO
        request["process"] = subprocess.Popen(
            [config["mc_bin_path"], "mc", "rm", filename]
        )

        # Delete from file cache.
        if filename in files_cache:
            try:
                os.unlink(files_cache["temp_name"])
            except OSError as e:
                print("Failed to remove temporary file, with exception:", e)
            files_cache.pop(filename)

        # Delete from metadata cache.
        metadata_cache.pop(filename, None)


def process_operation(
    request: str,
    operation: str,
    config: dict,
    files_cache: dict,
    metadata_cache: dict,
) -> bool:
    """
    Process individual operation, by calling correct function.
    """

    if operation == "read":
        do_read(request, config, files_cache)
    elif operation == "write":
        do_write(request, config, files_cache)
    elif operation == "truncate":
        do_truncate(request, config, files_cache)
    elif operation == "unlink":
        do_unlink(request, config, files_cache)
    else:
        raise NotImplementedError(f"Operation: {operation}")


def process_requests(
    requests: dict,
    config: dict,
    files_cache: dict,
    metadata_cache: dict,
) -> bool:
    """
    Process all active requests.
    Returns bool indicating if any updates have occurred.
    """

    nr = len(requests)
    print(f"Process all {nr} requests...")

    for k in requests:
        cr = requests[k]

        # Skip done operations, nothing to do but wait until safe to delete.
        if cr["done"]:
            continue

        op = cr["operation"]
        process_operation(cr, op, config, files_cache, metadata_cache)


def add_requests(comm_path: str, requests: dict) -> bool:
    """
    Detect new requests to handle.
    Returns bool indicating if any updates have occurred.
    """

    print("Handle all requests with start file present...")
    sfs = glob.glob(f"{comm_path}*.start")
    nfs = len(sfs)
    print(f"Found {nfs} start indicator files.")
    new_activity = False

    # Add new requests to list.
    for sf in sfs:
        base = re.sub(r"\.start$", "", sf)

        if base not in requests:
            print("Found new request:", base)

            op = get_file_str(f"{base}.task")
            print("Operation type:", op)
            if op is None:
                print("Error: invalid request, operation type cannot be missing!")

            requests[base] = {
                "base": base,
                "init": False,
                "done": False,
                "operation": op,
            }
            new_activity = True

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

    files_cache = {}
    metadata_cache = {}
    requests = {}

    print("Handle requests with infinite loop...")
    while True:
        # Detect new requests that should be processed.
        add_requests(comm_path, requests)

        # Process all existing requests.
        process_requests(requests, config, files_cache, metadata_cache)

        # Detect old request files to be deleted.
        delete_old_comm_files(comm_path, requests)

        # TODO: sleep between requests, duration depends on activity


if __name__ == "__main__":
    main()
