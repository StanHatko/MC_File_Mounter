#!/usr/bin/env python3
"""
Backend server for handling MinIO-based file-system requests.
"""

import glob
import io
import multiprocessing as mp
import os
import sys
import tempfile
import time

from implementations import handle_io_request

# Operations that maintain file state.
KEEP_STATE_OPS = [
    "read",
    "write",
    "create",
    "flush",
    "release",
    "truncate",
]

# Operations that get metadata.
GET_METADATA_OPS = ["access", "list_dir"]

# Operations that modify paths.
MODIFY_PATH_OPS = ["mkdir", "rmdir", "unlink"]


class FileObject:
    """
    Object in the MinIO remote file-system.
    Operations on this object are done in a new process.
    """

    def __init__(self, minio_path: str, metadata: dict | None = None):
        self.minio_path = minio_path
        self.queue_in = None
        self.queue_out = None
        self.process = None
        self.last_modified = time.time()
        self.metadata_cache = metadata
        self.num_requests_sent = 0
        self.num_requests_done = 0

    def get_cached_metadata(self):
        """
        Returns metadata dictionary about the current object, if available.
        If not available, instead need to query process.
        Updates the last modified time as well.
        """
        self.last_modified = time.time()
        return self.metadata_cache

    def init_queues(self):
        """
        Initializes the input and output queues, if necessary.
        Updates the last modified time.
        """
        if self.queue_in is None:
            self.queue_in = mp.Queue()
            self.queue_out = mp.Queue()

    def cleanup_process(self):
        """
        If process is no longer alive, clean-up the process.
        Does not update last modified time.
        """
        if self.process is not None and not self.process.is_alive():
            print(
                (
                    f"Process with PID {self.process.pid} exited with return "
                    f"code {self.process.exitcode}, for path {self.minio_path}."
                )
            )
            self.process = None

    def start_process(self):
        """
        Starts (or re-starts) the process associated with this file.
        Updates the last modified time.
        """
        if self.process is None:
            self.init_queues()
            self.metadata_cache = None
            self.process = mp.Process(
                target=file_process,
                args=(
                    self.minio_path,
                    self.queue_in,
                    self.queue_out,
                ),
            )
            print(
                f"Start process with PID {self.process.pid}, for path {self.minio_path}."
            )
        self.last_modified = time.time()

    def send_request(self, operation: str, pipe_in: str, pipe_out: str):
        """
        Send request with specified operation and pipes to use by adding to queue.
        Updates the last modified time.
        """

        self.init_queues()
        self.queue_in.put(
            {
                "operation": operation,
                "pipe_in": pipe_in,
                "pipe_out": pipe_out,
            }
        )

        # Update last modified time and certain metadata.
        self.last_modified = time.time()
        self.num_requests_sent += 1

    def update_with_output(self, full_db: dict):
        """
        Get any outputs from the process and update necessary information.
        Reach response updates last modified time.
        """

        # If no queue for file, nothing to do.
        if self.queue_out is None:
            return

        # Get any new outputs from process available.
        while not self.queue_out.empty():
            r = self.queue_out.get()

            # Indicate existing process done.
            if "is_done" in r and r["is_done"]:
                self.num_requests_done += 1

            # Update metadata in current object (can be None to delete existing).
            if "metadata_cur" in r:
                self.metadata_cache = r["metadata_cur"]

            # Create new metadata object, if doesn't already exist.
            if "metadata_new" in r:
                meta_new = r["metadata_new"]
                path_new = meta_new["minio_path"]
                if path_new not in full_db:
                    full_db[path_new] = FileObject(path_new, meta_new)

            # Update last modified if any such operation performed.
            self.last_modified = time.time()

    def delete_inactive(self, full_db: dict, age_delete_inactive: float):
        """
        If process exited and too old last modified, remove from database.
        """
        max_age = self.last_modified + age_delete_inactive
        if self.process is None and (time.time() > max_age):
            # Destroy queues.
            if self.queue_in is not None:
                self.queue_in.close()
            if self.queue_out is not None:
                self.queue_out.close()

            # Remove from database.
            full_db.pop(self.minio_path, None)


def get_config_var(var_name: str) -> str:
    """
    Gets specified configuration variable (from environment variables).
    """
    v = os.getenv(var_name)
    if v is None:
        raise ValueError(f"Environment variable {var_name} must be set!")
    print(f"Configuration variable {var_name} has value: {v}")
    return v


def get_request_info(control_pipe: io.BufferedReader):
    """
    Get information about request from pipe.
    """
    line = control_pipe.readline()
    return line.split("|", maxsplit=3)


def operation_process(
    minio_path: str,
    temp_path: str | None,
    queue_in: mp.Queue,
    queue_out: mp.Queue,
):
    """
    Runs process for single operation.
    Input and output via controller process done using the per-file queues.
    Other input and output done via pipes filenames passed using input queue.
    """

    pid = os.getpid()
    print(f"Running process {pid} for operation on file {minio_path}.")

    task = queue_in.get()
    operation = task["operation"]
    pipe_in = task["pipe_in"]
    pipe_out = task["pipe_out"]
    print(
        (
            f"Current operation is {operation}, with input pipe {pipe_in}, "
            f"output pipe {pipe_out}, and temporary file {temp_path}."
        )
    )

    output_data = handle_io_request(
        minio_path,
        operation,
        pipe_in,
        pipe_out,
        temp_path,
    )


def start_operation(
    operation: str,
    pipe_in: io.BufferedReader,
    pipe_out: io.BufferedWriter,
    minio_path: str,
    config: dict,
    processes_stateful: dict,
    processes_stateless: dict,
    metadata: dict,
):
    """
    Start operation, do one of:
    * Create new process.
    * Send request to existing process.
    * Return cached metadata.
    """

    if operation in KEEP_STATE_OPS:
        # Send request to process that keeps state.
        if minio_path not in processes_stateful:
            processes_stateful[minio_path] = start_process_stateful(
                minio_path,
                config,
                operation,
                pipe_in,
                pipe_out,
            )
        else:
            send_process(processes_stateful[minio_path], operation, pipe_in, pipe_out)
    elif operation in GET_METADATA_OPS:
        # Send stateless get metadata request.
        if minio_path in processes_stateful:
            # If stateful process running, use metadata from that.
            send_process(TODO)
        elif minio_path in metadata:
            # If cached metadata available, use that.
            send_metadata(TODO)
        else:
            # If neither above available, start process to get metadata.
            processes_stateless[minio_path] = start_process_stateless(TODO)
    elif operation in MODIFY_PATH_OPS:
        if minio_path in processes_stateful:
            # If stateful process running, use that to handle request.
            send_process(TODO)
        else:
            # If already has stateless process, terminate it.
            if minio_path in process_stateless:
                end_process(processes_stateless[minio_path])
                processes_stateless.pop(minio_path, None)
            # Start process for stateless request that modifies path.
            processes_stateless[minio_path] = start_process(TODO)
    else:
        raise NotImplementedError(f"operation: {operation}")


def clean_operations(config: dict, processes: dict):
    """
    Cleanup files from old operations that have already completed.
    """

    for p in processes:
        if p["process"].poll():
            n = p["op_num"]
            print(f"Found process {n} has already finished.")

            base_path = p["base_path"]
            for x in glob.glob(f"{base_path}*"):
                print("Delete old temporary file:", x)
                try:
                    os.unlink(x)
                except OSError as e:
                    print("Error occurred removing file:", e)

            p["input_queue"].destroy()
            p["output_queue"].destroy()
            print(f"Done cleanup for finished process {n}.")


def main():
    """
    Main function invoked when running program.
    Initializes and handles requests in loop.
    """

    print("Initialize back-end for MinIO-MC based file system...")
    config = {
        "minio_server": get_config_var("minio_server"),
        "minio_username": get_config_var("minio_username"),
        "minio_password": get_config_var("minio_password"),
        "minio_bucket": get_config_var("minio_bucket"),
        "control_pipe": get_config_var("control_pipe"),
    }
    control_pipe_file = config["control_pipe"]
    processes_file = {}
    processes_path = {}
    metadata = {}

    print("Handle requests with infinite loop...")
    op_num = 1

    with open(control_pipe_file, "r", encoding="utf-8") as control_pipe:
        # Get the operation.
        print("Wait for operation #{op_num} to perform.")
        operation, pipe_in, pipe_out, minio_path = get_request_info(control_pipe)
        print(
            (
                f"Operation # {op_num} is {operation} on object {minio_path}, "
                f"using input pipe {pipe_in} and output pipe {pipe_out}."
            )
        )

        # Do the actual operation.
        start_operation(
            operation,
            pipe_in,
            pipe_out,
            minio_path,
            config,
            processes_file,
            processes_path,
            metadata,
        )

        # Do cleanup as necessary.
        clean_operations(config, processes)
        op_num += 1


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
