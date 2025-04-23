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
        self.temp_path = None
        self.write_out = False
        self.queue_in = None
        self.queue_out = None
        self.process = None
        self.last_modified = time.time()
        self.metadata = metadata
        self.num_requests = 0

    def send_request(self, operation: str, pipe_in: str, pipe_out: str):
        """
        Send request with specified operation and pipes to use by adding to queue.
        """

        # Initialize queues if necessary.
        if self.queue_in is None:
            self.queue_in = mp.Queue()
            self.queue_out = mp.Queue()

        # Send the request.
        self.queue_in.put(
            {
                "operation": operation,
                "pipe_in": pipe_in,
                "pipe_out": pipe_out,
            }
        )

        # Update last modified time.
        self.last_modified = time.time()
        self.num_requests += 1

    def _remove_finished_process(self):
        if self.process is not None and not self.process.is_alive():
            print(
                (
                    f"Process with PID {self.process.pid} exited with return "
                    f"code {self.process.exitcode}, for path {self.minio_path}."
                )
            )
            self.process = None
            self.last_modified = time.time()

    def _start_process_if_necessary(self):
        if (
            self.queue_in is not None
            and self.process is None
            and not self.queue_in.empty()
        ):
            self.process = mp.Process(
                target=operation_process,
                args=(self.minio_path, self.temp_path, self.queue_in, self.queue_out),
            )
            print(
                f"Start process with PID {self.process.pid}, for path {self.minio_path}."
            )
            self.last_modified = time.time()

    def _update_with_output(self, full_db: dict):
        """
        Get any outputs from the process.
        """

        # If no queue for file, nothing to do.
        if self.queue_out is None:
            return

        # Update object and add new metadata objects as needed.
        # TODO: change this, only run after process exits.
        # Loop until done.
        while not self.queue_out.empty():
            r = self.queue_out.get()

            # Update fields in current object.
            if "temp_path" in r:
                self.temp_path = r["temp_path"]
            if "write_out" in r:
                self.write_out = r["write_out"]
            if "metadata_cur" in r:
                self.metadata = r["metadata_cur"]

            # Create new metadata object, if doesn't already exist.
            if "metadata_new" in r:
                meta_new = r["metadata_new"]
                path_new = meta_new["minio_path"]
                if path_new not in full_db:
                    full_db[path_new] = FileObject(path_new, meta_new)

            # Update last modified if any such operation performed.
            self.last_modified = time.time()

    def _delete_inactive(self, full_db: dict, age_delete_inactive: float):
        if (
            self.temp_path is None
            and self.process is None
            and (not self.write_out)
            and (time.time() > self.last_modified + age_delete_inactive)
        ):
            # Destroy queues.
            if self.queue_in is not None:
                self.queue_in.close()
            if self.queue_out is not None:
                self.queue_out.close()

            # Remove from database.
            full_db.pop(self.minio_path, None)

    def update(self, full_db: dict, age_delete_inactive: float):
        """
        Update the database if necessary, with the following operations:
        * If existing processed finished, remove existing process.
        * If no process running and nonempty input queue, start process.
        * Update with output as necessary.
        * If no longer needed and too old process, delete and remove from database.
        """
        self._remove_finished_process()
        self._start_process_if_necessary()
        self._update_with_output(full_db)
        self._delete_inactive(full_db, age_delete_inactive)


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
