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


def send_request(process: dict, base_path: str, op_num: int):
    """
    Send new request to input queue of process.
    """
    iq = process["input_queue"]
    iq.put({"base_path": base_path, "op_num": op_num})


def start_process(minio_path: str, config: dict, processes: dict):
    """
    Starts a process dedicated for a specific object.
    """
    iq = mp.Queue()
    oq = mp.Queue()
    p = mp.Process(
        target=object_process,
        args=(
            minio_path,
            config,
            iq,
            oq,
        ),
    )
    print(f"Started process with PID {p.pid} for {minio_path}.")
    processes[minio_path] = {
        "minio_path": minio_path,
        "input_queue": iq,
        "output_queue": oq,
        "process": p,
    }


def start_operation(
    operation: str,
    pipe_in: io.BufferedReader,
    pipe_out: io.BufferedWriter,
    minio_path: str,
    config: dict,
    processes_file: dict,
):
    """
    Start operation, either create new process or send request to existing.
    """

    # Action depends on type of operation.
    if operation in ["read", "write", "create", "flush", "release", "truncate"]:
        # Operation applies to file process processes.
        if minio_path in processes_file:
            processes_file["input_queue"].put(TODO)
        else:
            # Need to start new process.
            start_process(TODO)
    elif operation in ["access", "list_dir"]:
        # Metadata-type operation.
        TODO
    elif operation in ["mkdir", "rmdir", "unlink"]:
        # MinIO path-oriented operation.
        TODO
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
        )

        # Do cleanup as necessary.
        clean_operations(config, processes)
        op_num += 1


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
