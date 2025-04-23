"""
Implementations of operations for backend server.
"""

import io
import multiprocessing as mp
import os
import sys
import tempfile

import minio


def get_input_uint64(pipe_request: io.BufferedReader) -> int:
    """
    Read unsigned 64-bit integer from pipe.
    """
    r = pipe_request.read(8)
    return int.from_bytes(r, sys.byteorder, signed=False)


def send_output_int8(pipe_response: io.BufferedWriter, value: int):
    """
    Sends 8-bit signed integer to pipe.
    """
    b = value.to_bytes(1, sys.byteorder, signed=True)
    pipe_response.write(b)


def send_output_uint64(pipe_response: io.BufferedWriter, value: int):
    """
    Sends 64-bit unsigned integer to pipe.
    """
    b = value.to_bytes(8, sys.byteorder, signed=False)
    pipe_response.write(b)


def init_local_file(minio_path: str, temp_path: str | None, retrieve: bool):
    """
    If necessary, initialize the local file either by copying from MinIO or creating empty.
    Returns path to the temporary file to use.
    """

    if temp_path is not None:
        # Already have temporary local file, no need to reload.
        return temp_path

    # Get name of temporary file to use and create empty.
    with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as tf:
        temp_path = tf.name
        print("Using temporary path for local file:", temp_path)

    if retrieve:
        # Replace that file name with temporary file from MinIO.
        copy_from_minio(minio_path, temp_path)


def do_read(
    minio_path: str,
    temp_path: str,
    pipe_request: io.BufferedReader,
    pipe_response: io.BufferedWriter,
) -> str:
    """
    Read input from file and send to output pipe.
    """

    print("Perform read operation on file:", minio_path)
    temp_path = init_local_file(minio_path, temp_path, True)
    print("Using temporary file:", temp_path)

    size = get_input_uint64(pipe_request)
    offset = get_input_uint64(pipe_request)
    print(f"Using size {size} and offset {offset}.")

    try:
        with open(temp_path, "rb") as f:
            f.seek(offset, os.SEEK_SET)
            data = f.read(size)
    except OSError as e:
        print("Encountered error:", e)
        send_output_int8(pipe_response, -1)
        return temp_path

    send_output_int8(pipe_response, 0)  # success status code
    length = len(data)
    send_output_uint64(pipe_response, length)
    pipe_response.write(data)
    print("Done the read operation.")
    return temp_path


def do_write(
    minio_path: str,
    temp_path: str,
    pipe_request: io.BufferedReader,
    pipe_response: io.BufferedWriter,
) -> str:
    """
    Write output to file, from input pipe.
    """

    print("Perform write operation on file:", minio_path)
    temp_path = init_local_file(minio_path, temp_path, True)
    print("Using temporary file:", temp_path)

    size = get_input_uint64(pipe_request)
    offset = get_input_uint64(pipe_request)
    data = pipe_request.read(size)
    print(f"Using size {size} and offset {offset}.")

    try:
        file_state["handle"].seek(offset, os.SEEK_SET)
        file_state["handle"].write(data)
        ret_code = 0
    except OSError as e:
        print("Encountered error:", e)
        ret_code = -1

    file_state["write"] = True
    send_output_int8(pipe_response, ret_code)
    print("Done the write operation.")
    return temp_path


def do_flush(
    file_state: dict,
    pipe_response: io.BufferedWriter,
):
    """
    Backup output to configured MinIO storage.
    """

    print("Perform flush operation on file:", file_state["name"])
    try:
        file_state["handle"].flush()

        if file_state["write_out"]:
            copy_to_minio(file_state)
        else:
            print("No need to copy to MinIO.")

        ret_code = 0
    except OSError as e:
        print("Encountered error:", e)
        ret_code = -1

    send_output_int8(pipe_response, ret_code)
    file_state["write"] = False
    print("Done the flush operation.")


def do_create(
    file_state: dict,
    pipe_response: io.BufferedWriter,
):
    """
    Create new file of specified size.
    """
    init_local_file(file_state, False)
    file_state["write"] = True


def do_truncate(
    file_state: dict,
    pipe_request: io.BufferedReader,
    pipe_response: io.BufferedWriter,
):
    """
    Truncate file to specified size and flush output.
    """

    print("Perform truncate operation on file:", file_state["name"])
    size = get_input_uint64(pipe_request)
    print("Truncate to size:", size)

    try:
        os.truncate(file_state["handle"], size)
        file_state["handle"].flush()
        copy_to_minio(file_state)
        ret_code = 0
    except OSError as e:
        print("Encountered error:", e)
        ret_code = -1

    file_state["write"] = False
    send_output_int8(pipe_response, ret_code)
    print("Done the truncate operation.")


def do_release(file_state: dict, pipe_response: io.BufferedWriter):
    """
    Close file, writing out to MinIO if necessary.
    """


def do_unlink(
    file_state: dict,
    pipe_response: io.BufferedWriter,
):
    """
    Delete file in MinIO.
    """
    print("Delete file in MinIO:", file_state["name"])
    try:
        delete_minio(file_state)
        ret_code = 0
    except OSError as e:
        print("Encountered error:", e)
        ret_code = -1
    send_output_int8(pipe_response, ret_code)
    print("Done the delete operation.")


def handle_io_request(
    minio_path: str,
    operation: str,
    file_pipe_in: str,
    file_pipe_out: str,
    temp_path: str | None,
) -> list:
    """
    Handle single I/O request.
    Returns list of objects to send on output queue.
    """

    # Open the files for the queues.
    with (
        open(file_pipe_in, "rb") as pipe_request,
        open(file_pipe_out, "wb") as pipe_response,
    ):
        # Handle the specified type of request.
        if operation == "read":
            do_read(minio_path, temp_path, pipe_request, pipe_response)
        elif operation == "write":
            do_write(minio_path, temp_path, pipe_request, pipe_response)
        elif operation == "flush":
            do_flush(file_state, pipe_response)
        elif operation == "truncate":
            do_truncate(file_state, pipe_request, pipe_response)
        elif operation == "create":
            do_create(file_state, pipe_response)
            return False
        elif operation == "release":
            do_release(file_state, pipe_response)
            return True
        elif operation == "unlink":
            status["is_stateful"] = False
            do_unlink(file_state, pipe_response)
            return True
        else:
            raise NotImplementedError(f"{request_type}: request_type")
