"""
Process that handles object, used by backend server.
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


def init_local_file(file_state: dict, retrieve: bool):
    """
    If necessary, initialize the local file referred to by file_state.
    """
    if not file_state["is_init"]:
        td = tempfile.TemporaryDirectory()
        file_state["file_name"] = f"{td}/data.bin"
        if retrieve:
            copy_from_minio(file_state)
        file_state["handle"] = open(file_state["file_name"], "+ab")


def do_read(
    file_state: dict,
    pipe_request: io.BufferedReader,
    pipe_response: io.BufferedWriter,
):
    """
    Read input from file and send to output pipe.
    """

    print("Perform read operation on file:", file_state["name"])
    size = get_input_uint64(pipe_request)
    offset = get_input_uint64(pipe_request)
    print(f"Using size {size} and offset {offset}.")

    try:
        file_state["handle"].seek(offset, os.SEEK_SET)
        data = file_state["handle"].read(size)
    except OSError as e:
        print("Encountered error:", e)
        send_output_int8(pipe_response, -1)
        return

    send_output_int8(pipe_response, 0)  # success status code
    length = len(data)
    send_output_uint64(pipe_response, length)
    pipe_response.write(data)
    print("Done the read operation.")


def do_write(
    file_state: dict,
    pipe_request: io.BufferedReader,
    pipe_response: io.BufferedWriter,
):
    """
    Write output to file, from input pipe.
    """

    print("Perform write operation on file:", file_state["name"])
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


def file_io_handler(file_state: dict, base_path: str) -> bool:
    """
    Handler of single I/O request.
    Returns boolean if should exit after request handled.
    """

    # Open the files.
    with (
        open(f"{base_path}/pipe_request", "rb") as pipe_request,
        open(f"{base_path}/pipe_response", "wb") as pipe_response,
    ):
        # Get the request type.
        request_type = get_request_type(pipe_request)

        # Handle the specified type of request.
        if request_type == "read":
            do_read(file_state, pipe_request, pipe_response)
            return False
        elif request_type == "write":
            do_write(file_state, pipe_request, pipe_response)
            return False
        elif request_type == "flush":
            do_flush(file_state, pipe_response)
            return False
        elif request_type == "truncate":
            do_truncate(file_state, pipe_request, pipe_response)
            return False
        elif request_type == "create":
            do_create(file_state, pipe_response)
            return False
        elif request_type == "release":
            do_release(file_state, pipe_response)
            return True
        elif request_type == "unlink":
            do_unlink(file_state, pipe_response)
            return True
        else:
            raise NotImplementedError(f"{request_type}: request_type")


def file_process(
    minio_path: str,
    config: dict,
    queue_in: mp.Queue,
    queue_out: mp.Queue,
):
    """
    Function started for handling file.
    """

    print("Started process to handle MinIO path:", minio_path)
    with tempfile.TemporaryDirectory() as temp_dir:
        print("Using temporary directory:", temp_dir)
        file_state = {
            "temp_dir": temp_dir,
            "file_name": f"{temp_dir}/data.bin",
            "handle": None,
            "is_init": False,
            "write_out": False,
        }

        while True:
            request = queue_in.get()
