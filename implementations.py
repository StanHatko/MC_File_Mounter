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


class CacheObject:
    """
    Object representing cached entry in MinIO file system.
    """

    def __init__(self, minio_path: str, temp_dir: str, config: dict):
        self.minio_path = minio_path
        self.basic_minio_path = minio_path.lstrip("/")
        self.temp_dir = temp_dir
        self.temp_path = f"{temp_dir}/file.bin"
        self.config = config

        self.write_out = False
        self.handle = None
        self.minio_client = minio.Minio(
            config["minio_host"],
            access_key=config["minio_access_key"],
            secret_key=config["minio_secret_key"],
        )
        self.minio_bucket = self.config["minio_bucket"]

    def init_handle(self, retrieve: bool):
        """
        Initialize handle to the temporary file, initializing it if necessary.
        Option retrieve controls whether file needs to be copied.
        """

        temp_path = self.temp_path
        minio_path = self.minio_path

        if retrieve:
            # Copy object from MinIO to temporary file.
            print(f"Copy to local file {temp_path} MinIO object {minio_path}.")
            self.minio_client.fget_object(
                self.minio_bucket,
                self.basic_minio_path,
                self.temp_path,
            )

            self.handle = open(temp_path, "+ab")
            print("Done obtaining the object from MinIO.")
        else:
            # Create empty.
            print(f"Create empty local file {temp_path} for MinIO object {minio_path}.")
            self.handle = open(temp_path, "+wb")
            print("Done creating the new empty file.")
            self.write_out = True

    def put_object_minio(self):
        """
        Copy temporary file object to MinIO storage.
        """
        self.minio_client.fput_object(
            self.minio_bucket,
            self.minio_path,
            self.temp_path,
        )

    def read(self, size: int, offset: int) -> bytes:
        """
        Read size bytes at offset.
        """
        self.init_handle(True)
        self.handle.seek(offset, os.SEEK_SET)
        return self.handle.read(size)

    def write(self, data: bytes, offset: int) -> bytes:
        """
        Write specified bytes at offset.
        """
        self.init_handle(True)
        self.handle.seek(offset, os.SEEK_SET)
        self.handle.write(data)
        self.write_out = True

    def flush(self):
        """
        Flush file to disk cache and MinIO, if anything to write.
        """
        self.init_handle(False)
        if self.write_out:
            self.handle.flush()
            self.put_object_minio()
            self.write_out = False

    def create(self):
        """
        Create new file.
        TODO: if file already exists, raise error instead.
        """
        self.init_handle(False)
        self.put_object_minio()

    def truncate(self, length: int):
        """
        Truncate file to specified size.
        """
        self.init_handle(length > 0)  # only need to copy if nonzero new length
        os.truncate(self.temp_path, length)
        self.put_object_minio()

    def unlink(self):
        """
        Delete file on MinIO.
        """

        if self.handle is not None:
            self.handle.close()
            self.handle = None

        try:
            os.unlink(self.temp_path)
        except FileNotFoundError:
            pass

        self.minio_client.remove_object(self.minio_bucket)


def do_read(
    config: dict,
    pipe_request: io.BufferedReader,
    pipe_response: io.BufferedWriter,
):
    """
    Read input from file and send to output pipe.
    """

    minio_path = config["minio_path"]
    print(f"Perform read operation on file {minio_path}.")
    init_local_file(config, True)

    size = get_input_uint64(pipe_request)
    offset = get_input_uint64(pipe_request)
    print(f"Using size {size} and offset {offset}.")

    try:
        f = config["handle"]
        f.seek(offset, os.SEEK_SET)
        data = f.read(size)
    except OSError as e:
        print("Encountered error during read:", e)
        send_output_int8(pipe_response, -1)
        return

    send_output_int8(pipe_response, 0)  # success status code
    length = len(data)
    send_output_uint64(pipe_response, length)
    pipe_response.write(data)
    print("Done the read operation.")


def do_write(
    config: dict,
    pipe_request: io.BufferedReader,
    pipe_response: io.BufferedWriter,
):
    """
    Write output to file, from input pipe.
    """

    minio_path = config["minio_path"]
    print(f"Perform write operation on file {minio_path}.")
    init_local_file(config, True)  # if create new, create operation done before

    size = get_input_uint64(pipe_request)
    offset = get_input_uint64(pipe_request)
    data = pipe_request.read(size)
    print(f"Using size {size} and offset {offset}.")

    try:
        f = config["handle"]
        f.seek(offset, os.SEEK_SET)
        f.write(data)
        ret_code = 0
    except OSError as e:
        print("Encountered error during write:", e)
        ret_code = -1

    send_output_int8(pipe_response, ret_code)
    print("Done the write operation.")


def do_flush_direct(config: dict) -> dict:
    """
    Does flush operation and backup to MinIO.
    Raises exception on error.
    """
    f = config["handle"]
    if f is None:
        print("No data to flush to file.")
    f.flush()
    copy_to_minio(config["temp_path"], config["minio_path"])


def do_flush(
    config: dict,
    pipe_response: io.BufferedWriter,
):
    """
    Backup output to configured MinIO storage.
    """

    minio_path = config["minio_path"]
    print(f"Perform flush operation on file {minio_path}.")

    try:
        do_flush_direct(config)
        ret_code = 0
    except OSError as e:
        print("Encountered error:", e)
        ret_code = -1

    config["write"] = False
    send_output_int8(pipe_response, ret_code)
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
    operation: str,
    file_pipe_in: str,
    file_pipe_out: str,
    config: dict,
    queue_out: mp.Queue,
) -> list:
    """
    Handle single I/O request.
    Returns list of objects to send on output queue.
    """

    minio_path = config["minio_path"]
    print(f"Do operation {operation} on path {minio_path}.")

    # Open the files for the queues.
    with (
        open(file_pipe_in, "rb") as pipe_request,
        open(file_pipe_out, "wb") as pipe_response,
    ):
        # Handle the specified type of request.
        if operation == "read":
            do_read(config, pipe_request, pipe_response)
        elif operation == "write":
            do_write(config, pipe_request, pipe_response)
        elif operation == "flush":
            do_flush(config, pipe_response, queue_out)
        elif operation == "truncate":
            do_truncate(config, pipe_request, pipe_response, queue_out)
        elif operation == "create":
            do_create(config, pipe_response, queue_out)
            return False
        elif operation == "release":
            do_release(config, pipe_response, queue_out)
            return True
        elif operation == "unlink":
            do_unlink(config, pipe_response, queue_out)
            return True
        else:
            raise NotImplementedError(f"{request_type}: request_type")

    # Some cleanup.
    try:
        print("Remove no longer needed pipe:", file_pipe_in)
        os.unlink(file_pipe_in)
        print("Remove no longer needed pipe:", file_pipe_out)
        os.unlink(file_pipe_out)
    except OSError as e:
        print("Error during cleanup pipes:", e)

    # Indicate done.
    queue_out.put({"is_done": True})
    print(f"Done the {operation} operation.")
