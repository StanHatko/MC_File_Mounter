"""
Main backend server that handles requests from FUSE process.
Uses separate process for each open file.
"""

import io
import multiprocessing as mp
import os
import socket
import socketserver
import sys
import tempfile

import fsspec


def get_path(self) -> str:
    """
    Get null-terminated UTF-8 path from input.
    """
    r = []
    while True:
        ch = self.rfile.read(1)
        if ch == 0:
            return b"".join(r).decode("UTF-8")
        r.append(ch)


def handler_process(manager, rfile: io.BufferedIOBase, timeout: float):
    """
    Send metadata object in format server expects.
    """

    # Get type of request and on which path.
    action = rfile.read(1)
    path = get_path()
    print(f"Perform operation {action} on {path}.")

    # TODO


class FileSocketServer(socketserver.StreamRequestHandler):
    """
    Class that responds to requests from FUSE connector and sends to Python processes.
    """

    manager = None
    objects_db = None

    def handle(self):
        """
        Handle requests to server.
        Runs synchronously, so while function running other requests queue.
        Call out to other objects as fast as possible.
        """

        # Transfer control to subprocess.
        mp.Process(
            target=handler_process,
            args=(
                self.objects_db,
                self.rfile,
                self.timeout,
            ),
        )


def main():
    """
    Function invoked when this program is run from command line.
    """
    mp.set_start_method("fork", force=True)
    with (
        socketserver.UnixStreamServer(
            "/tmp/fs_server.socket",  # TODO: make configurable
            FileSocketServer,
        ) as server,
        mp.Manager() as manager,
    ):
        server.manager = manager
        server.objects_db = manager.dict()
        server.serve_forever()


if __name__ == "__main__":
    main()
