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


class FileSocketServer(socketserver.StreamRequestHandler):
    """
    Class that responds to requests from FUSE connector and sends to Python processes.
    """

    objects_db = {}

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

    def get_cached_metadata(self, path: str) -> bool:
        TODO

    def send_object_process(action, action: bytes, path: str) -> bool:
        TODO

    def start_object_process(action, action: bytes, path: str):
        TODO

    def handle(self):
        """
        Handle requests to server.
        Runs synchronously, so while function running other requests queue.
        Call out to other objects as fast as possible.
        """

        # Get type of request and on which path.
        action = self.rfile.read(1)
        path = self.get_path()

        # For get metadata, check cache first.
        if action == b"G" and self.get_cached_metadata(path):
            return

        # If process already exists, send there.
        if self.send_object_process(action, path):
            return

        # Need to start new process to handle this request.
        self.start_object_process(action, path)


def main():
    """
    Function invoked when this program is run from command line.
    """

    with socketserver.UnixStreamServer(
        "/tmp/fs_server.socket",  # TODO: make configurable
        FileSocketServer,
    ) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
