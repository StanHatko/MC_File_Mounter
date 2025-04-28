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

from process import handler_process


class FileSocketServer(socketserver.StreamRequestHandler):
    """
    Class that responds to requests from FUSE connector and sends to Python processes.
    """

    num_request = 0
    manager = None
    objects_db = None
    requests_db = None

    def handle(self):
        """
        Handle requests to server.
        Transfers control to subprocess.
        """
        mp.Process(
            target=handler_process,
            args=(
                self.num_request,
                self.objects_db,
                self.rfile,
                self.timeout,
            ),
        )
        self.num_request += 1


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
        server.requests_db = manager.dict()
        server.serve_forever()


if __name__ == "__main__":
    main()
