"""
Functions for running the forked processes.
"""

import io
import multiprocessing as mp


def get_path(rfile: io.BufferedIOBase) -> str:
    """
    Get null-terminated UTF-8 path from input.
    """
    r = []
    while True:
        ch = rfile.read(1)
        if ch == 0:
            return b"".join(r).decode("UTF-8")
        r.append(ch)


def handler_process(
    num_request: int,
    objects_db,
    requests_db,
    rfile: io.BufferedIOBase,
    timeout: float,
):
    """
    Send metadata object in format server expects.
    """

    # Get type of request and on which path.
    action = rfile.read(1)
    path = get_path(rfile)
    print(f"Perform operation {action} on {path}.")

    # Check if file has already been seen.
    if path in objects_db:
        x = objects_db[path]

        # If active process, send there.
        if x["process"] is not None and x["process"].is_alive():
            # TODO: add locking
            requests_db[num_request] = get_request(action, path, rfile)

        # Done in this case.
        return

    # Need to start new process to handle request.
