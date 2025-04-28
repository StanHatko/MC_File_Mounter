"""
Bridge between server process and FSSpec objects.
Converts between socket representation and Python format.
"""


def send_metadata(rfile):
    """
    Send metadata object in format server expects.
    """
