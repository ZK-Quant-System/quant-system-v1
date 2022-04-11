import os


def wrap_path(path: str):
    os.makedirs(path, exist_ok=True)
    return path
