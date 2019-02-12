import pathlib
import re

remote_pattern = re.compile("^(.+):(.+)$")

def normalize_path(p):
    remote_match = remote_pattern.match(str(p))
    if remote_match is not None:
        return remote_match.group(1, 2)
    return pathlib.Path(p).expanduser().resolve()
