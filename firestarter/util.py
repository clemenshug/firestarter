import csv
import hashlib
import os
import pathlib
import re
import shutil
from collections import defaultdict
from pathlib import Path
from typing import List, Mapping, Optional, Sequence, Tuple, Union

PathLike = Union[Path, str]


remote_pattern = re.compile("^(.+):(.+)$")


def normalize_path(p: PathLike) -> PathLike:
    remote_match = remote_pattern.match(str(p))
    if remote_match is not None:
        return str(p)
    return pathlib.Path(p).expanduser().resolve()


def concatenate_files(source_files: Sequence[PathLike], destination: PathLike) -> None:
    if not all(Path(p).exists() for p in source_files):
        raise ValueError("One of the source files doesn't exist")
    with open(destination, "wb") as out_handle:
        for s in source_files:
            with open(s, "rb") as in_handle:
                shutil.copyfileobj(in_handle, out_handle, length=16 * 1024 * 1024)


def merge_files(
    files: Sequence[PathLike], destination: PathLike, skip_if_exists: bool = True
) -> Path:
    files = [Path(p) for p in files]
    destination = Path(destination)
    if destination.exists():
        combined_size = sum(f.stat().st_size for f in files)
        dest_size = destination.stat().st_size
        if dest_size == combined_size:
            print(
                f"Merged file {destination} already exists with correct size, skipping."
            )
            return destination
        print(
            f"Merged file {destination} already exists, {dest_size} instead of {combined_size}"
        )
    print("Merging", " ".join(str(f) for f in files), "into", destination)
    concatenate_files(files, destination)
    return destination


def parse_csv(p: PathLike):
    with open(p, "r") as f:
        reader = csv.DictReader(f)
        csv_contents = list(reader)
    return csv_contents


def str_diff(a, b):
    """ copy from http://stackoverflow.com/a/8545526 """
    return [i for i in range(len(a)) if a[i] != b[i]]


def splitext_plus(f):
    """Split on file extensions, allowing for zipped extensions.
    """
    base, ext = os.path.splitext(f)
    if ext in [".gz", ".bz2", ".zip"]:
        base, ext2 = os.path.splitext(base)
        ext = ext2 + ext
    return base, ext


def sort_filenames(filenames):
    """
    sort a list of files by filename only, ignoring the directory names
    """
    basenames = [os.path.basename(x) for x in filenames]
    indexes = [i[0] for i in sorted(enumerate(basenames), key=lambda x: x[1])]
    return [filenames[x] for x in indexes]


def combine_pairs(files, pair_patterns = [r"_R(\d)_\d{3}\.fastq", r"_(\d)\.fastq"]):
    regexes = [re.compile(p) for p in pair_patterns]
    pairs = defaultdict(list)
    for f in files:
        for r in regexes:
            m = r.search(f)
            if m is None:
                continue
            pairs[int(m.group(1))].append(f)
            break
        # If none of the regexes match assume it's read 1
        else:
            pairs[1].append(f)
    return pairs


# https://stackoverflow.com/a/3431838/4603385
def file_md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
