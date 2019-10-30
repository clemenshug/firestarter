import csv
import hashlib
import os
import pathlib
import re
import shutil
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


def rstrip_extra(fname):
    """Strip extraneous, non-discriminative filename info from the end of a file.
    """
    to_strip = ("_R", ".R", "-R", "_", "fastq", ".", "-")
    while fname.endswith(to_strip):
        for x in to_strip:
            if fname.endswith(x):
                fname = fname[: len(fname) - len(x)]
                break
    return fname


def sort_filenames(filenames):
    """
    sort a list of files by filename only, ignoring the directory names
    """
    basenames = [os.path.basename(x) for x in filenames]
    indexes = [i[0] for i in sorted(enumerate(basenames), key=lambda x: x[1])]
    return [filenames[x] for x in indexes]


def combine_pairs(input_files, force_single=False, full_name=False, separators=None):
    """ calls files pairs if they are completely the same except
    for one has _1 and the other has _2 returns a list of tuples
    of pairs or singles.
    From bipy.utils (https://github.com/roryk/bipy/blob/master/bipy/utils.py)
    Adjusted to allow different input paths or extensions for matching files.
    """
    PAIR_FILE_IDENTIFIERS = set(["1", "2", "3", "4"])

    pairs = []
    used = set([])
    used_separators = set([])
    separators = separators if separators else ("R", "_", "-", ".")
    for in_file in input_files:
        matches = set([])
        if in_file in used:
            continue
        if not force_single:
            for comp_file in input_files:
                if comp_file in used or comp_file == in_file:
                    continue
                if full_name:
                    in_file_name = in_file
                    comp_file_name = comp_file
                else:
                    in_file_name = os.path.basename(in_file)
                    comp_file_name = os.path.basename(comp_file)

                a = rstrip_extra(splitext_plus(in_file_name)[0])
                b = rstrip_extra(splitext_plus(comp_file_name)[0])
                if len(a) != len(b):
                    continue
                s = str_diff(a, b)
                # no differences, then its the same file stem
                if len(s) == 0:
                    raise ValueError(
                        "%s and %s have the same stem, so we don't know "
                        "how to assign it to the sample data in the CSV. To "
                        "get around this you can rename one of the files. "
                        "If they are meant to be the same sample run in two "
                        "lanes, combine them first with the "
                        "bcbio_prepare_samples.py script."
                        "(http://bcbio-nextgen.readthedocs.io/en/latest/contents/configuration.html#multiple-files-per-sample)"
                        % (in_file, comp_file)
                    )
                if len(s) > 1:
                    continue  # there is more than 1 difference
                if (
                    a[s[0]] in PAIR_FILE_IDENTIFIERS
                    and b[s[0]] in PAIR_FILE_IDENTIFIERS
                ):
                    # if the 1/2 isn't the last digit before a separator, skip
                    # this skips stuff like 2P 2A, often denoting replicates, not
                    # read pairings
                    if len(b) > (s[0] + 1):
                        if b[s[0] + 1] not in ("_", "-", "."):
                            continue
                    # if the 1/2 is not a separator or prefaced with R, skip
                    if b[s[0] - 1] in separators:
                        used_separators.add(b[s[0] - 1])
                        if len(used_separators) > 1:
                            print(
                                "To split into paired reads multiple separators were used: %s"
                                % used_separators
                            )
                            print("This can lead to wrong assignation.")
                            print(
                                "Use --separator option in bcbio_prepare_samples.py to specify only one."
                            )
                            print("For instance, --separator R.")
                        matches.update([in_file, comp_file])
                        used.update([in_file, comp_file])

            if matches:
                pairs.append(sort_filenames(list(matches)))
        if in_file not in used:
            pairs.append([in_file])
            used.add(in_file)
    return pairs


# https://stackoverflow.com/a/3431838/4603385
def file_md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
