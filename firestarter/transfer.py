import sys
import getpass
import os
import re
import tempfile
import typing
import itertools
import operator
import shutil
import pathlib
import yaml
import progressbar
import progressbar as widgets
from collections import defaultdict
from pathlib import Path
from shutil import copyfile
from typing import Union, Sequence, List, Mapping, Tuple, Optional, Text, Dict
from paramiko import SSHClient
from scp import SCPClient
from .util import PathLike


def load_ssh_config(path: Optional[Text] = None) -> Optional[Union[List[Dict], Dict]]:
    if path:
        return yaml.load(path)
    paths = [
        pathlib.Path(os.getcwd(), ".o2_ssh_config"),
        pathlib.Path.home() / ".o2_ssh_config",
    ]
    for p in paths:
        if p.exists():
            config = yaml.safe_load(p.read_text())
            return config if isinstance(config, List) else [config]
    return None


def find_config(host: Text) -> Optional[Dict]:
    if ssh_config is None:
        raise RuntimeError("No config file loaded. Specify user/password directly")
    for c in ssh_config:
        if c["host"] == host:
            return c
    return None


ssh_config = load_ssh_config()


class SCPProgressTracker(object):
    pb_widgets = [
        widgets.Percentage(),
        " ",
        widgets.DataSize(),
        " ",
        widgets.FileTransferSpeed(),
        " ",
        widgets.Timer(),
        " ",
        widgets.AdaptiveETA(),
    ]

    def __init__(self):
        self.pbs = {}

    def __call__(self, filename, size, sent, peername):
        h = (filename, size, peername)
        if h not in self.pbs:
            self.pbs[h] = progressbar.ProgressBar(
                max_value=size,
                widgets=self.pb_widgets,
                # prefix = f"Downloading {filename}"
            )
        self.pbs[h].update(sent)
        if sent == size:
            self.pbs[h].finish()
            del self.pbs[h]


progress_tracker = SCPProgressTracker()


class SCPTransfer(object):
    def __init__(self, host, user=None, keypass=None, quiet=False):
        self.host = host
        config = find_config(host)
        self.user = user or config.get("user") or input(f"Username for {host}: ")
        self.keypass = (
            keypass
            or config.get("keypass")
            or getpass.getpass("Password for keyfile: ")
        )
        self.quiet = quiet
        self._ssh = None
        self._scp = None

    def __enter__(self):
        self.establish_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._scp:
            self._scp.close()
        self._scp = None
        self._ssh = None

    def establish_connection(self):
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(
            hostname=self.host,
            username=self.user,
            passphrase=self.keypass,
            allow_agent=False,
            auth_timeout=20,
        )
        scp = SCPClient(ssh.get_transport(), progress4=progress_tracker)
        self._ssh = ssh
        self._scp = scp

    def get_file_size(self, source: Text) -> Optional[int]:
        # Hacky way to get remote file size by immediately raising exception
        # once transfer has started and size is known
        class SCPFileSizeException(Exception):
            def __init__(self, message, size=None):
                super().__init__(message)
                self.size = size

        def size_progress(filename, size, sent, peername):
            raise SCPFileSizeException("", size=size)

        size = None
        scp = SCPClient(self._ssh.get_transport(), progress4=size_progress)
        try:
            tmp = tempfile.mkstemp()[1]
            scp.get(source, tmp)
        except SCPFileSizeException as e:
            size = e.size
        finally:
            os.remove(tmp)
            scp.close()
        return size

    def get_file(
        self, source: PathLike, destination: PathLike, skip_if_exists: bool = True
    ) -> None:
        source = pathlib.Path(source)
        destination = pathlib.Path(destination)
        if destination.exists():
            destination_size = destination.stat().st_size
            remote_size = self.get_file_size(str(source))
            if destination_size == remote_size:
                print(f"File {source} already copied, skipping.")
                return
        self._scp.get(str(source), str(destination))


def check_transfer_success(p: PathLike) -> None:
    if not Path(p).exists():
        raise RuntimeError(f"Transfer of file to {p} failed!")


remote_pattern = re.compile("^(.+):(.+)$")


def extract_host(p: PathLike) -> Tuple[Text, PathLike]:
    remote_match = remote_pattern.match(str(p))
    if remote_match is None:
        return ("", p)
    return (remote_match[1], remote_match[2])


def transfer_scp_batch(
    host: Text, transfers: Sequence[Tuple[PathLike, PathLike]]
) -> None:
    print(f"Opening connection to {host}")
    with SCPTransfer(host) as scp:
        for o, d in transfers:
            print(f"Copy {Path(o).name} from server to {d}")
            scp.get_file(str(o), str(d))
            check_transfer_success(d)


def transfer_local_batch(transfers: Sequence[Tuple[PathLike, PathLike]]) -> None:
    for o, d in transfers:
        print(f"Copy {Path(o).name} to {d}")
        shutil.copy(str(o), str(d))
        check_transfer_success(d)


def transfer_files_batch(
    files: Mapping[Text, Sequence[Tuple[PathLike, PathLike]]]
) -> Dict[Text, List[Tuple[PathLike, PathLike]]]:
    def aggregate_by_host(transfers):
        keyfunc = operator.itemgetter(0)
        return itertools.groupby(sorted(transfers, key=keyfunc), key=keyfunc)

    def execute_transfers(transfers):
        for host, tlist in aggregate_by_host(transfers):
            tlist = [(o, d) for _, (o, d) in tlist]
            if host == "":
                transfer_local_batch(tlist)
            else:
                transfer_scp_batch(host, tlist)

    file_destinations: Dict[Text, List[Tuple[PathLike, PathLike]]] = defaultdict(list)
    file_transfers = []
    for n, transfers in files.items():
        for o, d in transfers:
            o_host, o_resolved = extract_host(o)
            d_resolved = Path(d) / Path(o).name
            file_destinations[n].append((o, d_resolved))
            file_transfers.append((o_host, (o_resolved, d_resolved)))
    execute_transfers(file_transfers)
    return file_destinations
