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
from shutil import copyfile
from paramiko import SSHClient
from scp import SCPClient
from .util import extract_host


def load_ssh_config(path=None):
    if path:
        return yaml.load(path)
    paths = [
        pathlib.Path(os.getcwd(), ".o2_ssh_config"),
        pathlib.Path.home() / ".o2_ssh_config",
    ]
    for p in paths:
        if p.exists():
            config = yaml.safe_load(p.read_text())
            return config if isinstance(config, typing.List) else [config]
    return None


def find_config(host):
    for c in ssh_config:
        if c["host"] == host:
            return c
    return None


ssh_config = load_ssh_config()
if not isinstance(ssh_config, typing.List):
    ssh_config = [ssh_config]


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

    def get_file_size(self, source):
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

    def get_file(self, source, destination, skip_if_exists=True):
        source = pathlib.Path(source)
        destination = pathlib.Path(destination)
        if destination.exists():
            destination_size = destination.stat().st_size
            remote_size = self.get_file_size(str(source))
            if destination_size == remote_size:
                print(f"File {source} already copied, skipping.")
                return
        self._scp.get(str(source), str(destination))


def transfer_files_batch(files):
    def resolve_location(source, destination):
        source = extract_host(source)
        if type(source) is tuple:
            new_location = pathlib.Path(destination) / pathlib.Path(source[1]).name
            return (source[0], (source[1], new_location))
        new_location = pathlib.Path(destination) / pathlib.Path(source).name
        return (None, (source, new_location))

    def aggregate_by_host(transfers):
        def sortfunc(item):
            k = item[0]
            return k if k is not None else ""

        return itertools.groupby(
            sorted(transfers, key=sortfunc), key=operator.itemgetter(0)
        )

    def check_transfer_success(d):
        if not d.exists():
            raise RuntimeError(f"Transfer of file to {d} failed!")

    def execute_transfers(transfers):
        for host, tlist in aggregate_by_host(transfers):
            if host is None:
                for _, (o, d) in tlist:
                    print(f"Copy {o.stem} to {d}")
                    shutil.copy(str(o), str(d))
                    check_transfer_success(d)
                continue
            print(f"Opening connection to {host}")
            with SCPTransfer(host) as scp:
                for _, (o, d) in tlist:
                    print(f"Copy {pathlib.Path(o).stem} from server to {d}")
                    scp.get_file(str(o), str(d))
                    check_transfer_success(d)

    file_locations = {}
    file_transfers = []
    for n, (o, d) in files.items():
        if isinstance(o, typing.List):
            location_list = []
            for host_location in o:
                host, (remote_location, new_location) = resolve_location(
                    host_location, d
                )
                location_list.append((host_location, new_location))
                file_transfers.append((host, (remote_location, new_location)))
            file_locations[n] = location_list
        else:
            host, (old_location, new_location) = resolve_location(o, d)
            file_locations[n] = [(o, new_location)]
            file_transfers.append((host, (old_location, new_location)))
    execute_transfers(file_transfers)
    return file_locations
