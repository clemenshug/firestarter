import sys
import os
import re
import typing
import itertools
import operator
import shutil
import pathlib
import yaml
import progressbar
from shutil import copyfile
from paramiko import SSHClient
from scp import SCPClient

remote_pattern = re.compile("^(.+):(.+)$")

def load_ssh_config(path = None):
    if path:
        return yaml.load(path)
    paths = [
        pathlib.Path(os.getcwd(), ".o2_ssh_config"),
        pathlib.Path.home() / ".o2_ssh_config"
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

def wrap_progress_bar(pb):
    def update_progress_bar(filename, size, sent):
        pb.update(sent/size)
    return update_progress_bar

class SCPProgressTracker(object):
    def __init__(self):
        self.pbs = {}
    
    def __call__(self, filename, size, sent, peername):
        h = hash((filename, size, peername))
        if h not in self.pbs:
            self.pbs[h] = progressbar.ProgressBar(
                max_value = 100,
                prefix = f"Downloading {filename}"
            )
        self.pbs[h].update(sent/size)

progress_tracker = SCPProgressTracker()

class SCPTransfer(object):
    def __init__(self, host, user = None, keypass = None, quiet = False):
        self.host = host
        config = find_config(host)
        self.user = user or config["user"]
        self.keypass = keypass or config["keypass"]
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
        ssh.connect(hostname=self.host, username=self.user,
            passphrase=self.keypass, auth_timeout=20)
        scp = SCPClient(ssh.get_transport(), progress4=progress_tracker)
        self._ssh = ssh
        self._scp = scp
    
    def get_file(self, source, destination):
        self._scp.get(source, destination)


# def transfer_file(source, destination):
#     pathlib.Path(destination).mkdir(exist_ok = True)
#     remote_match = remote_pattern.match(str(source))
#     if remote_match is not None:
#         source = remote_match.group(1, 2)
#     if type(source) is tuple:
#         print(f"Transfering {source[1]} from {source[0]} to {destination}")
#         scp = SCPTransfer(source[0])
#         scp.get_file(str(source[1]), str(destination))
#         new = pathlib.Path(destination) / pathlib.Path(source[1]).name
#     else:
#         new = source
#     #     print(f"Copying {o} to {d}")
#     #     copyfile(o, d)
#     if not new.exists():
#         raise RuntimeError(f"File {source} does not exist at {new}"
#                             "Did the transfer fail?")
#     return new

def transfer_files_batch(files):
    # files = files if type(files) is list else [files]
    # destination_dirs = destination_dirs if type(destination_dirs) is list else destination_dirs
    # remotes = [remote_pattern.match(f) for f in files]
    # origins = [f if r is None else r.group(1, 2) for f, r in zip(files, remotes)]
    # new_locations = []
    def resolve_location(source, destination):
        remote_match = remote_pattern.match(str(source))
        if remote_match is not None:
            remote_source = remote_match.group(1, 2)
            new_location = pathlib.Path(destination) / pathlib.Path(remote_source[1]).name
            return (remote_source[0], (remote_source[1], new_location))
        new_location = pathlib.Path(destination) / pathlib.Path(source).name
        return (None, (source, new_location))
    def aggregate_by_host(transfers):
        def sortfunc(item):
            k = item[0]
            return k if k is not None else ""
        return itertools.groupby(sorted(transfers, key=sortfunc), key=operator.itemgetter(0))
    def check_transfer_success(d):
        if not d.exists():
            raise RuntimeError(f"Transfer of file to {d} failed!") 
    def execute_transfers(transfers):
        for host, tlist in aggregate_by_host(transfers):
            if host is None:
                for _, (o, d) in tlist:
                    shutil.copy(str(o), str(d))
                    check_transfer_success(d)
                continue
            with SCPTransfer(host) as scp:
                for _, (o, d) in tlist:
                    scp.get_file(str(o), str(d))
                    check_transfer_success(d)
    file_locations = {}
    file_transfers = []
    for n, (o, d) in files.items():
        if isinstance(o, typing.List):
            location_list = []
            for o_ in o:
                host, (old_location, new_location) = resolve_location(o_, d)
                location_list.append(new_location)
                file_transfers.append((host, (old_location, new_location)))
            file_locations[n] = location_list
        else:
            host, (old_location, new_location) = resolve_location(o, d)
            file_locations[n] = new_location
            file_transfers.append((host, (old_location, new_location)))
    execute_transfers(file_transfers)
    return file_locations
