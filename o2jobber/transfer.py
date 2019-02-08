import sys
import os
import re
import pathlib
import yaml
import progressbar
from shutil import copyfile
from paramiko import SSHClient
from scp import SCPClient

remote_pattern = re.compile("^(.+):(.+)$")

class SSHConfig(yaml.YAMLObject):
    yaml_tag = "!SSHConfig"

    def __init__(self, host, user, keypass):
        self.host = host
        self.user = user
        self.keypass = keypass

def load_ssh_config(path = None):
    if path:
        return yaml.load(path)
    paths = [
        pathlib.Path(os.getcwd(), ".o2_ssh_config"),
        pathlib.Path.home() / ".o2_ssh_config"
    ]
    for p in paths:
        if p.exists():
            return yaml.load(p.read_text())
    return None

def find_config(host):
    for c in ssh_config:
        if c.host == host:
            return c
    return None

ssh_config = load_ssh_config()
if type(ssh_config) != list:
    ssh_config = [ssh_config]

def wrap_progress_bar(pb):
    def update_progress_bar(filename, size, sent):
        pb.update(sent/size)
    return update_progress_bar

class SCPProgressTracker:
    def __init__(self):
        self.pbs = {}
    
    def __call__(self, filename, size, sent, peername):
        h = hash((filename, size, peername))
        if h not in self.pbs:
            self.pbs[h] = progressbar.ProgressBar(max_value = 100, prefix = f"Downloading {filename}")
        self.pbs[h].update(sent/size)

progress_tracker = SCPProgressTracker()

class SCPTransfer:
    def __init__(self, host, user = None, keypass = None, quiet = False):
        self.host = host
        config = find_config(host)
        self.user = user or config.user
        self.keypass = keypass or config.keypass
        self.quiet = quiet
        self._ssh = None
        self._scp = None
        self.establish_connection()
    
    def establish_connection(self):
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(hostname=self.host, username=self.user, passphrase=self.keypass)
        scp = SCPClient(ssh.get_transport(), progress4=progress_tracker)
        self._ssh = ssh
        self._scp = scp
    
    def get_file(self, source, destination):
        self._scp.get(source, destination)


def transfer_files(files, destination_dirs):
    remotes = [remote_pattern.match(f) for f in files]
    origins = [f if r is None else r.groups(1, 2) for f, r in zip(files, remotes)]
    for o, d in zip(origins, destination_dirs):
        pathlib.Path(d).mkdir(exist_ok = True)
        if type(o) is tuple:
            print(f"Transfering {o[1]} from {o[0]} to {d}")
            scp = SCPTransfer(o[0])
            scp.get_file(o[1], d)
        else:
            print(f"Copying {o} to {d}")
            copyfile(o, d)
