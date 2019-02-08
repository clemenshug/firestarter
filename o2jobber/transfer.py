from paramiko import SSHClient
from scp import SCPClient
import sys

class SCPTransfer:
    def __init__(self, host, user):
        self.host = host
        self.user = user
        self._ssh = None
        self._scp = None
        self.establish_connection()
    
    def establish_connection(self):
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(hostname=self.host, username=self.user)
        scp = SCPClient(ssh.get_transport())
        self._ssh = ssh
        self._scp = scp
    
    def get_file(self, source, destination):
        self._scp.get(source, destination)
