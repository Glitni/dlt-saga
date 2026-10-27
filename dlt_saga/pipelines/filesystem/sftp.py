"""SFTP filesystem that verifies the server's host key against ``known_hosts``."""

from typing import Any, Optional

import paramiko
from fsspec.implementations.sftp import SFTPFileSystem


class KnownHostsSFTPFileSystem(SFTPFileSystem):
    """fsspec's SFTP filesystem, refusing servers whose host key isn't pinned.

    fsspec accepts whatever key a server presents on first connection, which
    would let an impersonating server receive the credentials. With
    ``known_hosts`` set, the connection is refused before authenticating unless
    the server's key matches an entry for the host (``[host]:port`` for a
    non-standard port, as ``ssh-keyscan -p`` writes it). Without it, any host
    key is accepted.
    """

    def __init__(self, host: str, known_hosts: Optional[str] = None, **ssh_kwargs: Any):
        # _connect runs inside the parent constructor
        self._known_hosts = known_hosts
        super().__init__(host, **ssh_kwargs)

    def _connect(self) -> None:
        self.client = paramiko.SSHClient()
        if self._known_hosts:
            self.client.load_host_keys(self._known_hosts)
            self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
        else:
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(self.host, **self.ssh_kwargs)
        self.ftp = self.client.open_sftp()
