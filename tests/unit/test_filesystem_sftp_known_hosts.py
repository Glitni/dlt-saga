"""SFTP ``known_hosts`` pins the server's host key.

The value used to be passed to dlt under a key its SFTP credentials don't have,
so it was dropped and fsspec accepted whatever host key the server presented.
"""

from unittest.mock import MagicMock, patch

import paramiko
import pytest

from dlt_saga.pipelines.filesystem.client import FilesystemClient
from dlt_saga.pipelines.filesystem.config import FilesystemConfig
from dlt_saga.pipelines.filesystem.sftp import KnownHostsSFTPFileSystem

_ENTRY = "sftp.example.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExampleKeyOnly"


def _sftp_client(**overrides) -> FilesystemClient:
    return FilesystemClient(
        FilesystemConfig(
            **{
                "filesystem_type": "sftp",
                "hostname": "sftp.example.com",
                "bucket_name": "data",
                "file_type": "csv",
                "file_glob": "*.csv",
                "username": "user",
                "password": "pass",
                **overrides,
            }
        )
    )


def _known_hosts_passed(client: FilesystemClient) -> object:
    with patch("dlt_saga.pipelines.filesystem.sftp.KnownHostsSFTPFileSystem") as opener:
        client.get_filesystem_kwargs()
    return opener.call_args.kwargs["known_hosts"]


@pytest.mark.unit
class TestKnownHostsResolution:
    def test_unset_passes_none(self):
        assert _known_hosts_passed(_sftp_client()) is None

    def test_file_path_passed_as_is(self, tmp_path):
        path = tmp_path / "known_hosts"
        path.write_text(_ENTRY + "\n")

        assert _known_hosts_passed(_sftp_client(known_hosts=str(path))) == str(path)

    @pytest.mark.parametrize(
        "content",
        [
            _ENTRY,
            # A single entry whose key type isn't ssh-*
            "sftp.example.com ecdsa-sha2-nistp256 AAAAE2VjZHNhExampleKeyOnly",
            f"{_ENTRY}\nother.example.com {_ENTRY.split(' ', 1)[1]}\n",
        ],
    )
    def test_content_written_to_a_file(self, content):
        path = _known_hosts_passed(_sftp_client(known_hosts=content))

        with open(path) as f:
            assert f.read() == content

    def test_missing_file_is_a_config_error(self):
        with pytest.raises(ValueError, match="neither an existing file"):
            _known_hosts_passed(_sftp_client(known_hosts="/no/such/known_hosts"))


def _connect(known_hosts):
    """Open the filesystem with a mocked SSH client; return the client mock."""
    ssh_client = MagicMock()
    with patch.object(paramiko, "SSHClient", return_value=ssh_client):
        KnownHostsSFTPFileSystem(
            "sftp.example.com",
            known_hosts=known_hosts,
            skip_instance_cache=True,
            username="user",
        )
    return ssh_client


@pytest.mark.unit
class TestHostKeyPolicy:
    def test_pinned_host_keys_loaded_and_unknown_hosts_rejected(self):
        ssh_client = _connect("/etc/saga/known_hosts")

        ssh_client.load_host_keys.assert_called_once_with("/etc/saga/known_hosts")
        (policy,) = ssh_client.set_missing_host_key_policy.call_args.args
        assert isinstance(policy, paramiko.RejectPolicy)

    def test_host_key_checked_before_authenticating(self):
        """The policy is in place before connect(), which sends the credentials."""
        ssh_client = _connect("/etc/saga/known_hosts")

        calls = [name for name, _, _ in ssh_client.method_calls]
        assert calls.index("load_host_keys") < calls.index("connect")
        assert calls.index("set_missing_host_key_policy") < calls.index("connect")

    def test_unpinned_accepts_any_host_key(self):
        ssh_client = _connect(None)

        ssh_client.load_host_keys.assert_not_called()
        (policy,) = ssh_client.set_missing_host_key_policy.call_args.args
        assert isinstance(policy, paramiko.AutoAddPolicy)

    def test_connection_arguments_forwarded(self):
        ssh_client = _connect(None)

        ssh_client.connect.assert_called_once_with("sftp.example.com", username="user")
