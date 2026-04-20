"""Tests for SecretStr wrapper and resolver integration."""

import pytest

from dlt_saga.utility.secrets.resolver import SecretResolver, resolve_secret
from dlt_saga.utility.secrets.secret_str import SecretStr, coerce_secret


class TestSecretStr:
    """Tests for SecretStr masking and behavior."""

    def test_repr_masks_value(self):
        s = SecretStr("my-api-key")
        assert repr(s) == "SecretStr('******')"

    def test_str_masks_value(self):
        s = SecretStr("my-api-key")
        assert str(s) == "******"

    def test_fstring_masks_value(self):
        s = SecretStr("my-api-key")
        assert f"token={s}" == "token=******"

    def test_get_secret_value_returns_actual(self):
        s = SecretStr("my-api-key")
        assert s.get_secret_value() == "my-api-key"

    def test_bool_true_for_nonempty(self):
        assert bool(SecretStr("value")) is True

    def test_bool_false_for_empty(self):
        assert bool(SecretStr("")) is False

    def test_equality_same_value(self):
        assert SecretStr("abc") == SecretStr("abc")

    def test_equality_different_value(self):
        assert SecretStr("abc") != SecretStr("xyz")

    def test_equality_not_implemented_for_str(self):
        assert SecretStr("abc") != "abc"

    def test_hash_same_for_equal_values(self):
        assert hash(SecretStr("abc")) == hash(SecretStr("abc"))

    def test_hash_different_for_different_values(self):
        # Not guaranteed by spec, but extremely unlikely to collide
        assert hash(SecretStr("abc")) != hash(SecretStr("xyz"))

    def test_usable_as_dict_key(self):
        d = {SecretStr("key"): "value"}
        assert d[SecretStr("key")] == "value"

    def test_rejects_non_string(self):
        with pytest.raises(TypeError, match="expects str"):
            SecretStr(123)

    def test_rejects_none(self):
        with pytest.raises(TypeError, match="expects str"):
            SecretStr(None)


class TestCoerceSecret:
    """Tests for the coerce_secret helper."""

    def test_coerce_str_to_secret(self):
        result = coerce_secret("my-secret")
        assert isinstance(result, SecretStr)
        assert result.get_secret_value() == "my-secret"

    def test_coerce_secret_str_passthrough(self):
        original = SecretStr("my-secret")
        result = coerce_secret(original)
        assert result is original

    def test_coerce_none_returns_none(self):
        assert coerce_secret(None) is None

    def test_coerce_rejects_int(self):
        with pytest.raises(TypeError, match="Expected str, SecretStr, or None"):
            coerce_secret(123)

    def test_coerce_empty_string(self):
        result = coerce_secret("")
        assert isinstance(result, SecretStr)
        assert result.get_secret_value() == ""


class TestResolverSecretStrIntegration:
    """Tests that SecretResolver handles SecretStr input transparently."""

    def test_is_secret_uri_with_secret_str(self):
        uri = SecretStr("googlesecretmanager::project::secret-name")
        assert SecretResolver.is_secret_uri(uri) is True

    def test_is_secret_uri_with_plain_secret_str(self):
        plain = SecretStr("plain-value")
        assert SecretResolver.is_secret_uri(plain) is False

    def test_resolve_plain_secret_str(self):
        """Non-URI SecretStr should be returned as plain string."""
        result = resolve_secret(SecretStr("plain-value"))
        assert result == "plain-value"
        assert isinstance(result, str)

    def test_resolve_none_passthrough(self):
        assert resolve_secret(None) is None

    def test_resolve_int_passthrough(self):
        assert resolve_secret(42) == 42


class TestConfigDataclassCoercion:
    """Tests that config dataclasses coerce str fields to SecretStr."""

    def test_api_config_coerces_auth_token(self):
        from dlt_saga.pipelines.api.config import ApiConfig

        config = ApiConfig(
            base_url="https://api.example.com",
            endpoint="/v1/data",
            auth_token="my-token",
        )
        assert isinstance(config.auth_token, SecretStr)
        assert config.auth_token.get_secret_value() == "my-token"

    def test_api_config_none_auth_token(self):
        from dlt_saga.pipelines.api.config import ApiConfig

        config = ApiConfig(
            base_url="https://api.example.com",
            endpoint="/v1/data",
        )
        assert config.auth_token is None

    def test_database_config_coerces_credentials(self):
        from dlt_saga.pipelines.database.config import DatabaseConfig

        config = DatabaseConfig(
            database_type="postgres",
            host="localhost",
            source_database="mydb",
            source_table="mytable",
            username="user",
            password="pass",
            connection_string="postgresql://user:pass@localhost/db",
        )
        assert isinstance(config.username, SecretStr)
        assert isinstance(config.password, SecretStr)
        assert isinstance(config.connection_string, SecretStr)
        assert config.username.get_secret_value() == "user"

    def test_filesystem_config_coerces_sftp_credentials(self):
        from dlt_saga.pipelines.filesystem.config import FilesystemConfig

        config = FilesystemConfig(
            filesystem_type="sftp",
            bucket_name="/data",
            file_type="csv",
            file_glob="*.csv",
            hostname="sftp.example.com",
            username="sftpuser",
            password="sftppass",
        )
        assert isinstance(config.username, SecretStr)
        assert isinstance(config.password, SecretStr)
        assert config.username.get_secret_value() == "sftpuser"

    def test_repr_does_not_leak_secrets(self):
        """Verify that dataclass repr masks secret fields."""
        from dlt_saga.pipelines.database.config import DatabaseConfig

        config = DatabaseConfig(
            database_type="postgres",
            host="localhost",
            source_database="mydb",
            source_table="mytable",
            password="super-secret-pass",
            connection_string="postgresql://user:super-secret-pass@localhost/db",
        )
        config_repr = repr(config)
        assert "super-secret-pass" not in config_repr
        assert "******" in config_repr


class TestCredentialFieldsDefaultToNone:
    """Tests that credential fields are None when not provided (no magic defaults)."""

    def test_api_config_auth_token_none_when_omitted(self):
        from dlt_saga.pipelines.api.config import ApiConfig

        config = ApiConfig(base_url="https://api.example.com", endpoint="/v1/data")
        assert config.auth_token is None

    def test_database_config_credentials_none_when_omitted(self):
        from dlt_saga.pipelines.database.config import DatabaseConfig

        config = DatabaseConfig(
            database_type="postgres",
            host="localhost",
            source_database="mydb",
            source_table="mytable",
        )
        assert config.username is None
        assert config.password is None
        assert config.connection_string is None
