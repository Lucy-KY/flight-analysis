import os
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def _load_private_key(key_path: str) -> bytes:
    """Load RSA private key (encrypted or not) and return DER bytes."""
    p = Path(key_path)
    if not p.is_absolute():
        p = Path(__file__).parent.parent / key_path

    if not p.exists():
        raise FileNotFoundError(f"Private key not found: {p}")

    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    password = passphrase.encode() if passphrase else None

    with open(p, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=password, backend=default_backend()
        )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_conn(schema: str = None) -> snowflake.connector.SnowflakeConnection:
    """
    Return an authenticated Snowflake connection using RSA key pair auth.
    Reads credentials from environment variables (load .env before calling).
    """
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "./rsa_key.p8")
    private_key_der = _load_private_key(key_path)

    return snowflake.connector.connect(
        account       = os.environ["SNOWFLAKE_ACCOUNT"],
        user          = os.environ["SNOWFLAKE_USER"],
        private_key   = private_key_der,
        warehouse     = os.getenv("SNOWFLAKE_WAREHOUSE", "FLIGHT_WH"),
        database      = os.getenv("SNOWFLAKE_DATABASE",  "FLIGHT_DB"),
        schema        = schema or os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        role          = os.getenv("SNOWFLAKE_ROLE",      "TRAINING_ROLE"),
    )
