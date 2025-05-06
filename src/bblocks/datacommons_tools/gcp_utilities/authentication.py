import json
import os

from dotenv import load_dotenv


def load_credentials_from_env(env_key: str) -> dict:
    """Return service-account JSON as dict"""

    credentials_json = os.getenv(env_key)
    if credentials_json:
        return json.loads(credentials_json)

    raise EnvironmentError(
        f"Google Cloud credentials are not set in the environment variable {env_key}."
    )


def load_credentials_from_env_file(env_key: str, file_path: str) -> dict:
    """Return service-account JSON as dict"""

    # Check if file exists
    if not os.path.exists(file_path):
        raise EnvironmentError(
            f"Google Cloud credentials file not found at {file_path}."
        )

    load_dotenv(file_path)

    return load_credentials_from_env(env_key)


def load_credentials_from_json(file_path: str) -> dict:
    """Return service-account JSON as dict"""

    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            return json.load(file)

    raise EnvironmentError(
        f"Google Cloud credentials are not found in the file {file_path}."
    )
