import os
from pathlib import Path

from dotenv import load_dotenv


class MissingEnvironmentFile(Exception):
    """Custom exception if environment file not available."""

    pass


class UserConfig:
    """Object containing user configuration information."""

    def __init__(self, application_dir: Path = Path.home() / "readwise-sqlalchemy"):
        """
        Initialise object.

        Attributes
        ----------
        APPLICATION_DIR: pathlib.Path

        ENV_FILE: pathlib.Path

        READWISE_API_TOKEN: str

        """
        self.app_dir: Path = application_dir
        self.app_dir.mkdir(exist_ok=True)
        self.log_path = self.app_dir / "logs" / "app.log"
        self.env_file: Path = self.app_dir / ".env"
        self.load_environment_variables_file()
        self.readwise_api_token: str | None = os.getenv("READWISE_API_TOKEN")
        self.db_path: Path = self.app_dir / "readwise.db"

    def load_environment_variables_file(self) -> None:
        """
        Load the `.env` file.

        Raises
        ------
        MissingEnvironmentFile
            If the .env file is not in the expected location.
        """
        if self.env_file.exists():
            load_dotenv(self.env_file)
        else:
            raise MissingEnvironmentFile(
                "A `.env` file is expected in the `~/readwise-sqlalchemy-application` "
                "directory."
            )


USER_CONFIG = UserConfig()
