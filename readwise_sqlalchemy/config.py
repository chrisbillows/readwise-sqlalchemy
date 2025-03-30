import os
from pathlib import Path

from dotenv import load_dotenv

class MissingEnvironmentFile(Exception):
    """Custom exception if environment file not available."""

    pass


class UserConfig:
    """Object containing user configuration information."""

    def __init__(
        self, application_dir: Path = Path.home() / "readwise-sqlalchemy-application"
    ):
        """
        Initialise object.

        Attributes
        ----------
        APPLICATION_DIR: pathlib.Path

        ENV_FILE: pathlib.Path

        READWISE_API_TOKEN: str

        """
        self.APPLICATION_DIR: Path = application_dir
        self.APPLICATION_DIR.mkdir(exist_ok=True)
        self.ENV_FILE: Path = self.APPLICATION_DIR / ".env"
        self.load_environment_variables_file()
        self.READWISE_API_TOKEN: str | None = os.getenv("READWISE_API_TOKEN")
        self.DB: Path = self.APPLICATION_DIR / "readwise.db"

    def load_environment_variables_file(self) -> None:
        """
        Load the `.env` file.

        Raises
        ------
        MissingEnvironmentFile
            If the .env file is not in the expected location.
        """
        if self.ENV_FILE.exists():
            load_dotenv(self.ENV_FILE)
        else:
            raise MissingEnvironmentFile(
                "A `.env` file is expected in the `~/readwise-sqlalchemy-application` "
                "directory."
            )

USER_CONFIG = UserConfig()
