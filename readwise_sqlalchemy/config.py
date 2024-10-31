from pathlib import Path
from dotenv import load_dotenv

ENV_FILE = Path.home() / "readwise-sqlalchemy-application" / ".env"

class MissingEnvironmentFile(Exception):
    """Custom exception if environment file is not in the expected location."""
    pass

def load_environment_variables_file():
    """Load the `.env` file if its present in the expected location. Else raise an error."""
    if ENV_FILE.exists():
        load_dotenv(ENV_FILE)
    else:
        raise MissingEnvironmentFile("A `.env` file is expected in the `~/readwise-sqlalchemy-application` directory.")

load_environment_variables_file()
