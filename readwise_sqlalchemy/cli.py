"""CLI for Readwise SQLAlchemy sync tool."""

import argparse
import logging
import sys
from argparse import _SubParsersAction
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from readwise_sqlalchemy import __version__
from readwise_sqlalchemy.config import UserConfig, fetch_user_config
from readwise_sqlalchemy.configure_logging import setup_logging
from readwise_sqlalchemy.pipeline import run_pipeline_flattened_objects
from readwise_sqlalchemy.utils import (
    fetch_real_user_data_json_for_end_to_end_testing,
    list_invalid_db_objects,
    readwise_api_fetch_since_custom_date,
)

logger = logging.getLogger(__name__)

# Type hint for subparsers action, used for type checking
# This is a workaround for the fact that _SubParsersAction is not directly importable
# from argparse in some Python < 3.10. Once Python 3.10+ is the minimum version, this
# can be simplified.
if TYPE_CHECKING:
    from argparse import _SubParsersAction

    SubParsersAction = _SubParsersAction[argparse.ArgumentParser]
else:
    SubParsersAction = object


def parse_iso_datetime(value: str) -> str:
    try:
        # Datetime.fromisoformat does not accept 'Z' as a timezone before Python 3.11.
        # We validate without it, but use the Z if it is present when using the value.
        if value.endswith("Z"):
            test_value = value.replace("Z", "+00:00")
        datetime.fromisoformat(test_value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Not an ISOformat string: '{value}'. Must be ISO 8601 e.g. "
            "2025-07-02T14:30Z."
        )
    return value


def setup_readwise_api_subparser(subparsers: "SubParsersAction") -> None:
    readwise_api = subparsers.add_parser(
        "rw-api",
        help="Fetch updates from the Readwise API since a specific ISO formatted "
        "datetime.",
    )
    readwise_api.add_argument(
        "--datetime",
        "-d",
        metavar="ISO 8601 datetime string",
        type=parse_iso_datetime,
        required=True,
        help="Datetime to fetch updates since (e.g. highlights where 'updated_after' is"
        "after the given datetime string). Must be ISO 8601 valid string e.g. "
        "2025-07-02T14:30Z.",
    )
    readwise_api.add_argument(
        "--log-output",
        "-l",
        action="store_true",
        help="If set, writes the fetched data to a file named to your application dir.",
    )


def setup_e2e_data_subparser(subparsers: "SubParsersAction") -> None:
    subparsers.add_parser(
        "e2e-data",
        help="Create a JSON file of *YOUR* user data for rigorous end-2-end testing. "
        "The data never leaves your machine.",
    )


def setup_invalids_subparser(subparsers: "SubParsersAction") -> None:
    """Setup the 'invalids' subparser for the CLI."""
    subparsers.add_parser("list-invalids", help="Report invalid objects.")


def setup_sync_subparser(subparsers: "SubParsersAction") -> None:
    """Setup the 'sync' subparser for the CLI."""
    sync = subparsers.add_parser("sync", help="Run the main Readwise sync pipeline.")
    group = sync.add_mutually_exclusive_group()
    group.add_argument(
        "--delta", action="store_true", help="Run a delta sync (default)."
    )
    group.add_argument("--all", action="store_true", help="Run a full sync.")


def setup_parser() -> argparse.ArgumentParser:
    """Setup the main argument parser for the CLI."""
    parser = argparse.ArgumentParser(description="Readwise CLI sync tool")
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    return parser


def setup_parser_and_subparsers() -> argparse.ArgumentParser:
    """Setup the main argument parser and its subparsers."""
    setup_logging()
    parser = setup_parser()

    subparsers = parser.add_subparsers(dest="command")
    setup_sync_subparser(subparsers)
    setup_invalids_subparser(subparsers)
    setup_e2e_data_subparser(subparsers)
    setup_readwise_api_subparser(subparsers)

    return parser


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = setup_parser_and_subparsers()
    args = parser.parse_args()
    args.command = args.command or "sync"  # Default to 'sync' if no command
    return args


def main(user_config: Optional[UserConfig] = None) -> None:
    """
    Main function that runs with the entry point.

    Parameters
    ----------
    user_config, default = None
        A UserConfig object.
    """
    setup_logging()
    if user_config is None:
        user_config = fetch_user_config()

    args = parse_args()

    if args.command == "sync":
        if args.all:
            logger.info("Running full sync (--all).")
            raise NotImplementedError(
                "Full sync (--all) is not implemented yet. Please use --delta."
            )
            # run_pipeline_flattened_objects(user_config) #(all=True)
        else:
            logger.info("Running delta sync (--delta).")
            run_pipeline_flattened_objects(user_config)

    elif args.command == "list-invalids":
        list_invalid_db_objects(user_config)

    elif args.command == "e2e-data":
        logger.info("Creating end-to-end test data.")
        fetch_real_user_data_json_for_end_to_end_testing(user_config)

    elif args.command == "rw-api":
        logger.info(f"Fetching updates since {args.datetime}.")
        readwise_api_fetch_since_custom_date(
            args.datetime, args.log_output, user_config
        )

    else:
        logger.error("Unknown command. Use --help for usage.")
        sys.exit(1)
