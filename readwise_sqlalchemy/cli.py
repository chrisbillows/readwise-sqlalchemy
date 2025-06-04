"""
CLI for readwise_sqlalchemy: debug and utility commands.
"""
import argparse
from readwise_sqlalchemy import main
from readwise_sqlalchemy.config import USER_CONFIG, MissingEnvironmentFile
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import sys
from readwise_sqlalchemy.report_invalid_db_objects import report_invalid_db_objects

def main_cli():
    parser = argparse.ArgumentParser(description="Readwise SQLAlchemy CLI utilities.")
    subparsers = parser.add_subparsers(dest='command')

    parser_sync = subparsers.add_parser('sync', help='Run the main Readwise sync pipeline (default if no command).')
    parser_report = subparsers.add_parser('report-invalids', help='Report books with invalid children.')

    args = parser.parse_args()
    # Default to sync if no command is given
    command = args.command or 'sync'
    if command == 'sync':
        try:
            main.main()
        except MissingEnvironmentFile as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    elif command == 'report-invalids':
        try:
            report_invalid_db_objects()
        except MissingEnvironmentFile as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        parser.print_help()

if __name__ == "__main__":
    main_cli()
