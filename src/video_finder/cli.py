import logging
import sys

from . import arguments, modes

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """Handle command-line arguments and run the selected video finder mode."""
    args = arguments.parse_arguments()

    if args.inspect_db:
        modes.run_inspect_db(args)
    elif args.create_watched_source:
        modes.run_create_watched_db(args)
    elif args.directory:
        modes.run_find_similar(args)
    else:
        logging.error(
            "Error: No valid operation mode selected or required arguments missing."
        )
        arguments.parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
