import logging
import sys

from . import arguments, modes

# Set up logging to both console and file
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)

# File handler
file_handler = logging.FileHandler("video_finder.log")
file_handler.setFormatter(log_formatter)

# Root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(console_handler)
root_logger.addHandler(file_handler)


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
