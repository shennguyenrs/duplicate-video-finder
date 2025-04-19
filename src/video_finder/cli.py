import logging
import sys

from . import arguments, modes

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """Main function to handle command-line arguments and run the video finder."""
    args = arguments.parse_arguments()

    # --- Mode Selection Logic ---
    if args.inspect_db:
        modes.run_inspect_db(args)  # Call inspect mode
    elif args.create_watched_source:
        modes.run_create_watched_db(args)  # Call create mode
    elif args.directory:
        # 'directory' is the default positional argument for find_similar mode
        modes.run_find_similar(args)  # Call find similar mode
    else:
        # This case should ideally not be reachable if argparse is set up correctly
        # with mutually exclusive groups and required flags/positionals.
        logging.error(
            "Error: No valid operation mode selected or required arguments missing."
        )
        # Consider printing parser help here
        arguments.parser.print_help()  # Assuming parser is accessible
        sys.exit(1)


# --- Entry Point ---
if __name__ == "__main__":
    main()
