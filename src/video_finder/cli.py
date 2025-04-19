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
    if args.create_watched_source:
        modes.run_create_watched_db(args)  # Call via the modes package
    else:
        # Ensure 'directory' argument is present for this mode
        if args.directory is None:
            logging.error(
                "Error: The 'directory' argument is required for find similar/watched mode."
            )
            # Consider printing usage help here if possible/desired
            sys.exit(1)
        modes.run_find_similar(args)  # Call via the modes package


# --- Entry Point ---
if __name__ == "__main__":
    main()
