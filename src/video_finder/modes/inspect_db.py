import logging
import os
import sys

from .. import watched_db_manager


def run_inspect_db(args):
    """Handles the logic for inspecting a watched video database."""
    db_path = args.inspect_db
    abs_db_path = os.path.abspath(db_path)

    print("-" * 30)
    print("Mode: Inspect Watched Database")
    print(f"Inspecting database file: '{abs_db_path}'")
    print("-" * 30)

    # Note: We don't check os.path.exists here because load_watched_hashes handles
    # the case where the file (or its .db/.dat variants) doesn't exist.

    try:
        # Load hashes and metadata using the absolute path
        # Pass the absolute path to the manager function
        watched_hashes_set, db_metadata = watched_db_manager.load_watched_hashes(
            abs_db_path  # Pass the absolute path
        )

        # load_watched_hashes logs if the file is not found or cannot be opened,
        # so we just report the results it returned.

        print(f"Total unique hashes stored: {len(watched_hashes_set)}")

        if db_metadata:
            print("Hashing Parameters Found:")
            print(
                f"  - Frames sampled per video: {db_metadata.get('num_frames', 'N/A')}"
            )
            print(f"  - Hash size: {db_metadata.get('hash_size', 'N/A')}")
        else:
            # This message is printed if the metadata key is missing OR if the DB
            # file wasn't found/couldn't be loaded (load_watched_hashes returned None).
            # load_watched_hashes already logged the specific reason (e.g., not found, db type error).
            print(
                "Hashing parameter metadata not found or database could not be loaded."
            )
            # Removed the redundant existence check block here

    except Exception as e:
        # Catch potential errors during shelve loading (e.g., corruption)
        # that might not have been caught within load_watched_hashes
        error_msg = f"An unexpected error occurred while trying to read the database file '{abs_db_path}': {e}"
        logging.exception(error_msg)  # Log the full traceback
        print(f"\n{error_msg}")
        print("The database file might be corrupted or inaccessible.")
        sys.exit(1)

    print("-" * 30)
    print("Inspection complete.")
