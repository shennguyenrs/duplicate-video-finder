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

    try:
        # Load video data and metadata using the absolute path
        watched_videos_dict, db_metadata = watched_db_manager.load_watched_videos_data(
            abs_db_path
        )

        print(f"Total videos stored: {len(watched_videos_dict)}")

        if db_metadata:
            print("Hashing Parameters Found (apply to all entries):")
            print(
                f"  - Frames sampled per video: {db_metadata.get('num_frames', 'N/A')}"
            )
            print(f"  - Hash size: {db_metadata.get('hash_size', 'N/A')}")
        else:
            print(
                "Hashing parameter metadata not found or database could not be loaded."
            )

    except Exception as e:
        error_msg = f"An unexpected error occurred while trying to read the database file '{abs_db_path}': {e}"
        logging.exception(error_msg)
        print(f"\n{error_msg}")
        print("The database file might be corrupted or inaccessible.")
        sys.exit(1)

    print("-" * 30)
    print("Inspection complete.")
