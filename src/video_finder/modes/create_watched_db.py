import logging
import os
import sys

from .. import config, core, utils, watched_db_manager


def run_create_watched_db(args):
    """Handles the logic for creating the watched video database."""
    source_directory = args.create_watched_source
    abs_source_directory = os.path.abspath(source_directory)

    if not os.path.isdir(abs_source_directory):
        logging.error(f"Error: Source directory not found: {abs_source_directory}")
        sys.exit(1)

    # Determine the watched DB path
    if args.watched_db:
        db_path_to_use = args.watched_db
    else:
        db_path_to_use = os.path.join(
            abs_source_directory, config.DEFAULT_WATCHED_DB_FILENAME
        )
    abs_db_path_to_use = os.path.abspath(db_path_to_use)

    # Display settings using the utility function
    utils.display_settings(
        args,
        "Create Watched Database",
        abs_source_directory,
        db_path=abs_db_path_to_use,
        cache_dir=abs_source_directory,
    )

    try:
        # Calculate hashes for all videos in the source directory
        all_video_hashes = core.calculate_all_hashes(
            directory=abs_source_directory,
            recursive=args.recursive,
            cache_filename=args.cache_file,
            num_frames=args.frames,
            hash_size=args.hash_size,
            skip_duration=args.skip_duration,
            max_workers=args.workers,
        )

        if not all_video_hashes:
            print("No video files found or processed in the source directory.")
            return  # Exit cleanly

        # Collect all unique hashes
        all_hashes_set = set()
        for hashes_list in all_video_hashes.values():
            if hashes_list:
                all_hashes_set.update(str(h) for h in hashes_list)

        if not all_hashes_set:
            print("No valid hashes generated from the videos found.")
            return  # Exit cleanly

        print(
            f"Found {len(all_video_hashes)} videos, generating {len(all_hashes_set)} unique hashes."
        )
        print(f"Adding hashes to watched database: {abs_db_path_to_use}")

        # Add hashes to the database, including metadata
        watched_db_manager.add_hashes_to_watched_db(
            db_path=abs_db_path_to_use,
            new_hashes_set=all_hashes_set,
            num_frames=args.frames,
            hash_size=args.hash_size,
        )

        print("Watched database update complete.")

    except Exception as e:
        logging.exception(
            f"An unexpected error occurred during watched DB creation: {e}"
        )
        print(
            "\nAn error occurred. Please check the logs or run with -v for more details."
        )
        sys.exit(1)
