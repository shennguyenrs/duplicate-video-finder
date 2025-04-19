import logging
import os
import sys

from .. import config, core, watched_db_manager


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

    print("-" * 30)
    print("Mode: Create Watched Database")
    print(f"Scanning source directory: '{abs_source_directory}'")
    print(f"Frames sampled per video: {args.frames}")
    print(f"Hash size: {args.hash_size}x{args.hash_size}")
    print(f"Skip duration: {args.skip_duration} seconds")
    cache_path_display = os.path.join(abs_source_directory, args.cache_file + ".db")
    print(f"Using cache file: ~{os.path.relpath(cache_path_display)}")
    print(f"Max workers: {args.workers}")
    print(f"Recursive scan: {'Enabled' if args.recursive else 'Disabled'}")
    print("-" * 30)

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

        # Add hashes to the database
        watched_db_manager.add_hashes_to_watched_db(abs_db_path_to_use, all_hashes_set)

        print("Watched database update complete.")

    except Exception as e:
        logging.exception(
            f"An unexpected error occurred during watched DB creation: {e}"
        )
        print(
            "\nAn error occurred. Please check the logs or run with -v for more details."
        )
        sys.exit(1)
