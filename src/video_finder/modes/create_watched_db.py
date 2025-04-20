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
            return

        print(
            f"Found {len(all_video_hashes)} videos to process for the watched database."
        )
        print(f"Adding/updating entries in watched database: {abs_db_path_to_use}")

        processed_count = 0
        total_videos = len(all_video_hashes)
        for video_path, hashes_list in all_video_hashes.items():
            processed_count += 1
            if not hashes_list:
                logging.warning(
                    f"No hashes generated for video '{video_path}', skipping database entry."
                )
                continue

            video_hashes_set_str = {str(h) for h in hashes_list}

            watched_db_manager.add_video_to_watched_db(
                db_path=abs_db_path_to_use,
                video_identifier=video_path,
                video_hashes_set=video_hashes_set_str,
                num_frames=args.frames,
                hash_size=args.hash_size,
            )

            if processed_count % 50 == 0 or processed_count == total_videos:
                logging.info(
                    f"Processed {processed_count}/{total_videos} videos for watched database."
                )

        print("Watched database creation/update complete.")

    except Exception as e:
        logging.exception(
            f"An unexpected error occurred during watched DB creation: {e}"
        )
        print(
            "\nAn error occurred. Please check the logs or run with -v for more details."
        )
        sys.exit(1)
