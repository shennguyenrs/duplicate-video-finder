import logging
import os
import sys

from . import arguments, config, core, utils, watched_db_manager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """Main function to handle command-line arguments and run the video finder."""
    args = arguments.parse_arguments()

    target_directory = args.directory
    abs_target_directory = os.path.abspath(target_directory)  # Use absolute path

    if not os.path.isdir(abs_target_directory):
        logging.error(f"Error: Directory not found: {abs_target_directory}")
        sys.exit(1)  # Exit if directory is invalid

    # --- Display settings being used ---
    print("-" * 30)
    print(f"Starting video processing in: '{abs_target_directory}'")
    print(f"Similarity threshold: {args.threshold}%")
    print(f"Frames sampled per video: {args.frames}")
    print(f"Hash size: {args.hash_size}x{args.hash_size}")
    print(f"Skip duration: {args.skip_duration} seconds")
    cache_path_display = os.path.join(abs_target_directory, args.cache_file + ".db")
    print(f"Using cache file: ~{os.path.relpath(cache_path_display)}")
    print(f"Max workers: {args.workers}")
    print(f"Recursive scan: {'Enabled' if args.recursive else 'Disabled'}")
    if args.watched_db:
        print(f"Using watched database: {args.watched_db}")
        if args.update_watched_db:
            print("Watched database will be updated with new unique videos.")
    print("-" * 30)

    # --- Main Processing Logic ---
    try:
        # Step 1: Calculate Hashes for all videos
        all_video_hashes = core.calculate_all_hashes(
            directory=abs_target_directory,
            recursive=args.recursive,
            cache_filename=args.cache_file,
            num_frames=args.frames,
            hash_size=args.hash_size,
            skip_duration=args.skip_duration,
            max_workers=args.workers,
        )

        if not all_video_hashes:
            print("No video files found or processed. Exiting.")
            return  # Exit if no hashes were generated

        # Initialize variables for the workflow
        videos_to_check_for_duplicates = (
            all_video_hashes.copy()
        )  # Start with all videos
        watched_videos_found = []
        moved_watched_files = set()  # Keep track of files successfully moved as watched
        moved_duplicate_files = (
            set()
        )  # Keep track of files successfully moved as duplicates

        # Step 2: Filter Watched Videos (if --watched-db is provided)
        if args.watched_db:
            print("-" * 30)
            print(f"Loading watched database: {args.watched_db}")
            watched_hashes_set = watched_db_manager.load_watched_hashes(args.watched_db)

            if watched_hashes_set:
                print(
                    f"Comparing {len(all_video_hashes)} videos against {len(watched_hashes_set)} watched hashes..."
                )
                watched_videos_found, videos_to_check_for_duplicates = (
                    core.identify_watched_videos(
                        video_hashes_map=all_video_hashes,
                        watched_hashes_set=watched_hashes_set,
                        hash_size=args.hash_size,
                        similarity_threshold=args.threshold,
                    )
                )
            else:
                print(
                    "Watched database is empty or could not be loaded. Skipping watched check."
                )
                # No change needed, videos_to_check_for_duplicates remains all_video_hashes

            # Prompt to move watched videos if any were found
            if watched_videos_found:
                print("-" * 30)
                print(
                    f"Found {len(watched_videos_found)} video(s) matching the watched database."
                )
                confirm_watched = input(
                    f"Proceed with moving these watched videos to '{config.DEFAULT_WATCHED_DIR_NAME}' subdirectory? [y/N]: "
                )
                if confirm_watched.lower() == "y":
                    print("Moving watched videos...")
                    moved_count, failed_count, moved_watched_files = (
                        utils.move_watched_files(
                            watched_video_paths=watched_videos_found,
                            base_directory=abs_target_directory,
                            watched_subdir=config.DEFAULT_WATCHED_DIR_NAME,
                        )
                    )
                    print(
                        f"Finished moving watched videos: {moved_count} moved, {failed_count} failed."
                    )
                else:
                    print("Move operation for watched videos cancelled by user.")
            else:
                print("No videos matched the watched database.")
            print("-" * 30)
        else:
            # If not using watched_db, videos_to_check_for_duplicates remains all_video_hashes
            pass

        # Step 3: Find Duplicates (among remaining videos)
        print("-" * 30)
        print(
            f"Checking for duplicates among the remaining {len(videos_to_check_for_duplicates)} videos..."
        )
        similar_video_groups = []
        if len(videos_to_check_for_duplicates) >= 2:
            similar_video_groups = core.find_similar_groups(
                video_hashes_map=videos_to_check_for_duplicates,
                hash_size=args.hash_size,
                similarity_threshold=args.threshold,
            )
        else:
            print("Less than two videos remaining, skipping duplicate check.")

        # Print duplicate results
        utils.print_similar_video_groups(similar_video_groups)

        # Prompt to move duplicates if any groups were found
        if similar_video_groups:
            num_duplicates_to_move = sum(
                len(group)
                for group, _ in similar_video_groups  # Count all files in groups to be moved
            )
            if num_duplicates_to_move > 0:
                print("-" * 30)
                print(
                    f"Found {num_duplicates_to_move} file(s) across {len(similar_video_groups)} duplicate groups."
                )
                confirm_duplicates = input(
                    f"Proceed with moving ALL files in these groups to '{config.DEFAULT_DUPLICATE_DIR_NAME}' subdirectory? [y/N]: "
                )
                if confirm_duplicates.lower() == "y":
                    print("Moving duplicates...")
                    moved_count, failed_count = utils.move_duplicate_files(
                        groups=similar_video_groups,
                        base_directory=abs_target_directory,
                        duplicate_subdir=config.DEFAULT_DUPLICATE_DIR_NAME,
                    )
                    print(
                        f"Finished moving duplicates: {moved_count} file(s) moved, {failed_count} failed."
                    )
                    # Record which files were intended to be moved (all files in the groups)
                    # Note: move_duplicate_files doesn't return the set, so we reconstruct it.
                    # We assume if the user confirmed, all files in the groups were *attempted* to be moved.
                    # A more robust approach might involve modifying move_duplicate_files to return the set.
                    for group_set, _ in similar_video_groups:
                        moved_duplicate_files.update(group_set)
                else:
                    print("Move operation for duplicates cancelled by user.")
            # No else needed here, print_similar_video_groups already handles the "no groups found" message.
        print("-" * 30)

        # Step 4: Update Watched DB (if requested)
        if args.watched_db and args.update_watched_db:
            print("-" * 30)
            print(f"Preparing to update watched database: {args.watched_db}")

            # Identify the final set of unique, unwatched videos that were NOT moved
            # Start with videos that were candidates for duplicate checking
            final_unique_paths = set(videos_to_check_for_duplicates.keys())

            # Remove any that were successfully moved as watched (shouldn't be many/any if logic is correct, but be safe)
            final_unique_paths -= moved_watched_files

            # Remove any that were part of duplicate groups the user agreed to move
            final_unique_paths -= moved_duplicate_files

            if not final_unique_paths:
                print(
                    "No new unique, unwatched videos found to add to the watched database."
                )
            else:
                print(
                    f"Found {len(final_unique_paths)} unique, unwatched video(s) to add."
                )
                # Extract all hashes for these final videos
                hashes_to_add = set()
                for path in final_unique_paths:
                    # Get hashes from the original map (or the duplicate check map)
                    if (
                        path in videos_to_check_for_duplicates
                        and videos_to_check_for_duplicates[path]
                    ):
                        # Convert hash objects to strings for storage
                        hashes_to_add.update(
                            str(h) for h in videos_to_check_for_duplicates[path]
                        )
                    else:
                        logging.warning(
                            f"Could not find hashes for final unique video: {path}"
                        )

                if hashes_to_add:
                    print(
                        f"Adding {len(hashes_to_add)} unique hashes to the watched database..."
                    )
                    watched_db_manager.add_hashes_to_watched_db(
                        args.watched_db, hashes_to_add
                    )
                else:
                    print(
                        "No valid hashes found for the unique videos. Database not updated."
                    )
            print("-" * 30)

        print("Processing complete.")

    except Exception as e:
        logging.exception(f"An unexpected error occurred during processing: {e}")
        print(
            "\nAn error occurred. Please check the logs or run with -v for more details."
        )
        sys.exit(1)  # Exit with error code


# --- Entry Point ---
if __name__ == "__main__":
    main()
