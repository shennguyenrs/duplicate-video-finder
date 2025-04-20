import logging
import os
import sys

from .. import config, core, utils, watched_db_manager

# --- Helper Functions ---


def _handle_watched_videos(args, all_video_hashes, abs_target_directory):
    """
    Loads watched DB, validates parameters, identifies watched videos,
    prompts for moving, and moves them.

    Returns:
        tuple: (videos_to_check_for_duplicates, moved_watched_files_set)
               - dict: video paths -> hashes (subset of all_video_hashes)
               - set: paths of files successfully moved as watched
    """
    print("-" * 30)
    print(f"Loading watched database: {args.watched_db}")
    # Load hashes AND metadata
    watched_hashes_set, db_metadata = watched_db_manager.load_watched_hashes(
        args.watched_db
    )

    # --- Parameter Validation ---
    if db_metadata:
        db_frames = db_metadata.get("num_frames")
        db_hash_size = db_metadata.get("hash_size")

        # Check for mismatch only if metadata values exist
        mismatch = False
        error_msg = "Error: Watched DB parameters mismatch."
        if db_frames is not None and db_frames != args.frames:
            error_msg += f"\n - DB uses --frames={db_frames}, current run uses --frames={args.frames}."
            mismatch = True
        if db_hash_size is not None and db_hash_size != args.hash_size:
            error_msg += f"\n - DB uses --hash-size={db_hash_size}, current run uses --hash-size={args.hash_size}."
            mismatch = True

        if mismatch:
            error_msg += "\nParameters should match for reliable comparison."
            print(error_msg)  # Also print to console
            logging.warning(
                f"Watched DB parameter mismatch detected: {error_msg}"
            )  # Log as warning initially

            prompt_msg = (
                f"Use parameters from the watched database instead? \n"
                f" (DB: frames={db_frames}, hash_size={db_hash_size} | "
                f"Current: frames={args.frames}, hash_size={args.hash_size}) [y/N]: "
            )
            confirm_use_db_params = input(prompt_msg)

            if confirm_use_db_params.lower() == "y":
                print(
                    f"Proceeding with watched database parameters: frames={db_frames}, hash_size={db_hash_size}"
                )
                logging.info(
                    f"User opted to use watched DB parameters: frames={db_frames}, hash_size={db_hash_size}"
                )
                # Update args IN PLACE to use the database parameters
                # This modification will persist for the rest of the run_find_similar call
                args.frames = db_frames
                args.hash_size = db_hash_size
            else:
                print(
                    "Parameter mismatch and user chose not to proceed with DB parameters. Exiting."
                )
                logging.error("User aborted due to parameter mismatch.")
                sys.exit(1)
        else:
            logging.info("Watched DB parameters match current run parameters.")
    elif watched_hashes_set:  # Only warn if DB exists but has no metadata
        warn_msg = "Warning: Could not find hashing parameters (metadata) in the watched DB. Parameter consistency cannot be guaranteed. Ensure current settings match DB creation settings."
        logging.warning(warn_msg)
        print(warn_msg)
    # --- End Parameter Validation ---

    videos_to_check_for_duplicates = all_video_hashes.copy()  # Default: check all
    watched_videos_found = []
    moved_watched_files_set = set()  # Initialize return value

    if watched_hashes_set:
        print(
            f"Comparing {len(all_video_hashes)} videos against {len(watched_hashes_set)} watched hashes..."
        )
        # Note: identify_watched_videos uses args.hash_size and args.threshold,
        # which might have been updated by the validation logic above.
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
            # Note: move_watched_files returns the set of successfully moved files
            moved_count, failed_count, moved_watched_files_set = (
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

    return videos_to_check_for_duplicates, moved_watched_files_set


def _handle_duplicate_videos(args, videos_to_check, abs_target_directory):
    """
    Finds similar groups among the provided videos, prints results,
    prompts for moving, and moves them.

    Returns:
        tuple: (similar_video_groups, moved_duplicate_files_set)
               - list: list of groups found by core.find_similar_groups
               - set: paths of files *intended* to be moved as duplicates (based on user confirmation)
    """
    print("-" * 30)
    print(
        f"Checking for duplicates among the remaining {len(videos_to_check)} videos..."
    )
    similar_video_groups = []  # Initialize return value
    moved_duplicate_files_set = set()  # Initialize return value

    if len(videos_to_check) >= 2:
        # Note: find_similar_groups uses args.hash_size and args.threshold,
        # which might have been updated by the watched DB validation logic.
        similar_video_groups = core.find_similar_groups(
            video_hashes_map=videos_to_check,
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
                # We assume if the user confirmed, all files in the groups were *attempted* to be moved.
                for group_set, _ in similar_video_groups:
                    moved_duplicate_files_set.update(group_set)
            else:
                print("Move operation for duplicates cancelled by user.")
                # moved_duplicate_files_set remains empty if cancelled
        # No else needed here, print_similar_video_groups already handles the "no groups found" message.
    # No else needed: moved_duplicate_files_set is already initialized if no groups found

    print("-" * 30)
    return similar_video_groups, moved_duplicate_files_set


def _update_watched_database(
    args, videos_to_check, moved_watched_files, moved_duplicate_files
):
    """
    Determines the final set of unique, unwatched videos and updates the
    watched database if specified.

    Args:
        args: Command line arguments.
        videos_to_check (dict): Map of video paths -> hashes for videos that were
                                candidates for duplicate checking.
        moved_watched_files (set): Set of paths moved because they were watched.
        moved_duplicate_files (set): Set of paths moved because they were duplicates.
    """
    print("-" * 30)
    print(f"Updating watched database: {args.watched_db}")

    # Identify the final set of unique, unwatched videos that were NOT moved
    # Start with videos that were candidates for duplicate checking
    final_unique_paths = set(videos_to_check.keys())

    # Remove any that were successfully moved as watched (shouldn't be many/any if logic is correct, but be safe)
    final_unique_paths -= moved_watched_files

    # Remove any that were part of duplicate groups the user agreed to move
    final_unique_paths -= moved_duplicate_files

    if not final_unique_paths:
        print("No new unique, unwatched videos found to add to the watched database.")
    else:
        print(f"Found {len(final_unique_paths)} unique, unwatched video(s) to add.")
        # Extract all hashes for these final videos
        hashes_to_add = set()
        for path in final_unique_paths:
            # Get hashes from the map passed in (videos_to_check)
            if path in videos_to_check and videos_to_check[path]:
                # Convert hash objects to strings for storage
                hashes_to_add.update(str(h) for h in videos_to_check[path])
            else:
                # This case should ideally not happen if videos_to_check is correct
                logging.warning(
                    f"Could not find hashes for final unique video intended for DB update: {path}"
                )

        if hashes_to_add:
            print(
                f"Adding {len(hashes_to_add)} unique hashes and metadata to the watched database..."
            )
            # Add hashes and metadata to the database
            # Note: args.frames and args.hash_size reflect the effective parameters used
            # for this run (potentially updated from DB).
            watched_db_manager.add_hashes_to_watched_db(
                db_path=args.watched_db,
                new_hashes_set=hashes_to_add,
                num_frames=args.frames,
                hash_size=args.hash_size,
            )
        else:
            print("No valid hashes found for the unique videos. Database not updated.")
    print("-" * 30)


# --- Main Function ---


def run_find_similar(args):
    """Handles the logic for finding similar and watched videos."""
    target_directory = args.directory
    abs_target_directory = os.path.abspath(target_directory)

    if not os.path.isdir(abs_target_directory):
        logging.error(f"Error: Directory not found: {abs_target_directory}")
        sys.exit(1)  # Exit if directory is invalid

    # Display settings using the utility function
    # Note: db_path is implicitly handled by args.watched_db inside display_settings
    # Cache is stored relative to the target directory here
    utils.display_settings(
        args,
        "Find Similar/Watched Videos",
        abs_target_directory,
        cache_dir=abs_target_directory,
    )

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

        # Initialize variables for the workflow - these will be managed by helpers
        # videos_to_check_for_duplicates will be returned by _handle_watched_videos
        # moved_watched_files will be returned by _handle_watched_videos
        # moved_duplicate_files will be returned by _handle_duplicate_videos
        # similar_video_groups will be returned by _handle_duplicate_videos

        # Step 2: Handle Watched Videos (if --watched-db is provided)
        if args.watched_db:
            videos_to_check_for_duplicates, moved_watched_files = (
                _handle_watched_videos(args, all_video_hashes, abs_target_directory)
            )
        else:
            # If not using watched_db, check all videos for duplicates
            videos_to_check_for_duplicates = all_video_hashes.copy()
            moved_watched_files = set()  # No watched files moved

        # Step 3: Handle Duplicates (among remaining videos)
        # Initialize moved_duplicate_files before the call
        moved_duplicate_files = set()
        similar_video_groups, moved_duplicate_files = _handle_duplicate_videos(
            args, videos_to_check_for_duplicates, abs_target_directory
        )

        # Step 4: Update Watched DB (if --watched-db was provided)
        if args.watched_db:
            _update_watched_database(
                args,
                videos_to_check_for_duplicates,  # Videos remaining *before* duplicate check
                moved_watched_files,
                moved_duplicate_files,
            )

        print("Processing complete.")

    except Exception as e:
        logging.exception(f"An unexpected error occurred during processing: {e}")
        print(
            "\nAn error occurred. Please check the logs or run with -v for more details."
        )
        sys.exit(1)  # Exit with error code
