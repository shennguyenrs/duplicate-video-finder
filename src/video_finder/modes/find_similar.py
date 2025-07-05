import logging
import os
import sys

from .. import config, core, utils, watched_db_manager


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
    watched_videos_data, db_metadata = watched_db_manager.load_watched_videos_data(
        args.watched_db
    )

    # Validate hashing parameters against DB metadata
    if db_metadata:
        db_frames = db_metadata.get("num_frames")
        db_hash_size = db_metadata.get("hash_size")
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
            print(error_msg)
            logging.warning(f"Watched DB parameter mismatch detected: {error_msg}")
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
    elif watched_videos_data:
        warn_msg = "Warning: Could not find hashing parameters (metadata) in the watched DB. Parameter consistency cannot be guaranteed. Ensure current settings match DB creation settings."
        logging.warning(warn_msg)
        print(warn_msg)

    videos_to_check_for_duplicates = all_video_hashes.copy()
    watched_videos_found = []
    moved_watched_files_set = set()

    if watched_videos_data:
        print(
            f"Comparing {len(all_video_hashes)} videos against {len(watched_videos_data)} entries in watched database..."
        )
        watched_videos_found, videos_to_check_for_duplicates = (
            core.identify_watched_videos(
                video_hashes_map=all_video_hashes,
                watched_videos_data=watched_videos_data,
                hash_size=args.hash_size,
                similarity_threshold=args.threshold,
            )
        )
    else:
        print(
            "Watched database is empty or could not be loaded. Skipping watched check."
        )

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
    similar_video_groups = []
    moved_duplicate_files_set = set()

    if len(videos_to_check) >= 2:
        similar_video_groups = core.find_similar_groups(
            video_hashes_map=videos_to_check,
            hash_size=args.hash_size,
            similarity_threshold=args.threshold,
        )
    else:
        print("Less than two videos remaining, skipping duplicate check.")

    utils.print_similar_video_groups(similar_video_groups)

    if similar_video_groups:
        num_duplicates_to_move = sum(len(group) for group, _ in similar_video_groups)
        if num_duplicates_to_move > 0:
            print("-" * 30)
            print(
                f"Found {num_duplicates_to_move} file(s) across {len(similar_video_groups)} duplicate groups."
            )
            confirm_duplicates = input(
                f"Proceed with moving ALL files in these groups to '{config.DEFAULT_DUPLICATE_DIR_NAME}' subdirectory? [Y/n]: "
            )
            if confirm_duplicates.lower() != "n":
                print("Moving duplicates...")
                moved_count, failed_count = utils.move_duplicate_files(
                    groups=similar_video_groups,
                    base_directory=abs_target_directory,
                    duplicate_subdir=config.DEFAULT_DUPLICATE_DIR_NAME,
                )
                print(
                    f"Finished moving duplicates: {moved_count} file(s) moved, {failed_count} failed."
                )
                for group_set, _ in similar_video_groups:
                    moved_duplicate_files_set.update(group_set)
            else:
                print("Move operation for duplicates cancelled by user.")
    print("-" * 30)
    return similar_video_groups, moved_duplicate_files_set


def _handle_skipped_videos(args, skipped_during_hashing, abs_target_directory):
    """
    Handles moving videos that were skipped during the hashing process.

    Args:
        args: Command line arguments.
        skipped_during_hashing (set): A set of absolute paths for videos that were
                                      skipped by the hashing function.
        abs_target_directory (str): The absolute path to the target directory.
    """
    if not skipped_during_hashing:
        return

    print("-" * 30)
    print(
        f"Found {len(skipped_during_hashing)} video(s) that were skipped during hashing (e.g., too short, corrupted)."
    )
    confirm_skipped = input(
        f"Proceed with moving these skipped videos to '{config.DEFAULT_SKIPPED_DIR_NAME}' subdirectory? [Y/n]: "
    )
    if confirm_skipped.lower() != "n":
        print("Moving skipped videos...")
        moved_count, failed_count, _ = utils.move_skipped_files(
            skipped_video_paths=list(skipped_during_hashing),
            base_directory=abs_target_directory,
            skipped_subdir=config.DEFAULT_SKIPPED_DIR_NAME,
        )
        print(
            f"Finished moving skipped videos: {moved_count} moved, {failed_count} failed."
        )
    else:
        print("Move operation for skipped videos cancelled by user.")
    print("-" * 30)


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

    final_unique_paths = set(videos_to_check.keys())
    final_unique_paths -= moved_watched_files
    final_unique_paths -= moved_duplicate_files

    if not final_unique_paths:
        print("No new unique, unwatched videos found to add to the watched database.")
    else:
        print(f"Found {len(final_unique_paths)} unique, unwatched video(s) to add.")
        print("Adding/updating entries in the watched database...")

        added_count = 0
        for video_path in final_unique_paths:
            hashes_list = videos_to_check.get(video_path)
            if hashes_list:
                video_hashes_set_str = {str(h) for h in hashes_list}
                if video_hashes_set_str:
                    watched_db_manager.add_video_to_watched_db(
                        db_path=args.watched_db,
                        video_identifier=video_path,
                        video_hashes_set=video_hashes_set_str,
                        num_frames=args.frames,
                        hash_size=args.hash_size,
                    )
                    added_count += 1
                else:
                    logging.warning(
                        f"Hash list for video '{video_path}' resulted in an empty string set. Not adding to DB."
                    )
            else:
                logging.warning(
                    f"Could not find hashes for final unique video intended for DB update: {video_path}"
                )

        print(
            f"Finished updating watched database. Added/updated {added_count} video entries."
        )

    print("-" * 30)


def run_find_similar(args):
    """Handles the logic for finding similar and watched videos."""
    target_directory = args.directory
    abs_target_directory = os.path.abspath(target_directory)

    if not os.path.isdir(abs_target_directory):
        logging.error(f"Error: Directory not found: {abs_target_directory}")
        sys.exit(1)

    utils.display_settings(
        args,
        "Find Similar/Watched Videos",
        abs_target_directory,
        cache_dir=abs_target_directory,
    )

    try:
        all_video_hashes, skipped_during_hashing = core.calculate_all_hashes(
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
            if skipped_during_hashing:
                _handle_skipped_videos(
                    args, skipped_during_hashing, abs_target_directory
                )
            return

        if args.watched_db:
            videos_to_check_for_duplicates, moved_watched_files = (
                _handle_watched_videos(args, all_video_hashes, abs_target_directory)
            )
        else:
            videos_to_check_for_duplicates = all_video_hashes.copy()
            moved_watched_files = set()

        moved_duplicate_files = set()
        similar_video_groups, moved_duplicate_files = _handle_duplicate_videos(
            args, videos_to_check_for_duplicates, abs_target_directory
        )

        _handle_skipped_videos(
            args,
            skipped_during_hashing,
            abs_target_directory,
        )

        if args.watched_db:
            _update_watched_database(
                args,
                videos_to_check_for_duplicates,
                moved_watched_files,
                moved_duplicate_files,
            )

        print("Processing complete.")

    except Exception as e:
        logging.exception(f"An unexpected error occurred during processing: {e}")
        print(
            "\nAn error occurred. Please check the logs or run with -v for more details."
        )
        sys.exit(1)
