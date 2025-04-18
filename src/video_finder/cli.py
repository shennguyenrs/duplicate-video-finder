import logging
import os

from . import arguments, config, core, utils

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """Main function to handle command-line arguments and run the video finder."""
    args = arguments.parse_arguments()

    target_directory = args.directory

    if not os.path.isdir(target_directory):
        logging.error(f"Error: Directory not found: {target_directory}")
        return  # Exit if directory is invalid

    # Display settings being used
    print("-" * 30)
    print(
        f"Starting video similarity detection in: '{os.path.abspath(target_directory)}'"
    )
    print(f"Similarity threshold: {args.threshold}%")
    print(f"Frames sampled per video: {args.frames}")
    print(f"Hash size: {args.hash_size}x{args.hash_size}")
    print(f"Skip duration: {args.skip_duration} seconds")
    cache_path_display = os.path.join(target_directory, args.cache_file + ".db")
    print(f"Using cache file: ~{cache_path_display}")
    print(f"Max workers: {args.workers}")
    print(f"Recursive scan: {'Enabled' if args.recursive else 'Disabled'}")
    # Moving duplicates is now the default behavior if groups are found
    print("Note: If similar groups are found, you will be prompted to move duplicates.")
    print("-" * 30)

    # Call the core logic from the core module
    try:
        similar_video_groups = core.find_similar_videos(
            directory=target_directory,
            similarity_threshold=args.threshold,
            num_frames=args.frames,
            hash_size=args.hash_size,
            cache_filename=args.cache_file,
            max_workers=args.workers,
            recursive=args.recursive,
            skip_duration=args.skip_duration,
        )
    except Exception as e:
        logging.exception(f"An unexpected error occurred during processing: {e}")
        print("\nAn error occurred. Please check the logs.")
        return  # Exit on error

    # --- Output Results ---
    utils.print_similar_video_groups(similar_video_groups)

    # --- Move Duplicates (Default Behavior) ---
    if similar_video_groups:  # Check if any groups were found
        num_duplicates_to_move = sum(
            len(group) - 1 for group, _ in similar_video_groups
        )
        if num_duplicates_to_move > 0:
            print("-" * 30)
            print(
                f"Found {num_duplicates_to_move} duplicate file(s) across {len(similar_video_groups)} groups."
            )
            confirm = input(
                f"Proceed with moving duplicates to '{config.DEFAULT_DUPLICATE_DIR_NAME}' subdirectory? [y/N]: "
            )
            if confirm.lower() == "y":
                print("Moving duplicates...")
                try:
                    moved_count, failed_count = utils.move_duplicate_files(
                        similar_video_groups, target_directory
                    )
                    print(
                        f"Finished moving: {moved_count} file(s) moved, {failed_count} failed."
                    )
                except Exception as e:
                    logging.exception(
                        f"An error occurred during the move operation: {e}"
                    )
                    print("An error occurred while moving files. Please check logs.")
            else:
                print("Move operation cancelled by user.")
        else:
            # This case should technically not happen if similar_video_groups is not empty
            # because groups should have > 1 item, but handle defensively.
            logging.info("No duplicates found to move (all groups have only 1 item?).")


# --- Entry Point ---
if __name__ == "__main__":
    main()
