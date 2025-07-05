import logging
import os
import shutil


def move_skipped_files(skipped_video_paths, base_directory, skipped_subdir):
    """
    Moves a list of skipped video files to a specified subdirectory within the base directory.

    Args:
        skipped_video_paths (list): List of absolute paths to the skipped video files.
        base_directory (str): The absolute path to the directory where the search was performed.
        skipped_subdir (str): The name of the subdirectory to move skipped files into.

    Returns:
        tuple: A tuple containing:
            - int: The number of files successfully moved.
            - int: The number of files that failed to move.
            - set: A set of paths for the files that were successfully moved.
    """
    moved_count = 0
    failed_count = 0
    moved_files_set = set()
    skipped_dir = os.path.join(base_directory, skipped_subdir)

    if not os.path.exists(skipped_dir):
        try:
            os.makedirs(skipped_dir)
            logging.info(f"Created directory: {skipped_dir}")
        except OSError as e:
            logging.error(f"Error creating directory {skipped_dir}: {e}")
            print(f"Error: Could not create directory {skipped_dir}. Aborting move.")
            return 0, len(skipped_video_paths), set()

    for src_path in skipped_video_paths:
        if not os.path.exists(src_path):
            logging.warning(
                f"File not found, cannot move: {src_path}. It might have been moved already."
            )
            failed_count += 1
            continue

        try:
            filename = os.path.basename(src_path)
            dest_path = os.path.join(skipped_dir, filename)

            if os.path.exists(dest_path):
                logging.warning(
                    f"Destination file already exists, skipping move: {dest_path}"
                )
                failed_count += 1
                continue

            shutil.move(src_path, dest_path)
            logging.info(f"Moved {src_path} to {dest_path}")
            moved_count += 1
            moved_files_set.add(src_path)
        except Exception as e:
            logging.error(f"Failed to move {src_path} to {skipped_dir}: {e}")
            failed_count += 1

    return moved_count, failed_count, moved_files_set
