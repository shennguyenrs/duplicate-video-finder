import logging
import os
import shutil

from .. import config


def move_watched_files(
    watched_video_paths,
    base_directory,
    watched_subdir=config.DEFAULT_WATCHED_DIR_NAME,
):
    """
    Moves files identified as 'watched' into a specified subdirectory.

    Args:
        watched_video_paths (list): Absolute paths to video files to be moved.
        base_directory (str): Directory where the scan was performed and where the watched subdirectory will be created.
        watched_subdir (str): Name of the subdirectory to move watched files into.

    Returns:
        tuple: (number_of_files_moved, number_of_failures, set_of_moved_files)
    """
    watched_dir_path = os.path.join(base_directory, watched_subdir)
    moved_count = 0
    failed_count = 0
    files_successfully_moved = set()

    if not watched_video_paths:
        logging.info("No watched video paths provided for moving.")
        return 0, 0, set()

    logging.info(
        f"Attempting to move {len(watched_video_paths)} watched files to '{watched_dir_path}'."
    )

    try:
        os.makedirs(watched_dir_path, exist_ok=True)
        logging.info(f"Ensured watched directory exists: {watched_dir_path}")
    except OSError as e:
        logging.error(f"Failed to create directory {watched_dir_path}: {e}")
        print(f"Error: Could not create directory {watched_dir_path}. Aborting move.")
        return 0, len(watched_video_paths), set()

    for file_path in watched_video_paths:
        if not os.path.isabs(file_path):
            logging.warning(f"Skipping non-absolute path: {file_path}")
            failed_count += 1
            continue
        if not os.path.exists(file_path):
            logging.warning(f"File not found, cannot move: {file_path}")
            print(
                f"  Error: Watched file not found '{os.path.basename(file_path)}'. Skipping move."
            )
            failed_count += 1
            continue

        base_name = os.path.basename(file_path)
        dest_path = os.path.join(watched_dir_path, base_name)

        # Ensure unique filename in destination directory
        counter = 1
        original_dest_path = dest_path
        while os.path.exists(dest_path):
            name, ext = os.path.splitext(base_name)
            dest_path = os.path.join(watched_dir_path, f"{name}_{counter}{ext}")
            counter += 1
            if counter > 100:
                logging.error(
                    f"Failed to find unique name for {base_name} in {watched_dir_path} after 100 attempts."
                )
                print(
                    f"  Error: Could not find a unique name for '{base_name}' in '{watched_subdir}'. Skipping move."
                )
                failed_count += 1
                break
        if counter > 100 and os.path.exists(dest_path):
            continue

        if dest_path != original_dest_path:
            logging.info(
                f"Destination file '{base_name}' exists. Renaming to '{os.path.basename(dest_path)}'."
            )

        try:
            logging.debug(f"Attempting to move '{file_path}' to '{dest_path}'")
            shutil.move(file_path, dest_path)
            print(
                f"  Moved Watched: '{os.path.basename(file_path)}' -> '{os.path.relpath(dest_path, base_directory)}'"
            )
            moved_count += 1
            files_successfully_moved.add(file_path)
        except FileNotFoundError:
            logging.warning(
                f"File not found during move (may have been deleted): {file_path}"
            )
            print(
                f"  Error: File not found '{os.path.basename(file_path)}'. Skipping move."
            )
            failed_count += 1
        except Exception as e:
            logging.error(f"Failed to move file {file_path} to {dest_path}: {e}")
            print(f"  Error moving '{os.path.basename(file_path)}': {e}")
            failed_count += 1

    logging.info(
        f"Move watched files complete: {moved_count} moved, {failed_count} failed."
    )
    return moved_count, failed_count, files_successfully_moved
