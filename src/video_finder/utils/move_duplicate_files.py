import logging
import os
import shutil

from .. import config


def move_duplicate_files(
    groups, base_directory, duplicate_subdir=config.DEFAULT_DUPLICATE_DIR_NAME
):
    """
    Moves duplicate files into a specified subdirectory.

    Args:
        groups (list): List of tuples `(group_set, average_similarity)`.
        base_directory (str): The directory where the scan was performed and
                              where the duplicate subdirectory will be created.
        duplicate_subdir (str): Name of the subdirectory to move duplicates into.

    Returns:
        tuple: (number_of_files_moved, number_of_failures)
    """
    duplicate_dir_path = os.path.join(base_directory, duplicate_subdir)
    moved_count = 0
    failed_count = 0

    try:
        os.makedirs(duplicate_dir_path, exist_ok=True)
        logging.info(f"Ensured duplicate directory exists: {duplicate_dir_path}")
    except OSError as e:
        logging.error(f"Failed to create directory {duplicate_dir_path}: {e}")
        print(f"Error: Could not create directory {duplicate_dir_path}. Aborting move.")
        return 0, sum(len(g[0]) - 1 for g in groups if len(g[0]) > 1)

    for group, avg_similarity in groups:
        if len(group) < 2:
            continue

        sorted_group = sorted(list(group))
        logging.info(
            f"Processing group with {len(group)} files (Avg Sim: {avg_similarity:.2f}%) for moving."
        )

        for file_path in sorted_group:
            base_name = os.path.basename(file_path)
            dest_path = os.path.join(duplicate_dir_path, base_name)

            # Ensure unique destination filename if collision occurs
            counter = 1
            original_dest_path = dest_path
            while os.path.exists(dest_path):
                name, ext = os.path.splitext(base_name)
                dest_path = os.path.join(duplicate_dir_path, f"{name}_{counter}{ext}")
                counter += 1
                if counter > 100:
                    logging.error(
                        f"Failed to find unique name for {base_name} in {duplicate_dir_path} after 100 attempts."
                    )
                    print(
                        f"  Error: Could not find a unique name for '{base_name}' in '{duplicate_subdir}'. Skipping move."
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
                    f"  Moved: '{os.path.basename(file_path)}' -> '{os.path.relpath(dest_path, base_directory)}'"
                )
                moved_count += 1
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

    return moved_count, failed_count
