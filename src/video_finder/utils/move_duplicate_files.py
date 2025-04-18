import logging
import os
import shutil


def move_duplicate_files(groups, base_directory, duplicate_subdir="duplicate_videos"):
    """
    Moves duplicate files (all but one from each group) into a specified subdirectory.

    Args:
        groups (list): List of tuples `(group_set, average_similarity)`.
        base_directory (str): The directory where the scan was performed and
                              where the duplicate subdirectory will be created.
        duplicate_subdir (str): Name of the subdirectory to move duplicates into.

    Returns:
        tuple: A tuple containing (number_of_files_moved, number_of_failures).
    """
    duplicate_dir_path = os.path.join(base_directory, duplicate_subdir)
    moved_count = 0
    failed_count = 0

    # Create the duplicate directory if it doesn't exist
    try:
        os.makedirs(duplicate_dir_path, exist_ok=True)
        logging.info(f"Ensured duplicate directory exists: {duplicate_dir_path}")
    except OSError as e:
        logging.error(f"Failed to create directory {duplicate_dir_path}: {e}")
        print(f"Error: Could not create directory {duplicate_dir_path}. Aborting move.")
        return 0, sum(
            len(g[0]) - 1 for g in groups if len(g[0]) > 1
        )  # All potential moves fail

    for group, avg_similarity in groups:
        if len(group) < 2:
            continue  # Should not happen based on grouping logic, but check

        # Sort group alphabetically by path to consistently choose which file to keep
        sorted_group = sorted(list(group))
        file_to_keep = sorted_group[0]
        files_to_move = sorted_group[1:]

        logging.info(
            f"Group (Avg Sim: {avg_similarity:.2f}%): Keeping '{os.path.basename(file_to_keep)}'"
        )

        for file_path in files_to_move:
            base_name = os.path.basename(file_path)
            dest_path = os.path.join(duplicate_dir_path, base_name)

            # Handle potential filename collisions in the destination directory
            counter = 1
            original_dest_path = (
                dest_path  # Store original for logging clarity if renamed
            )
            while os.path.exists(dest_path):
                name, ext = os.path.splitext(base_name)
                dest_path = os.path.join(duplicate_dir_path, f"{name}_{counter}{ext}")
                counter += 1
                if counter > 100:  # Safety break to prevent infinite loop
                    logging.error(
                        f"Failed to find unique name for {base_name} in {duplicate_dir_path} after 100 attempts."
                    )
                    print(
                        f"  Error: Could not find a unique name for '{base_name}' in '{duplicate_subdir}'. Skipping move."
                    )
                    failed_count += 1
                    # Break inner while loop, go to next file_path
                    break
            # If the inner loop broke due to the safety break, skip the move for this file
            if counter > 100 and os.path.exists(
                dest_path
            ):  # Check condition that caused break
                continue

            # Log if the name was changed
            if dest_path != original_dest_path:
                logging.info(
                    f"Destination file '{base_name}' exists. Renaming to '{os.path.basename(dest_path)}'."
                )

            # Perform the move
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
