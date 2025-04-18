import itertools
import logging
import os
import shutil
from collections import defaultdict

from . import config


def get_video_files(directory):
    """Recursively finds all video files in a directory."""
    video_files = []
    try:
        for root, _, files in os.walk(directory):
            for file in files:
                if os.path.splitext(file)[1].lower() in config.VIDEO_EXTENSIONS:
                    # Store absolute paths for reliable caching keys
                    video_files.append(os.path.abspath(os.path.join(root, file)))
    except Exception as e:
        logging.error(f"Error walking directory {directory}: {e}")
    return video_files


def human_readable_size(size_bytes):
    """Convert a file size in bytes to a human-readable string."""
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = 0
    p = 1024
    # Ensure size_bytes is treated as a number for comparison
    try:
        size_bytes_num = float(size_bytes)
    except ValueError:
        return "Invalid size"  # Handle cases where size might not be numeric

    while size_bytes_num >= p and i < len(size_name) - 1:
        size_bytes_num /= p
        i += 1
    return f"{size_bytes_num:.2f} {size_name[i]}"


def group_similar_items(item_pairs_with_similarity):
    """
    Groups items based on similarity pairs and calculates average similarity for each group.

    Args:
        item_pairs_with_similarity (list): A list of tuples, where each tuple represents
                           a pair of similar items and their similarity score
                           (e.g., [(item1, item2, 95.5), (item2, item3, 98.0)]).

    Returns:
        list: A list of tuples `(group_set, average_similarity)`, where each `group_set`
              contains grouped similar items and `average_similarity` is the mean
              similarity of pairs *within* that group.
    """
    if not item_pairs_with_similarity:
        return []

    logging.info("Grouping similar items and calculating average similarity...")
    adj = defaultdict(list)
    all_items = set()
    # Store similarities for easy lookup, using sorted tuples as keys
    pair_similarities = {}
    for u, v, similarity in item_pairs_with_similarity:
        adj[u].append(v)
        adj[v].append(u)
        all_items.add(u)
        all_items.add(v)
        # Ensure consistent key order for lookup
        pair_key = tuple(sorted((u, v)))
        pair_similarities[pair_key] = similarity

    groups_with_similarity = []
    visited = set()

    for item in all_items:
        if item not in visited:
            current_group = set()
            # Use a list as a queue for BFS
            q = [item]
            visited.add(item)
            while q:
                curr = q.pop(0)
                current_group.add(curr)
                for neighbor in adj[curr]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        q.append(neighbor)

            # Only process groups with more than one item (actual duplicates/similars)
            if len(current_group) > 1:
                group_similarity_sum = 0.0
                group_pair_count = 0
                # Iterate through all unique pairs within the found group
                for item_a, item_b in itertools.combinations(current_group, 2):
                    pair_key = tuple(sorted((item_a, item_b)))
                    # Check if this pair was one of the original similar pairs
                    if pair_key in pair_similarities:
                        group_similarity_sum += pair_similarities[pair_key]
                        group_pair_count += 1

                average_similarity = 0.0
                if group_pair_count > 0:
                    average_similarity = group_similarity_sum / group_pair_count
                else:
                    # This case should ideally not happen for groups > 1 derived
                    # from pairs, but handle defensively. Could happen if a group
                    # is formed transitively but no direct pairs within it met the threshold.
                    logging.warning(
                        f"Group {current_group} has no internal pairs meeting threshold? Assigning 0 similarity."
                    )

                groups_with_similarity.append((current_group, average_similarity))

    logging.info(f"Found {len(groups_with_similarity)} groups of similar items.")
    # Sorting is now done in core.py after this function returns
    return groups_with_similarity


def print_similar_video_groups(grouped_videos_with_similarity):
    """Prints the groups of similar videos with their sizes and average similarity."""
    print("-" * 30)
    if not grouped_videos_with_similarity:
        print("No similar video groups found.")
    else:
        print(
            f"Found {len(grouped_videos_with_similarity)} groups of similar videos (sorted by similarity):"
        )
        # Input is now a list of tuples: (group_set, average_similarity)
        for i, (group, avg_similarity) in enumerate(grouped_videos_with_similarity):
            print(f"\nGroup {i + 1} (Average Similarity: {avg_similarity:.2f}%):")
            # Sort paths within the group for consistent display
            sorted_group = sorted(list(group))
            for video_path in sorted_group:
                try:
                    # Get file size using os.path.getsize
                    size = os.path.getsize(video_path)
                    size_str = human_readable_size(size)
                except FileNotFoundError:
                    size_str = "File not found"
                except Exception as e:
                    logging.warning(f"Could not get size for {video_path}: {e}")
                    size_str = "Unknown size"
                # Print the absolute path stored in the group
                print(f"  - {video_path}  [Size: {size_str}]")


# --- New function to move duplicates ---
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
