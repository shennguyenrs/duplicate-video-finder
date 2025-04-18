import logging
import os
from collections import defaultdict

from . import config  # Use relative import for config


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


def group_similar_items(item_pairs):
    """
    Groups items based on similarity pairs.

    Args:
        item_pairs (list): A list of tuples, where each tuple represents
                           a pair of similar items (e.g., [(item1, item2), (item2, item3)]).

    Returns:
        list: A list of sets, where each set contains grouped similar items.
    """
    if not item_pairs:
        return []

    logging.info("Grouping similar items...")
    adj = defaultdict(list)
    all_items = set()
    for u, v in item_pairs:
        adj[u].append(v)
        adj[v].append(u)
        all_items.add(u)
        all_items.add(v)

    groups = []
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
            # Only add groups with more than one item (actual duplicates/similars)
            if len(current_group) > 1:
                groups.append(current_group)

    logging.info(f"Found {len(groups)} groups of similar items.")
    return groups


def print_similar_video_groups(similar_video_groups):
    """Prints the groups of similar videos with their sizes."""
    print("-" * 30)
    if not similar_video_groups:
        print("No similar video groups found.")
    else:
        print(f"Found {len(similar_video_groups)} groups of similar videos:")
        for i, group in enumerate(similar_video_groups):
            print(f"\nGroup {i + 1}:")
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
