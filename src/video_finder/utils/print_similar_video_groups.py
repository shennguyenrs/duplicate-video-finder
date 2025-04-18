import logging
import os

from .human_readable_size import human_readable_size


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
