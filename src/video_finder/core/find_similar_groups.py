import itertools
import logging
import os

from .. import hashing, utils


def find_similar_groups(video_hashes_map, hash_size, similarity_threshold):
    """
    Finds groups of similar videos based on their hashes.

    Args:
        video_hashes_map (dict): {video_path: [hash_obj, ...]}. Typically contains
                                 only unwatched videos if filtering was applied.
        hash_size (int): Size of the perceptual hash grid.
        similarity_threshold (float): Percentage (0-100). Pairs above this are similar.

    Returns:
        list: A list of tuples `(group_set, average_similarity)`, sorted by similarity descending.
              Each `group_set` contains absolute paths of similar videos.
              Returns an empty list if fewer than two videos are provided or no similar pairs found.
    """
    valid_videos = list(video_hashes_map.keys())
    logging.info(f"Comparing {len(valid_videos)} videos for duplicates...")

    if len(valid_videos) < 2:
        logging.info(
            "Need at least two videos with valid hashes to compare for duplicates."
        )
        return []

    total_comparisons = len(valid_videos) * (len(valid_videos) - 1) // 2
    if (
        total_comparisons == 0 and len(valid_videos) == 2
    ):  # Handle case with exactly 2 valid videos
        total_comparisons = 1
    elif total_comparisons == 0:
        logging.info("No pairs to compare.")
        return []

    logging.info(f"Comparing {total_comparisons} pairs...")
    similar_pairs_with_scores = []
    compared_count = 0

    # Iterate through all unique pairs of videos with valid hashes
    for video1, video2 in itertools.combinations(valid_videos, 2):
        # Hashes should exist, but double-check defensively
        if video1 not in video_hashes_map or video2 not in video_hashes_map:
            logging.warning(
                f"Missing hashes for comparison pair (should not happen): {os.path.basename(video1)}, {os.path.basename(video2)}"
            )
            continue  # Skip this pair

        hashes1 = video_hashes_map[video1]
        hashes2 = video_hashes_map[video2]

        # Perform the comparison using the hashing utility function
        similarity = hashing.compare_hashes(hashes1, hashes2, hash_size=hash_size)

        # If similarity meets the threshold, record the pair and its score
        if similarity >= similarity_threshold:
            similar_pairs_with_scores.append((video1, video2, similarity))
            logging.debug(
                f"Found similar pair: {os.path.basename(video1)} and {os.path.basename(video2)} (Similarity: {similarity:.2f}%)"
            )

        compared_count += 1
        # Log comparison progress periodically
        if compared_count % 1000 == 0 or compared_count == total_comparisons:
            logging.info(f"Compared {compared_count}/{total_comparisons} pairs...")

    logging.info(
        f"Found {len(similar_pairs_with_scores)} similar pairs above {similarity_threshold}% threshold."
    )

    if not similar_pairs_with_scores:
        logging.info("No similar pairs found.")
        return []

    # --- Group Similar Videos ---
    grouped_videos_with_similarity = utils.group_similar_items(
        similar_pairs_with_scores
    )

    # --- Sort Groups by Average Similarity (Descending) ---
    grouped_videos_with_similarity.sort(key=lambda x: x[1], reverse=True)

    logging.info(
        f"Found {len(grouped_videos_with_similarity)} groups of similar items."
    )

    return grouped_videos_with_similarity
