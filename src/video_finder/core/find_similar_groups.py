import logging
import os
from collections import defaultdict

from .. import hashing, utils
from ..utils.bktree import BKTree


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
    if total_comparisons == 0 and len(valid_videos) == 2:
        total_comparisons = 1
    elif total_comparisons == 0:
        logging.info("No pairs to compare.")
        return []

    logging.info(f"Comparing {total_comparisons} pairs...")
    similar_pairs_with_scores = []

    # Build reverse mapping from hash to videos
    hash_to_video_map = defaultdict(list)
    for video_path, hashes in video_hashes_map.items():
        for h in hashes:
            hash_to_video_map[h].append(video_path)

    # Create and populate BK-Tree
    bktree = BKTree(distance_func=lambda h1, h2: h1 - h2)
    for h in hash_to_video_map:
        bktree.add(h)

    # Find candidate pairs through BK-Tree queries
    candidate_pairs = set()
    total_bits = hash_size * hash_size
    max_avg_distance = total_bits * (1 - similarity_threshold / 100.0)
    max_distance = int(max_avg_distance) + 1

    for h, videos in hash_to_video_map.items():
        results = bktree.query(h, max_distance)
        for dist, found_hash in results:
            matched_videos = hash_to_video_map.get(found_hash, [])
            for v1 in videos:
                for v2 in matched_videos:
                    if v1 == v2:
                        continue

                    pair = tuple(sorted((v1, v2)))
                    candidate_pairs.add(pair)

    # Perform exact similarity check on candidate pairs
    similar_pairs_with_scores = []
    for video1, video2 in candidate_pairs:
        hashes1 = video_hashes_map[video1]
        hashes2 = video_hashes_map[video2]

        similarity = hashing.compare_hashes(hashes1, hashes2, hash_size=hash_size)

        if similarity >= similarity_threshold:
            similar_pairs_with_scores.append((video1, video2, similarity))
            logging.debug(
                f"Found similar pair: {os.path.basename(video1)} and {os.path.basename(video2)} (Similarity: {similarity:.2f}%)"
            )

    logging.info(
        f"Found {len(similar_pairs_with_scores)} similar pairs above {similarity_threshold}% threshold."
    )

    if not similar_pairs_with_scores:
        logging.info("No similar pairs found.")
        return []

    # Group similar videos based on similar pairs
    grouped_videos_with_similarity = utils.group_similar_items(
        similar_pairs_with_scores
    )

    # Sort groups by average similarity (descending)
    grouped_videos_with_similarity.sort(key=lambda x: x[1], reverse=True)

    logging.info(
        f"Found {len(grouped_videos_with_similarity)} groups of similar items."
    )

    return grouped_videos_with_similarity
