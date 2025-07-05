import logging
import os
import imagehash

from ..utils.bktree import BKTree


def identify_watched_videos(
    video_hashes_map, watched_videos_data, hash_size, similarity_threshold
):
    """
    Identifies videos whose hashes match any hash stored in the watched data.

    Args:
        video_hashes_map (dict): {video_path: [hash_obj, ...]}.
        watched_videos_data (dict): {video_identifier: {hash_set_str}}.
        hash_size (int): Size of the perceptual hash grid (used for comparison).
        similarity_threshold (float): Percentage (0-100).

    Returns:
        tuple: (
            list: watched_paths_list - Absolute paths of videos considered watched.
            dict: unwatched_hashes_dict - {video_path: [hash_obj, ...]} for non-watched videos.
        )
    """
    watched_paths = []
    unwatched_hashes = {}
    count = 0
    total = len(video_hashes_map)

    if not watched_videos_data:
        logging.info(
            "Watched videos data is empty, skipping watched video identification."
        )
        return [], video_hashes_map

    # Extract all unique hash strings from watched data
    all_watched_hashes_str = set().union(*watched_videos_data.values())

    if not all_watched_hashes_str:
        logging.info(
            "No actual hash strings found within the watched videos data. Skipping identification."
        )
        return [], video_hashes_map

    # Convert string hashes to ImageHash objects and build BK-Tree
    try:
        watched_hashes_obj_set = {
            imagehash.hex_to_hash(h_str) for h_str in all_watched_hashes_str
        }
        logging.info(
            f"Successfully converted {len(watched_hashes_obj_set)} unique watched hashes from strings to objects."
        )

        # Build BK-Tree with Hamming distance metric
        watched_bktree = BKTree(distance_func=lambda h1, h2: h1 - h2)
        for h in watched_hashes_obj_set:
            watched_bktree.add(h)
        logging.info("Built BK-Tree for efficient hash searching")

    except Exception as e:
        logging.error(
            f"Error initializing watched hashes: {e}. Skipping video identification.",
            exc_info=True,
        )
        return [], video_hashes_map

    logging.info(
        f"Analyzing {total} videos using BK-Tree with {len(watched_hashes_obj_set)} hashes..."
    )

    for video_path, hashes_list in video_hashes_map.items():
        count += 1
        if not hashes_list:
            continue

        best_match_distances = []
        hash_len_bits = hash_size * hash_size

        for video_hash in hashes_list:
            if not isinstance(video_hash, imagehash.ImageHash):
                logging.warning(
                    f"Skipping non-ImageHash object found for video {video_path}"
                )
                continue

            # BK-Tree nearest neighbor search
            min_dist = hash_len_bits  # Init with max possible distance
            if watched_bktree.root is not None:
                queue = [watched_bktree.root]
                while queue:
                    node = queue.pop(0)
                    current_dist = watched_bktree.distance_func(video_hash, node.item)
                    if current_dist < min_dist:
                        min_dist = current_dist

                    # Explore children that could have closer matches
                    lower_bound = current_dist - min_dist
                    upper_bound = current_dist + min_dist
                    for d in list(node.children.keys()):
                        if lower_bound <= d <= upper_bound:
                            queue.append(node.children[d])

            best_match_distances.append(min_dist)

        if not best_match_distances:
            continue

        # Calculate overall similarity
        avg_distance = sum(best_match_distances) / len(best_match_distances)
        similarity = max(0.0, (hash_len_bits - avg_distance) / hash_len_bits) * 100

        if similarity >= similarity_threshold:
            watched_paths.append(video_path)
            logging.debug(
                f"Video '{os.path.basename(video_path)}' matched watched (Similarity: {similarity:.2f}%)"
            )
        else:
            unwatched_hashes[video_path] = hashes_list

        if count % 100 == 0 or count == total:
            logging.info(f"Checked {count}/{total} videos against watched database.")

    logging.info(f"Identified {len(watched_paths)} watched videos.")
    return watched_paths, unwatched_hashes
