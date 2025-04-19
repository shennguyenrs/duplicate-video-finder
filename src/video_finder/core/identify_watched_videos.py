import logging
import os

from .. import hashing


def identify_watched_videos(
    video_hashes_map, watched_hashes_set, hash_size, similarity_threshold
):
    """
    Identifies videos whose hashes match any hash in the watched set.

    Args:
        video_hashes_map (dict): {video_path: [hash_obj, ...]}.
        watched_hashes_set (set): A set of watched hash strings.
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

    if not watched_hashes_set:
        logging.info(
            "No watched hashes provided, skipping watched video identification."
        )
        # If no watched hashes, all videos are considered unwatched
        return [], video_hashes_map

    logging.info(
        f"Comparing {total} videos against {len(watched_hashes_set)} watched hashes..."
    )

    for video_path, hashes_list in video_hashes_map.items():
        count += 1
        is_watched = False
        if not hashes_list:  # Skip if a video somehow has no hashes
            continue

        # Compare each hash of the video against each hash in the watched set
        for video_hash in hashes_list:
            for watched_hash in watched_hashes_set:
                similarity = hashing.compare_hashes(
                    [video_hash], [watched_hash], hash_size
                )  # Compare single hash pair

                if similarity >= similarity_threshold:
                    is_watched = True
                    logging.debug(
                        f"Video '{os.path.basename(video_path)}' matched watched hash. Similarity: {similarity:.2f}%"
                    )
                    break  # Stop comparing hashes for this video
            if is_watched:
                break  # Stop checking hashes in watched_hashes_set

        if is_watched:
            watched_paths.append(video_path)
        else:
            unwatched_hashes[video_path] = hashes_list

        if count % 100 == 0 or count == total:
            logging.info(f"Checked {count}/{total} videos against watched database.")

    logging.info(f"Identified {len(watched_paths)} watched videos.")
    return watched_paths, unwatched_hashes
