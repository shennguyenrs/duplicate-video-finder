import logging
import os
import imagehash # Import imagehash for conversion

from .. import hashing


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
        # If no watched data, all videos are considered unwatched
        return [], video_hashes_map

    # --- Extract and convert watched hashes ---
    # 1. Get all unique hash strings from the dictionary values (which are sets of strings)
    all_watched_hashes_str = set().union(*watched_videos_data.values())

    if not all_watched_hashes_str:
        logging.info(
            "No actual hash strings found within the watched videos data. Skipping identification."
        )
        return [], video_hashes_map

    # 2. Convert string hashes to ImageHash objects for comparison
    watched_hashes_obj_set = set()
    try:
        watched_hashes_obj_set = {
            imagehash.hex_to_hash(h_str) for h_str in all_watched_hashes_str
        }
        logging.info(
            f"Successfully converted {len(watched_hashes_obj_set)} unique watched hashes from strings to objects for comparison."
        )
    except Exception as e:
        logging.error(
            f"Error converting watched hashes from string to object: {e}. Skipping watched video identification.",
            exc_info=True,
        )
        # If conversion fails, treat as if no watched hashes exist
        return [], video_hashes_map
    # --- End extraction and conversion ---


    logging.info(
        f"Comparing {total} videos against {len(watched_hashes_obj_set)} unique watched hash objects..."
    )

    for video_path, hashes_list in video_hashes_map.items():
        count += 1
        is_watched = False
        if not hashes_list:  # Skip if a video somehow has no hashes
            continue

        # Compare each hash of the video against each hash object in the watched set
        for video_hash in hashes_list:
            # Ensure video_hash is an ImageHash object (should be from calculate_all_hashes)
            if not isinstance(video_hash, imagehash.ImageHash):
                 logging.warning(f"Skipping non-ImageHash object found for video {video_path}")
                 continue

            for watched_hash_obj in watched_hashes_obj_set:
                similarity = hashing.compare_hashes(
                    [video_hash], [watched_hash_obj], hash_size
                )  # Compare single hash pair (both should be objects)

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
