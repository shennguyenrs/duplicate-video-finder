import itertools
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import cache_manager, config, hashing, utils


def calculate_all_hashes(
    directory,
    recursive=False,
    cache_filename=config.DEFAULT_CACHE_FILENAME,
    num_frames=config.NUM_FRAMES_TO_SAMPLE,
    hash_size=config.HASH_SIZE,
    skip_duration=config.DEFAULT_SKIP_DURATION_SECONDS,
    max_workers=config.MAX_WORKERS,
):
    """
    Calculates or retrieves from cache the hashes for all valid video files
    in the specified directory.

    Args:
        directory (str): The path to the directory to scan.
        recursive (bool): Whether to scan subdirectories recursively.
        cache_filename (str): Base name of the cache file.
        num_frames (int): Number of frames to sample per video.
        hash_size (int): Size of the perceptual hash grid.
        skip_duration (int): Minimum video duration in seconds to process.
        max_workers (int): Maximum number of threads for parallel processing.

    Returns:
        dict: A dictionary mapping absolute video file paths to a list of their
              calculated hash objects. Returns an empty dict if no videos found
              or processed.
    """
    logging.info(f"Scanning directory: {directory} (Recursive: {recursive})")
    video_files = utils.get_video_files(directory, recursive=recursive)
    logging.info(f"Found {len(video_files)} potential video files.")

    if not video_files:
        logging.info("No video files found in the specified directory.")
        return {}

    # --- Step 1: Load Cache & Determine Videos to Process ---
    cache_path = cache_manager.get_cache_path(directory, cache_filename)
    video_hashes, videos_to_process, _ = cache_manager.load_or_check_cache(
        video_files, cache_path, num_frames, hash_size
    )
    # video_hashes dict now contains hashes loaded from cache

    # --- Step 2: Calculate Hashes for Videos Needing Processing ---
    processed_count = 0
    newly_cached_hashes = (
        {}
    )  # Store hashes calculated in this run before writing to cache

    if videos_to_process:
        logging.info(
            f"Calculating hashes for {len(videos_to_process)} videos using {max_workers} workers..."
        )
        futures = {}
        # Use ThreadPoolExecutor for parallel hash calculation
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for video_path in videos_to_process:
                # Submit the hashing task
                future = executor.submit(
                    hashing.calculate_video_hashes,
                    video_path,
                    num_frames,
                    hash_size,
                    skip_duration,
                )
                futures[future] = video_path  # Map future back to video path

            # Process completed futures as they finish
            for future in as_completed(futures):
                processed_count += 1
                video_path = futures[future]
                try:
                    # Get the result (list of hashes or None/[])
                    hashes = future.result()

                    # Check if successful and got the expected number of hashes
                    if hashes is not None and len(hashes) == num_frames:
                        video_hashes[video_path] = (
                            hashes  # Add successful results to main dict
                        )
                        # Prepare data for caching
                        try:
                            current_mtime = os.path.getmtime(video_path)
                            newly_cached_hashes[video_path] = {
                                "hashes": hashes,
                                "mtime": current_mtime,
                                "num_frames": num_frames,
                                "hash_size": hash_size,
                            }
                        except FileNotFoundError:
                            logging.warning(
                                f"File disappeared before mtime could be read for caching: {video_path}"
                            )
                        except Exception as e:
                            logging.error(
                                f"Error getting mtime for caching {video_path}: {e}"
                            )
                    # Log failures or unexpected results from hashing function
                    elif hashes is None:
                        logging.warning(
                            f"Failed to calculate hashes for {os.path.basename(video_path)} (returned None)."
                        )
                    else:  # Includes empty list [] or wrong count
                        logging.warning(
                            f"Incorrect number/failed hash calculation for {os.path.basename(video_path)}. Skipping."
                        )

                except Exception as e:
                    # Catch exceptions during future.result() or processing
                    logging.error(
                        f"Exception retrieving result for {os.path.basename(video_path)}: {e}"
                    )

                # Log progress periodically
                if processed_count % 50 == 0 or processed_count == len(
                    videos_to_process
                ):
                    logging.info(
                        f"Processed {processed_count}/{len(videos_to_process)} videos for hashing."
                    )
    else:
        logging.info("No new videos needed hash calculation (all loaded from cache).")

    # --- Step 3: Update Cache (after processing finishes) ---
    if newly_cached_hashes:
        cache_manager.update_cache(cache_path, newly_cached_hashes)

    logging.info(f"Total videos with valid hashes: {len(video_hashes)}")
    return video_hashes


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
