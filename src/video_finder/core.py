import itertools
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import cache_manager, config, hashing, utils


def find_similar_videos(
    directory,
    similarity_threshold=config.DEFAULT_THRESHOLD,
    num_frames=config.NUM_FRAMES_TO_SAMPLE,
    hash_size=config.HASH_SIZE,
    cache_filename=config.DEFAULT_CACHE_FILENAME,
    max_workers=config.MAX_WORKERS,
    recursive=False,
):
    """
    Finds groups of similar videos in a directory, using caching and parallel processing.

    Args:
        directory (str): The path to the directory to scan.
        similarity_threshold (float): Percentage (0-100). Pairs above this are considered similar.
        num_frames (int): Number of frames to sample per video.
        hash_size (int): Size of the perceptual hash grid (e.g., 8 for 8x8).
        cache_filename (str): Base name of the cache file to use/create within the directory.
        max_workers (int): Maximum number of threads for parallel processing.
        recursive (bool): Whether to scan subdirectories recursively.

    Returns:
        list: A list of tuples `(group_set, average_similarity)`, sorted by similarity descending.
              Each `group_set` contains absolute paths of similar videos.
              Returns an empty list if fewer than two videos are found or processed successfully.
    """
    logging.info(f"Scanning directory: {directory} (Recursive: {recursive})")
    video_files = utils.get_video_files(directory, recursive=recursive)
    logging.info(f"Found {len(video_files)} potential video files.")

    if len(video_files) < 2:
        logging.info("Need at least two videos to compare.")
        return []

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
                    hashing.calculate_video_hashes, video_path, num_frames, hash_size
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

    # --- Step 4: Compare Video Pairs ---
    # Get list of videos that have valid hashes (either from cache or newly calculated)
    valid_videos = list(video_hashes.keys())
    logging.info(f"Total videos with valid hashes: {len(valid_videos)}")

    if len(valid_videos) < 2:
        logging.info("Not enough videos with valid hashes to compare.")
        return []

    logging.info(
        f"Comparing {len(valid_videos)} videos ({len(list(itertools.combinations(valid_videos, 2)))} pairs)..."
    )
    # Store pairs along with their similarity score
    similar_pairs_with_scores = []
    compared_count = 0
    total_comparisons = len(valid_videos) * (len(valid_videos) - 1) // 2
    if total_comparisons == 0:  # Handle case with exactly 2 valid videos
        total_comparisons = 1

    # Iterate through all unique pairs of videos with valid hashes
    for video1, video2 in itertools.combinations(valid_videos, 2):
        # Hashes should exist if they are in valid_videos, but double-check defensively
        if video1 not in video_hashes or video2 not in video_hashes:
            logging.warning(
                f"Missing hashes for comparison pair (should not happen): {os.path.basename(video1)}, {os.path.basename(video2)}"
            )
            continue  # Skip this pair

        hashes1 = video_hashes[video1]
        hashes2 = video_hashes[video2]

        # Perform the comparison using the hashing utility function
        # Pass hash_size for correct normalization in compare_hashes
        similarity = hashing.compare_hashes(hashes1, hashes2, hash_size=hash_size)

        # If similarity meets the threshold, record the pair and its score
        if similarity >= similarity_threshold:
            # Store the pair and its similarity score
            similar_pairs_with_scores.append((video1, video2, similarity))
            logging.debug(  # Use debug for individual pair findings
                f"Found similar pair: {os.path.basename(video1)} and {os.path.basename(video2)} (Similarity: {similarity:.2f}%)"
            )

        compared_count += 1
        # Log comparison progress periodically
        if compared_count % 1000 == 0 or compared_count == total_comparisons:
            logging.info(f"Compared {compared_count}/{total_comparisons} pairs...")

    logging.info(
        f"Found {len(similar_pairs_with_scores)} similar pairs above {similarity_threshold}% threshold."
    )

    # --- Step 5: Group Similar Videos ---
    # Pass pairs with scores to the grouping function
    grouped_videos_with_similarity = utils.group_similar_items(
        similar_pairs_with_scores
    )

    # --- Step 6: Sort Groups by Average Similarity (Descending) ---
    # The second element of each tuple is the average similarity
    grouped_videos_with_similarity.sort(key=lambda x: x[1], reverse=True)

    logging.info(
        f"Found {len(grouped_videos_with_similarity)} groups of similar items."
    )

    return grouped_videos_with_similarity
