import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from .. import cache_manager, config, hashing, utils


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

    # Load cache and determine which videos need processing
    cache_path = cache_manager.get_cache_path(directory, cache_filename)
    video_hashes, videos_to_process, _ = cache_manager.load_or_check_cache(
        video_files, cache_path, num_frames, hash_size
    )

    processed_count = 0
    newly_cached_hashes = {}

    if videos_to_process:
        logging.info(
            f"Calculating hashes for {len(videos_to_process)} videos using {max_workers} workers..."
        )
        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for video_path in videos_to_process:
                future = executor.submit(
                    hashing.calculate_video_hashes,
                    video_path,
                    num_frames,
                    hash_size,
                    skip_duration,
                )
                futures[future] = video_path

            for future in as_completed(futures):
                processed_count += 1
                video_path = futures[future]
                try:
                    hashes = future.result()
                    if hashes is not None and len(hashes) == num_frames:
                        video_hashes[video_path] = hashes
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
                    elif hashes is None:
                        logging.warning(
                            f"Failed to calculate hashes for {os.path.basename(video_path)} (returned None)."
                        )
                    elif not hashes:
                        logging.info(
                            f"Skipped hashing for {os.path.basename(video_path)}"
                        )
                    else:
                        logging.warning(
                            f"Incorrect number/failed hash calculation for {os.path.basename(video_path)}. Skipping."
                        )
                except Exception as e:
                    logging.error(
                        f"Exception retrieving result for {os.path.basename(video_path)}: {e}"
                    )

                if processed_count % 50 == 0 or processed_count == len(
                    videos_to_process
                ):
                    logging.info(
                        f"Processed {processed_count}/{len(videos_to_process)} videos for hashing."
                    )
    else:
        logging.info("No new videos needed hash calculation (all loaded from cache).")

    # Update cache after processing
    if newly_cached_hashes:
        cache_manager.update_cache(cache_path, newly_cached_hashes)

    logging.info(f"Total videos with valid hashes: {len(video_hashes)}")
    return video_hashes
