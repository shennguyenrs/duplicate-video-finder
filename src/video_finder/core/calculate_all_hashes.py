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
        tuple: A tuple containing:
            - dict: A dictionary mapping absolute video file paths to a list of their
                    calculated hash objects.
            - set: A set of absolute paths for videos that were skipped during hashing.
    """
    logging.info(f"Scanning directory: {directory} (Recursive: {recursive})")
    video_files = utils.get_video_files(directory, recursive=recursive)
    logging.info(f"Found {len(video_files)} potential video files.")

    if not video_files:
        logging.info("No video files found in the specified directory.")
        return {}, set()

    # Load cache and determine which videos need processing
    cache_path = cache_manager.get_cache_path(directory, cache_filename)
    video_hashes, videos_to_process, cached_skipped_files, _ = (
        cache_manager.load_or_check_cache(
            video_files, cache_path, num_frames, hash_size
        )
    )

    processed_count = 0
    newly_cached_hashes = {}
    newly_skipped_info = {}

    # Initialize with files that were marked as skipped in the cache
    skipped_during_hashing = cached_skipped_files.copy()

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
                    current_mtime = os.path.getmtime(video_path)

                    if hashes is not None and len(hashes) == num_frames:
                        video_hashes[video_path] = hashes
                        newly_cached_hashes[video_path] = {
                            "hashes": hashes,
                            "mtime": current_mtime,
                            "num_frames": num_frames,
                            "hash_size": hash_size,
                        }
                    else:
                        # Hashing failed or was skipped, mark for caching as skipped
                        skipped_during_hashing.add(video_path)
                        newly_skipped_info[video_path] = {"mtime": current_mtime}
                        if hashes is None:
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

                except FileNotFoundError:
                    logging.warning(
                        f"File disappeared before processing completed: {video_path}"
                    )
                except Exception as e:
                    logging.error(
                        f"Exception processing {os.path.basename(video_path)}: {e}"
                    )
                    # Also mark as skipped if an unexpected exception occurs
                    if video_path not in skipped_during_hashing:
                        try:
                            current_mtime = os.path.getmtime(video_path)
                            skipped_during_hashing.add(video_path)
                            newly_skipped_info[video_path] = {"mtime": current_mtime}
                        except FileNotFoundError:
                            logging.warning(
                                f"File disappeared before it could be marked as skipped: {video_path}"
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
    if newly_cached_hashes or newly_skipped_info:
        cache_manager.update_cache(
            cache_path, newly_cached_hashes, newly_skipped_info, num_frames, hash_size
        )

    logging.info(f"Total videos with valid hashes: {len(video_hashes)}")
    logging.info(f"Total videos skipped: {len(skipped_during_hashing)}")
    return video_hashes, skipped_during_hashing
