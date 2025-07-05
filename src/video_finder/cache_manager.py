import logging
import os
import shelve


def load_or_check_cache(video_files, cache_path, num_frames, hash_size):
    """
    Loads cached hashes, checks for stale entries, and identifies files needing processing.

    Args:
        video_files (list): List of absolute paths to video files found.
        cache_path (str): Absolute path to the shelve cache file (without extension).
        num_frames (int): Expected number of frames used for hashing.
        hash_size (int): Expected hash size used for hashing.

    Returns:
        tuple: (
            dict: video_hashes - {video_path: [hash_obj, ...]},
            list: videos_to_process - [video_path, ...],
            set: cached_skipped_files - {video_path, ...},
            dict: stats - {'hits': int, 'misses': int, 'stale': int, 'skipped': int}
        )
    """
    video_hashes = {}
    videos_to_process = []
    cached_skipped_files = set()
    stats = {"hits": 0, "misses": 0, "stale": 0, "skipped": 0}
    video_files_set = set(video_files)

    logging.info(f"Using cache file: {cache_path}.db")

    try:
        with shelve.open(cache_path, flag="c") as cache:
            # Remove cache entries for files that no longer exist
            keys_to_delete = [k for k in cache if k not in video_files_set]
            if keys_to_delete:
                logging.info(
                    f"Pruning {len(keys_to_delete)} non-existent files from cache..."
                )
                deleted_count = 0
                for key in keys_to_delete:
                    try:
                        del cache[key]
                        deleted_count += 1
                    except KeyError:
                        logging.warning(
                            f"Key {key} already removed from cache during pruning."
                        )
                    except Exception as e:
                        logging.error(f"Error deleting key {key} from cache: {e}")
                logging.info(
                    f"Cache pruning complete ({deleted_count} entries removed)."
                )

            for video_path in video_files:
                try:
                    current_mtime = os.path.getmtime(video_path)
                    if video_path in cache:
                        cached_data = cache[video_path]
                        is_stale = not (
                            isinstance(cached_data, dict)
                            and cached_data.get("mtime") == current_mtime
                            and cached_data.get("num_frames") == num_frames
                            and cached_data.get("hash_size") == hash_size
                        )

                        if is_stale:
                            videos_to_process.append(video_path)
                            stats["stale"] += 1
                            logging.debug(
                                f"Cache stale/invalid for: {os.path.basename(video_path)}"
                            )
                            continue

                        # Entry is valid and not stale, check if it was skipped or hashed
                        if cached_data.get("hashes") == "SKIPPED":
                            cached_skipped_files.add(video_path)
                            stats["skipped"] += 1
                        elif isinstance(cached_data.get("hashes"), list):
                            video_hashes[video_path] = cached_data["hashes"]
                            stats["hits"] += 1
                        else:
                            videos_to_process.append(video_path)
                            stats["stale"] += 1
                    else:
                        videos_to_process.append(video_path)
                        stats["misses"] += 1

                except FileNotFoundError:
                    logging.warning(f"File not found during cache check: {video_path}")
                    if video_path in cache:
                        try:
                            del cache[video_path]
                            logging.info(
                                f"Removed missing file {video_path} from cache."
                            )
                        except Exception as e:
                            logging.error(
                                f"Error removing missing file {video_path} from cache: {e}"
                            )
                except Exception as e:
                    logging.error(f"Error checking cache for {video_path}: {e}")
                    if video_path not in videos_to_process:
                        videos_to_process.append(video_path)

            logging.info(
                f"Cache stats: Hits={stats['hits']}, Misses={stats['misses']}, Stale={stats['stale']}, Skipped={stats['skipped']}"
            )

    except Exception as e:
        logging.error(
            f"Failed to open or process cache file {cache_path}.db: {e}. Proceeding without cache."
        )
        videos_to_process = list(video_files)
        video_hashes = {}
        cached_skipped_files = set()
        stats = {"hits": 0, "misses": len(video_files), "stale": 0, "skipped": 0}

    return video_hashes, videos_to_process, cached_skipped_files, stats


def update_cache(
    cache_path, newly_cached_hashes, newly_skipped_info, num_frames, hash_size
):
    """
    Updates the cache file with newly calculated hashes and skipped video information.

    Args:
        cache_path (str): Absolute path to the shelve cache file (without extension).
        newly_cached_hashes (dict): {video_path: {"hashes": ..., "mtime": ...}}.
        newly_skipped_info (dict): {video_path: {"mtime": ...}}.
        num_frames (int): The number of frames used for the hash calculation.
        hash_size (int): The hash size used for the calculation.
    """
    if not newly_cached_hashes and not newly_skipped_info:
        logging.info("No new information to update in the cache.")
        return

    logging.info(
        f"Updating cache with {len(newly_cached_hashes)} new hashes and {len(newly_skipped_info)} skipped files..."
    )
    updated_count = 0
    try:
        with shelve.open(cache_path, flag="c") as cache:
            # Update with new successful hashes
            for video_path, data in newly_cached_hashes.items():
                cache[video_path] = {
                    "hashes": data["hashes"],
                    "mtime": data["mtime"],
                    "num_frames": num_frames,
                    "hash_size": hash_size,
                }
                updated_count += 1

            # Update with new skipped files
            for video_path, data in newly_skipped_info.items():
                cache[video_path] = {
                    "hashes": "SKIPPED",
                    "mtime": data["mtime"],
                    "num_frames": num_frames,
                    "hash_size": hash_size,
                }
                updated_count += 1

        logging.info(f"Cache update complete ({updated_count} entries written).")
    except Exception as e:
        logging.error(
            f"Failed to open or write to cache file {cache_path}.db for updates: {e}"
        )


def get_cache_path(directory, cache_filename):
    """Constructs the absolute path for the cache file."""
    return os.path.abspath(os.path.join(directory, cache_filename))
