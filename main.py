import os
import cv2
from PIL import Image
import imagehash
import itertools
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from collections import defaultdict
import shelve  # Added for caching
import time  # Added for mtime

# --- Configuration ---
VIDEO_EXTENSIONS = {".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv"}
NUM_FRAMES_TO_SAMPLE = 20
HASH_SIZE = 8
MAX_WORKERS = os.cpu_count()
# Default cache file name (can be overridden)
DEFAULT_CACHE_FILENAME = ".video_hashes_cache"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Helper Functions (get_video_files, calculate_video_hashes, compare_hashes remain the same) ---


def get_video_files(directory):
    """Recursively finds all video files in a directory."""
    video_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if os.path.splitext(file)[1].lower() in VIDEO_EXTENSIONS:
                # Store absolute paths for reliable caching keys
                video_files.append(os.path.abspath(os.path.join(root, file)))
    return video_files


def calculate_video_hashes(
    video_path, num_frames=NUM_FRAMES_TO_SAMPLE, hash_size=HASH_SIZE
):
    """
    Extracts frames from a video and calculates their average hashes.
    Returns a list of imagehash objects or None if an error occurs.
    (No changes needed in this specific function for caching logic itself)
    """
    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            logging.warning(f"Could not open video file: {video_path}")
            return None

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        duration = total_frames / fps if fps > 0 else 0

        if (
            total_frames < num_frames or duration < 1
        ):  # Skip very short or invalid videos
            logging.info(
                f"Skipping too short or invalid video: {video_path} (Frames: {total_frames}, Duration: {duration:.2f}s)"
            )
            cap.release()
            return []  # Return empty list, not None, to distinguish from open errors

        frame_hashes = []
        indices = np.linspace(0, total_frames - 1, num_frames, dtype=int)

        for frame_idx in indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
            ret, frame = cap.read()
            if ret:
                img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
                h = imagehash.average_hash(img, hash_size=hash_size)
                frame_hashes.append(h)
            else:
                logging.warning(f"Could not read frame {frame_idx} from {video_path}")

        cap.release()

        if not frame_hashes:
            logging.warning(f"No frames could be processed for: {video_path}")
            return []

        # Ensure correct number of hashes were generated
        if len(frame_hashes) != num_frames:
            logging.warning(
                f"Expected {num_frames} hashes but got {len(frame_hashes)} for {video_path}. Discarding result."
            )
            return []  # Treat as failure if count mismatch

        return frame_hashes

    except Exception as e:
        logging.error(f"Error processing video {video_path}: {e}")
        if "cap" in locals() and cap.isOpened():
            cap.release()
        return None


def compare_hashes(hashes1, hashes2, hash_size=HASH_SIZE):
    """
    Compares two lists of hashes. Returns a similarity percentage.
    (No changes needed)
    """
    if not hashes1 or not hashes2 or len(hashes1) != len(hashes2):
        return 0.0

    total_distance = 0
    num_hashes = len(hashes1)
    # Hash length in bits (e.g., 8x8 = 64 bits)
    hash_len_bits = hash_size * hash_size

    for h1, h2 in zip(hashes1, hashes2):
        try:
            # Hamming distance calculation is built into imagehash
            distance = h1 - h2
            total_distance += distance
        except TypeError:
            # Handle case where a hash might be None or invalid if error occurred
            logging.warning("Invalid hash object encountered during comparison.")
            # Penalize similarity heavily if hashes are invalid/mismatched type
            total_distance += hash_len_bits

    average_distance = total_distance / num_hashes

    # Convert average distance to similarity percentage
    similarity = max(0.0, (hash_len_bits - average_distance) / hash_len_bits) * 100
    return similarity


def human_readable_size(size_bytes):
    """Convert a file size in bytes to a human-readable string."""
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = 0
    p = 1024
    while size_bytes >= p and i < len(size_name) - 1:
        size_bytes /= p
        i += 1
    return f"{size_bytes:.2f} {size_name[i]}"


# --- Main Function ---


def find_similar_videos(
    directory,
    similarity_threshold=90.0,
    num_frames=NUM_FRAMES_TO_SAMPLE,
    hash_size=HASH_SIZE,
    cache_filename=DEFAULT_CACHE_FILENAME,
):
    """
    Finds groups of similar videos in a directory, using a cache for hashes.

    Args:
        directory (str): The path to the directory to scan.
        similarity_threshold (float): Percentage (0-100). Pairs above this are considered similar.
        num_frames (int): Number of frames to sample per video.
        hash_size (int): Size of the perceptual hash grid (e.g., 8 for 8x8).
        cache_filename (str): Name of the cache file to use/create.

    Returns:
        list: A list of sets, where each set contains paths of similar videos.
    """
    logging.info(f"Scanning directory: {directory}")
    # Get absolute paths for reliable cache keys
    video_files = get_video_files(directory)
    video_files_set = set(video_files)  # For quick existence checks
    logging.info(f"Found {len(video_files)} potential video files.")

    if len(video_files) < 2:
        logging.info("Need at least two videos to compare.")
        return []

    # Construct absolute path for the cache file relative to the target directory
    cache_path = os.path.abspath(os.path.join(directory, cache_filename + ".db"))
    logging.info(f"Using cache file: {cache_path}")

    video_hashes = {}
    videos_to_process = []
    cache_hits = 0
    cache_misses = 0
    cache_stale = 0
    processed_count = 0
    newly_cached_hashes = (
        {}
    )  # Store hashes calculated in this run before writing to cache

    # --- Step 1: Load Cache & Determine Videos to Process ---
    try:
        # Using 'c' flag: open for read/write, create if doesn't exist
        with shelve.open(cache_path, flag="c") as cache:
            # Prune cache: Remove entries for files that no longer exist
            keys_to_delete = [k for k in cache if k not in video_files_set]
            if keys_to_delete:
                logging.info(
                    f"Pruning {len(keys_to_delete)} non-existent files from cache..."
                )
                for key in keys_to_delete:
                    del cache[key]
                logging.info("Cache pruning complete.")

            # Check cache for each video found
            for video_path in video_files:
                try:
                    current_mtime = os.path.getmtime(video_path)
                    if video_path in cache:
                        cached_data = cache[video_path]
                        # Check if file modified or parameters changed
                        if (
                            cached_data.get("mtime") == current_mtime
                            and cached_data.get("num_frames") == num_frames
                            and cached_data.get("hash_size") == hash_size
                            and cached_data.get("hashes")
                            is not None  # Ensure hashes are present
                            and len(cached_data.get("hashes", [])) == num_frames
                        ):  # Check hash count consistency
                            video_hashes[video_path] = cached_data["hashes"]
                            cache_hits += 1
                            # logging.debug(f"Cache hit for: {os.path.basename(video_path)}")
                        else:
                            # Cache entry is stale (file modified or parameters changed)
                            videos_to_process.append(video_path)
                            cache_stale += 1
                            logging.debug(
                                f"Cache stale for: {os.path.basename(video_path)}"
                            )
                    else:
                        # Not in cache
                        videos_to_process.append(video_path)
                        cache_misses += 1
                        # logging.debug(f"Cache miss for: {os.path.basename(video_path)}")
                except FileNotFoundError:
                    logging.warning(
                        f"File not found during cache check (should not happen if pruning worked): {video_path}"
                    )
                    if video_path in cache:  # Clean up if somehow missed by pruning
                        del cache[video_path]
                except Exception as e:
                    logging.error(f"Error checking cache for {video_path}: {e}")
                    videos_to_process.append(
                        video_path
                    )  # Process if cache check failed

            logging.info(
                f"Cache stats: Hits={cache_hits}, Misses={cache_misses}, Stale={cache_stale}"
            )

    except Exception as e:
        logging.error(
            f"Failed to open or process cache file {cache_path}: {e}. Proceeding without cache."
        )
        # If cache fails entirely, process all videos
        videos_to_process = video_files
        video_hashes = {}  # Reset hashes loaded from potentially corrupt cache

    # --- Step 2: Calculate Hashes for Videos Needing Processing ---
    if videos_to_process:
        logging.info(
            f"Calculating hashes for {len(videos_to_process)} videos using {MAX_WORKERS} workers..."
        )
        futures = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for video_path in videos_to_process:
                future = executor.submit(
                    calculate_video_hashes, video_path, num_frames, hash_size
                )
                futures[future] = video_path

            for future in as_completed(futures):
                processed_count += 1
                video_path = futures[future]
                try:
                    hashes = future.result()
                    # Check if successful and got expected number of hashes
                    if hashes is not None and len(hashes) == num_frames:
                        video_hashes[video_path] = hashes
                        # Store for caching later
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
                                f"File disappeared before caching could occur: {video_path}"
                            )
                        except Exception as e:
                            logging.error(
                                f"Error getting mtime for caching {video_path}: {e}"
                            )

                    elif hashes is None:
                        logging.warning(
                            f"Failed to calculate hashes for {video_path} (returned None)."
                        )
                    else:  # Includes empty list [] or wrong count
                        logging.warning(
                            f"Incorrect number/failed hash calculation for {video_path}. Skipping."
                        )

                except Exception as e:
                    logging.error(f"Exception retrieving result for {video_path}: {e}")

                if processed_count % 50 == 0 or processed_count == len(
                    videos_to_process
                ):
                    logging.info(
                        f"Processed {processed_count}/{len(videos_to_process)} videos for hashing."
                    )
    else:
        logging.info("No new videos need hash calculation.")

    # --- Step 3: Update Cache (after processing finishes) ---
    if newly_cached_hashes:
        logging.info(
            f"Updating cache with {len(newly_cached_hashes)} new/updated entries..."
        )
        try:
            with shelve.open(cache_path, flag="c") as cache:
                for video_path, data_to_cache in newly_cached_hashes.items():
                    try:
                        cache[video_path] = data_to_cache
                    except Exception as e:
                        logging.error(
                            f"Failed to write entry for {video_path} to cache: {e}"
                        )
                # cache.sync() # Ensure data is written (often done on close)
            logging.info("Cache update complete.")
        except Exception as e:
            logging.error(
                f"Failed to open or write to cache file {cache_path} for updates: {e}"
            )

    valid_videos = list(video_hashes.keys())
    logging.info(
        f"Total videos with valid hashes (cached + calculated): {len(valid_videos)}"
    )

    if len(valid_videos) < 2:
        logging.info("Not enough videos with valid hashes to compare.")
        return []

    # --- Step 4: Compare Video Pairs (Same as before) ---
    logging.info(
        f"Comparing {len(valid_videos)} videos ({len(list(itertools.combinations(valid_videos, 2)))} pairs)..."
    )
    similar_pairs = []
    compared_count = 0
    total_comparisons = len(valid_videos) * (len(valid_videos) - 1) // 2

    for video1, video2 in itertools.combinations(valid_videos, 2):
        # Ensure hashes are actually available before comparing
        if video1 not in video_hashes or video2 not in video_hashes:
            logging.warning(
                f"Missing hashes for comparison pair: {os.path.basename(video1)}, {os.path.basename(video2)}"
            )
            continue

        hashes1 = video_hashes[video1]
        hashes2 = video_hashes[video2]

        # Pass hash_size to compare_hashes for correct normalization
        similarity = compare_hashes(hashes1, hashes2, hash_size=hash_size)

        if similarity >= similarity_threshold:
            similar_pairs.append(tuple(sorted((video1, video2))))  # Store sorted tuple
            logging.debug(
                f"Found similar pair: {os.path.basename(video1)} and {os.path.basename(video2)} (Similarity: {similarity:.2f}%)"
            )

        compared_count += 1
        if compared_count % 1000 == 0 or compared_count == total_comparisons:
            logging.info(f"Compared {compared_count}/{total_comparisons} pairs...")

    logging.info(
        f"Found {len(similar_pairs)} similar pairs above {similarity_threshold}% threshold."
    )

    # --- Step 5: Group Similar Videos (Same as before) ---
    if not similar_pairs:
        return []

    logging.info("Grouping similar videos...")
    groups = []
    visited = set()
    adj = defaultdict(list)
    all_similar_videos = set()
    for u, v in similar_pairs:
        adj[u].append(v)
        adj[v].append(u)
        all_similar_videos.add(u)
        all_similar_videos.add(v)

    for video in all_similar_videos:
        if video not in visited:
            current_group = set()
            q = [video]
            visited.add(video)
            while q:
                curr = q.pop(0)
                current_group.add(curr)
                for neighbor in adj[curr]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        q.append(neighbor)
            if len(current_group) > 1:
                groups.append(current_group)

    logging.info(f"Found {len(groups)} groups of similar videos.")
    return groups


# --- Example Usage ---
if __name__ == "__main__":
    target_directory = input("Enter the directory containing videos: ")
    # Example: target_directory = '/home/user/my_videos'

    if not os.path.isdir(target_directory):
        print(f"Error: Directory not found: {target_directory}")
    else:
        similarity_level = 80.0
        cache_file = ".video_duplicate_cache"  # Specific name for this script's cache

        print(f"\nStarting video similarity detection in '{target_directory}'...")
        print(f"Similarity threshold: {similarity_level}%")
        print(f"Frames sampled per video: {NUM_FRAMES_TO_SAMPLE}")
        print(f"Hash size: {HASH_SIZE}x{HASH_SIZE}")
        print(
            f"Using cache file: {os.path.join(target_directory, cache_file + '.db')}"
        )  # Show expected cache path
        print("-" * 30)

        similar_video_groups = find_similar_videos(
            directory=target_directory,
            similarity_threshold=similarity_level,
            num_frames=NUM_FRAMES_TO_SAMPLE,
            hash_size=HASH_SIZE,
            cache_filename=cache_file,
        )

        print("-" * 30)
        if not similar_video_groups:
            print("No similar video groups found.")
        else:
            print(f"Found {len(similar_video_groups)} groups of similar videos:")
            for i, group in enumerate(similar_video_groups):
                print(f"\nGroup {i + 1}:")
                # Print absolute paths since those are stored/used
                for video_path in sorted(list(group)):
                    try:
                        size = os.path.getsize(video_path)
                        size_str = human_readable_size(size)
                    except Exception:
                        size_str = "Unknown size"
                    print(f"  - {video_path}  [Size: {size_str}]")
