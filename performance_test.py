import os
import sys
import shutil
import time
import logging
import csv
import subprocess
from collections import defaultdict
from datetime import datetime

# --- CONFIGURATION ---
# IMPORTANT: Provide paths to at least one or two real video files.
# The script will create copies of these to build the test set.
BASE_VIDEO_FILES = [
    # Example: "/path/to/your/video1.mp4",
]

# --- Test Parameters ---
TEST_DIR = "temp_perf_test_videos"
LOG_FILE = "performance_test.log"
STATS_FILE = "performance_stats.csv"
NUM_VIDEOS_TO_SCAN = 100
NUM_WATCHED_VIDEOS = 100
# Note: A total of NUM_VIDEOS_TO_SCAN + NUM_WATCHED_VIDEOS files will be created.

TEST_HASH_SIZE = 8
TEST_NUM_FRAMES = 10
TEST_THRESHOLD = 90.0

# --- SCRIPT SETUP ---

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

try:
    from video_finder.core import (
        calculate_all_hashes,
        identify_watched_videos,
        find_similar_groups,
    )
    from video_finder import config
except ImportError as e:
    print(
        f"Fatal Error: Could not import video_finder modules. Ensure your environment is set up correctly."
    )
    print(f"Details: {e}")
    sys.exit(1)


def setup_logging():
    """Configures logging to both console and file."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="w"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def get_git_commit_hash():
    """Retrieves the current Git commit hash."""
    try:
        commit_hash = (
            subprocess.check_output(["git", "rev-parse", "HEAD"])
            .strip()
            .decode("utf-8")
        )
        return commit_hash
    except (subprocess.CalledProcessError, FileNotFoundError):
        logging.warning(
            "Could not get Git commit hash. Not a Git repository or Git is not installed."
        )
        return "N/A"


def setup_test_directory():
    """Prepares a clean directory with numerous video files for testing."""
    if not BASE_VIDEO_FILES or not all(os.path.exists(p) for p in BASE_VIDEO_FILES):
        logging.error(
            "Please configure the BASE_VIDEO_FILES list in this script with valid paths to video files."
        )
        sys.exit(1)

    logging.info(f"--- Setting up test directory: '{TEST_DIR}' ---")
    if os.path.exists(TEST_DIR):
        logging.info("Removing old test directory.")
        shutil.rmtree(TEST_DIR)

    scan_dir = os.path.join(TEST_DIR, "videos_to_scan")
    watched_dir = os.path.join(TEST_DIR, "watched_videos")
    os.makedirs(scan_dir)
    os.makedirs(watched_dir)

    logging.info(f"Generating {NUM_VIDEOS_TO_SCAN} files for scanning...")
    for i in range(NUM_VIDEOS_TO_SCAN):
        base_file = BASE_VIDEO_FILES[i % len(BASE_VIDEO_FILES)]
        _, ext = os.path.splitext(base_file)
        shutil.copy(base_file, os.path.join(scan_dir, f"scan_video_{i}{ext}"))

    logging.info(f"Generating {NUM_WATCHED_VIDEOS} files for the watched database...")
    for i in range(NUM_WATCHED_VIDEOS):
        base_file = BASE_VIDEO_FILES[i % len(BASE_VIDEO_FILES)]
        _, ext = os.path.splitext(base_file)
        shutil.copy(base_file, os.path.join(watched_dir, f"watched_video_{i}{ext}"))

    logging.info("Setup complete.\n")
    return scan_dir, watched_dir


def time_it(description, stats_dict, func, *args, **kwargs):
    """Times a function and stores the result in a dictionary."""
    logging.info(f"--- Timing: {description} ---")
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    duration = end_time - start_time
    stats_dict[description] = duration
    logging.info(f"--- Completed: {description} in {duration:.4f} seconds ---\n")
    return result


def write_stats_to_csv(stats_dict):
    """Appends a dictionary of statistics to a CSV file."""
    file_exists = os.path.isfile(STATS_FILE)

    # Order the fields for consistent CSV writing
    fieldnames = [
        "timestamp",
        "commit_hash",
        "Hashing all videos to scan",
        "Identifying watched videos",
        "Finding similar/duplicate groups",
    ]

    with open(STATS_FILE, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        # Ensure all fields are present in the dictionary, even if empty
        row_to_write = {field: stats_dict.get(field, "N/A") for field in fieldnames}
        writer.writerow(row_to_write)

    logging.info(f"Performance stats appended to '{STATS_FILE}'")


def main():
    """Main function to run the performance test suite."""
    setup_logging()

    # Suppress verbose logging from the video_finder library itself
    logging.getLogger("video_finder").setLevel(logging.WARNING)

    commit_hash = get_git_commit_hash()
    logging.info(f"Starting performance test for commit: {commit_hash}")
    logging.info(
        f"Test parameters: Scan Videos={NUM_VIDEOS_TO_SCAN}, Watched Videos={NUM_WATCHED_VIDEOS}"
    )

    stats = {"timestamp": datetime.now().isoformat(), "commit_hash": commit_hash}

    scan_dir, watched_dir = setup_test_directory()

    # --- 1. Test Hashing Performance ---
    all_video_hashes = time_it(
        "Hashing all videos to scan",
        stats,
        calculate_all_hashes,
        directory=scan_dir,
        num_frames=TEST_NUM_FRAMES,
        hash_size=TEST_HASH_SIZE,
    )

    # --- 2. Test Watched Video Identification Performance ---
    logging.info("Pre-calculating hashes for watched DB (not timed)...")
    watched_video_hashes = calculate_all_hashes(
        directory=watched_dir, num_frames=TEST_NUM_FRAMES, hash_size=TEST_HASH_SIZE
    )

    unwatched_hashes = all_video_hashes  # Start by assuming all videos are unwatched

    if not watched_video_hashes:
        logging.warning(
            "No watched video data generated. Skipping watched video identification."
        )
    else:
        # First, structure the watched data into the correct format
        watched_videos_data_structured = defaultdict(set)
        for path, hashes in watched_video_hashes.items():
            watched_videos_data_structured[path] = {str(h) for h in hashes}

        # Now, run the identification with the correctly structured data
        unwatched_hashes = time_it(
            "Identifying watched videos",
            stats,
            identify_watched_videos,
            video_hashes_map=all_video_hashes,
            watched_videos_data=watched_videos_data_structured,
            hash_size=TEST_HASH_SIZE,
            similarity_threshold=TEST_THRESHOLD,
        )[1]

    # --- 3. Test Duplicate Finding Performance ---
    # This block will now ALWAYS run, as long as there are hashes.
    if unwatched_hashes:
        time_it(
            "Finding similar/duplicate groups",
            stats,
            find_similar_groups,
            video_hashes_map=unwatched_hashes,
            hash_size=TEST_HASH_SIZE,
            similarity_threshold=TEST_THRESHOLD,
        )
    else:
        logging.warning("No unwatched videos left to check for duplicates.")

    write_stats_to_csv(stats)

    logging.info("--- Performance Test Finished ---")
    logging.info(f"Logs are available in '{LOG_FILE}'")

    # Optional: Clean up the large temporary directory
    # cleanup = input(f"Do you want to delete the '{TEST_DIR}' directory? [y/N]: ")
    # if cleanup.lower() == 'y':
    #     print(f"Removing '{TEST_DIR}'...")
    #     shutil.rmtree(TEST_DIR)
    #     print("Cleanup complete.")


if __name__ == "__main__":
    main()
