import argparse
import logging
import os

from . import config, core, utils

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """Main function to handle command-line arguments and run the video finder."""
    parser = argparse.ArgumentParser(
        description="Find similar video files in a directory based on perceptual hashing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "directory", help="The path to the directory containing video files to scan."
    )
    parser.add_argument(
        "-t",
        "--threshold",
        type=float,
        default=config.DEFAULT_THRESHOLD,
        help="Similarity threshold percentage (0-100). Pairs above this are considered similar.",
    )
    parser.add_argument(
        "-f",
        "--frames",
        type=int,
        default=config.NUM_FRAMES_TO_SAMPLE,
        help="Number of frames to sample per video for hashing.",
    )
    parser.add_argument(
        "-s",
        "--hash-size",
        type=int,
        default=config.HASH_SIZE,
        help="Size of the perceptual hash grid (e.g., 8 for 8x8).",
    )
    parser.add_argument(
        "-c",
        "--cache-file",
        default=config.DEFAULT_CACHE_FILENAME,
        help="Base name for the cache file (will be stored in the target directory).",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=config.MAX_WORKERS,
        help="Maximum number of worker threads for parallel processing.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG level) logging.",
    )

    args = parser.parse_args()

    # Adjust logging level if verbose flag is set
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Verbose logging enabled.")

    target_directory = args.directory

    if not os.path.isdir(target_directory):
        logging.error(f"Error: Directory not found: {target_directory}")
        return  # Exit if directory is invalid

    # Display settings being used
    print("-" * 30)
    print(
        f"Starting video similarity detection in: '{os.path.abspath(target_directory)}'"
    )
    print(f"Similarity threshold: {args.threshold}%")
    print(f"Frames sampled per video: {args.frames}")
    print(f"Hash size: {args.hash_size}x{args.hash_size}")
    cache_path_display = os.path.join(
        target_directory, args.cache_file + ".db"
    )
    print(f"Using cache file: ~{cache_path_display}")
    print(f"Max workers: {args.workers}")
    print("-" * 30)

    # Call the core logic from the core module
    try:
        similar_video_groups = core.find_similar_videos(
            directory=target_directory,
            similarity_threshold=args.threshold,
            num_frames=args.frames,
            hash_size=args.hash_size,
            cache_filename=args.cache_file,
            max_workers=args.workers,
        )
    except Exception as e:
        logging.exception(
            f"An unexpected error occurred during processing: {e}"
        )
        print("\nAn error occurred. Please check the logs.")
        return  # Exit on error

    # --- Output Results ---
    utils.print_similar_video_groups(similar_video_groups)


# --- Entry Point ---
if __name__ == "__main__":
    main()
