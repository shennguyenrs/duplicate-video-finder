import argparse
import logging

from . import config


def parse_arguments():
    """Parses command-line arguments for the video finder."""
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
        "-r",
        "--recursive",
        action="store_true",
        default=False,
        help="Scan subdirectories recursively. If disabled, only scans the top-level directory.",
    )
    parser.add_argument(
        "--skip-duration",
        type=int,
        default=config.DEFAULT_SKIP_DURATION_SECONDS,
        help="Minimum video duration in seconds. Videos shorter than this will be skipped.",
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

    return args
