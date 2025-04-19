import argparse
import logging

from . import config


def parse_arguments():
    """Parses command-line arguments for the video finder."""
    parser = argparse.ArgumentParser(
        description="Find similar video files in a directory based on perceptual hashing. Can optionally filter against a watched videos database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # --- Core Scan Arguments ---
    core_group = parser.add_argument_group("Core Scan Options")
    core_group.add_argument(
        "directory", help="The path to the directory containing video files to scan."
    )
    core_group.add_argument(
        "-t",
        "--threshold",
        type=float,
        default=config.DEFAULT_THRESHOLD,
        help="Similarity threshold percentage (0-100). Pairs above this are considered similar.",
    )
    core_group.add_argument(
        "-f",
        "--frames",
        type=int,
        default=config.NUM_FRAMES_TO_SAMPLE,
        help="Number of frames to sample per video for hashing.",
    )
    core_group.add_argument(
        "-s",
        "--hash-size",
        type=int,
        default=config.HASH_SIZE,
        help="Size of the perceptual hash grid (e.g., 8 for 8x8).",
    )
    core_group.add_argument(
        "-c",
        "--cache-file",
        default=config.DEFAULT_CACHE_FILENAME,
        help="Base name for the hash cache file (will be stored in the target directory).",
    )
    core_group.add_argument(
        "-w",
        "--workers",
        type=int,
        default=config.MAX_WORKERS,
        help="Maximum number of worker threads for parallel processing.",
    )
    core_group.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        default=False,
        help="Scan subdirectories recursively. If disabled, only scans the top-level directory.",
    )
    core_group.add_argument(
        "--skip-duration",
        type=int,
        default=config.DEFAULT_SKIP_DURATION_SECONDS,
        help="Minimum video duration in seconds. Videos shorter than this will be skipped during hashing.",
    )

    # --- Watched Videos Arguments ---
    watched_group = parser.add_argument_group("Watched Video Options")
    watched_group.add_argument(
        "--watched-db",
        type=str,
        default=None,
        metavar="<path>",
        help=f"Path to the watched videos database file (e.g., '{config.DEFAULT_WATCHED_DB_FILENAME}'). If provided, videos matching hashes in this DB will be identified as 'watched'.",
    )
    watched_group.add_argument(
        "--update-watched-db",
        action="store_true",
        default=False,
        help="If --watched-db is provided, update the database with hashes of unique, unwatched videos found during this scan.",
    )

    # --- General Arguments ---
    general_group = parser.add_argument_group("General Options")
    general_group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG level) logging.",
    )

    args = parser.parse_args()

    # --- Argument Validation ---
    if args.update_watched_db and not args.watched_db:
        parser.error("--update-watched-db requires --watched-db to be specified.")

    # Validate threshold range
    if not 0 <= args.threshold <= 100:
        parser.error(f"Threshold must be between 0 and 100, got {args.threshold}")

    # Validate positive integers where applicable
    if args.frames <= 0:
        parser.error(f"Number of frames must be positive, got {args.frames}")
    if (
        args.hash_size <= 1
    ):  # Hash size must be at least 2x2 technically, but 1 is definitely invalid
        parser.error(f"Hash size must be greater than 1, got {args.hash_size}")
    if args.workers <= 0:
        parser.error(f"Number of workers must be positive, got {args.workers}")
    if args.skip_duration < 0:
        parser.error(f"Skip duration cannot be negative, got {args.skip_duration}")

    # Adjust logging level if verbose flag is set
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Verbose logging enabled.")
    else:
        # Ensure default level is INFO if not verbose
        logging.getLogger().setLevel(logging.INFO)

    return args
