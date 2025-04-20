import argparse
import logging

from . import config


def parse_arguments():
    """Parses command-line arguments for the video finder."""
    parser = argparse.ArgumentParser(
        description=(
            "Find similar/duplicate video files in a directory based on perceptual hashing (standard mode), "
            "or populate a watched videos database from a source directory (--create-watched-db-from mode)."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Mutually exclusive operation modes
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "directory",
        nargs="?",
        help="Standard Mode: Path to the directory containing video files to scan for duplicates/watched.",
    )
    mode_group.add_argument(
        "--create-watched-db-from",
        metavar="<source_directory>",
        dest="create_watched_source",
        help="Create Mode: Path to a directory whose video files should ALL be added to the watched database.",
    )
    mode_group.add_argument(
        "--inspect-db",
        metavar="<db_path>",
        help="Inspect Mode: Show parameters and hash count stored in a watched database file. Does not perform scanning.",
    )

    # Core hashing options
    core_group = parser.add_argument_group(
        "Core Hashing Options (used in find/create modes)"
    )
    core_group.add_argument(
        "-t",
        "--threshold",
        type=float,
        default=config.DEFAULT_THRESHOLD,
        help="Similarity threshold percentage (0-100). Used in standard mode to find similar videos.",
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
        help="Base name for the hash cache file (will be stored in the scanned directory).",
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

    # Watched videos database options
    watched_group = parser.add_argument_group(
        "Watched Video Options (Standard Mode & Create Mode)"
    )
    watched_group.add_argument(
        "--watched-db",
        type=str,
        default=None,
        metavar="<path>",
        help=(
            f"Path to the watched videos database file (e.g., '{config.DEFAULT_WATCHED_DB_FILENAME}'). "
            "In Standard Mode: Used to identify watched videos. "
            "In Create Mode: Specifies the database file to create/update (defaults to <source_directory>/.watched_videos.db if omitted)."
        ),
    )

    # General options
    general_group = parser.add_argument_group("General Options")
    general_group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG level) logging.",
    )

    args = parser.parse_args()

    # Argument validation
    if not 0 <= args.threshold <= 100:
        parser.error(f"Threshold must be between 0 and 100, got {args.threshold}")
    if args.frames <= 0:
        parser.error(f"Number of frames must be positive, got {args.frames}")
    if args.hash_size <= 1:
        parser.error(f"Hash size must be greater than 1, got {args.hash_size}")
    if args.workers <= 0:
        parser.error(f"Number of workers must be positive, got {args.workers}")
    if args.skip_duration < 0:
        parser.error(f"Skip duration cannot be negative, got {args.skip_duration}")

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Verbose logging enabled.")
    else:
        logging.getLogger().setLevel(logging.INFO)

    return args
