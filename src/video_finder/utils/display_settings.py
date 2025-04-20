import os


def display_settings(args, mode_name, primary_directory, db_path=None, cache_dir=None):
    """Prints the common settings block for different modes."""
    if cache_dir is None:
        cache_dir = primary_directory

    print("-" * 30)
    print(f"Mode: {mode_name}")
    print(f"Scanning directory: '{primary_directory}'")
    print(f"Frames sampled per video: {args.frames}")
    print(f"Hash size: {args.hash_size}x{args.hash_size}")
    print(f"Skip duration: {args.skip_duration} seconds")

    # Cache file path display
    cache_path_display = os.path.join(cache_dir, args.cache_file + ".db")
    try:
        # Try to make path relative for cleaner display, fallback to absolute
        rel_cache_path = os.path.relpath(cache_path_display)
        print(f"Using cache file: ~{rel_cache_path}")
    except ValueError:  # Handle cases like different drives on Windows
        print(f"Using cache file: {cache_path_display}")

    print(f"Max workers: {args.workers}")
    print(f"Recursive scan: {'Enabled' if args.recursive else 'Disabled'}")

    # Mode-specific settings
    if "Find Similar" in mode_name:  # Check based on mode name convention
        print(f"Similarity threshold: {args.threshold}%")
        if args.watched_db:  # Use args.watched_db directly as it's set by argparse
            print(f"Using watched database: {args.watched_db}")
            print(
                "Watched database will be updated with unique, unwatched videos found."
            )
    elif "Create Watched" in mode_name:
        if db_path:
            print(f"Creating/updating watched database: {db_path}")

    print("-" * 30)
