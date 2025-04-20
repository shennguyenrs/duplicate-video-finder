import logging
import shelve
import os
from contextlib import closing

# Keys used within the shelve database
# _WATCHED_HASHES_KEY = "watched_hashes" # Old key, replaced by _WATCHED_VIDEOS_DATA_KEY
_WATCHED_VIDEOS_DATA_KEY = (
    "watched_videos_data"  # Stores {video_identifier: {hash_set}}
)
_METADATA_KEY = "metadata"  # Stores hashing parameters


def load_watched_videos_data(db_path_arg):
    """
    Loads the dictionary of watched videos and their hashes from a shelve database.

    Args:
        db_path_arg (str): Path to the watched database file. Can include common
                           shelve extensions (.db, .dat, .bak) or be the base path.

    Returns:
        tuple: A tuple containing (watched_videos_dict, metadata_dict).
               - watched_videos_dict (dict): {video_identifier: {hash_set}}. Empty if none found.
               - metadata_dict (dict | None): Dictionary with 'num_frames' and 'hash_size'
                 if found, otherwise None.
    """
    watched_videos_data = {}
    metadata = None
    possible_extensions = [
        ".db",
        ".dat",
        ".bak",
        "",
    ]  # Check common extensions + no extension
    base_path = db_path_arg  # Assume it's the base path initially
    actual_file_path = None  # The full path to the file that exists

    # Check if the provided path already ends with a known extension
    for ext in possible_extensions:
        if ext and db_path_arg.endswith(ext):
            # If it ends with an extension, derive the base path
            base_path = db_path_arg[: -len(ext)]
            # Check if this specific file exists
            if os.path.exists(db_path_arg):
                actual_file_path = db_path_arg
            break  # Found a potential extension match

    # If no extension was found in the argument, or if the file with the extension didn't exist,
    # check for files based on the (potentially derived) base_path
    if actual_file_path is None:
        for ext in possible_extensions:
            check_path = f"{base_path}{ext}"
            if os.path.exists(check_path):
                actual_file_path = check_path  # Store the full path that exists
                # Keep base_path as is, shelve.open needs the base path
                break

    if actual_file_path is None:
        # Use the original argument in the message for clarity to the user
        logging.info(f"Watched database not found near '{db_path_arg}'.")
        return watched_videos_data, metadata  # Return empty dict

    logging.debug(
        f"Attempting to open shelve with base path '{base_path}' (found file: {actual_file_path})"
    )

    try:
        # Use closing to ensure the shelve file is closed properly
        # *** Always pass the BASE path to shelve.open ***
        with closing(
            shelve.open(base_path, flag="r")
        ) as db:  # Open read-only using base path
            # Retrieve the dictionary of videos, default to empty dict if key missing
            watched_videos_data = db.get(_WATCHED_VIDEOS_DATA_KEY, {})
            # Ensure video data is a dictionary
            if not isinstance(watched_videos_data, dict):
                logging.warning(
                    f"Data under key '{_WATCHED_VIDEOS_DATA_KEY}' in {actual_file_path} is not a dictionary. Treating as empty."
                )
                watched_videos_data = {}

            # Retrieve metadata, default to None if key missing
            metadata = db.get(_METADATA_KEY, None)
            if metadata is not None and not isinstance(metadata, dict):
                logging.warning(
                    f"Data under key '{_METADATA_KEY}' in {actual_file_path} is not a dictionary. Ignoring metadata."
                )
                metadata = None  # Ignore invalid metadata

        log_msg = f"Loaded {len(watched_videos_data)} video entries"
        if metadata:
            log_msg += f" and metadata ({metadata.get('num_frames')} frames, {metadata.get('hash_size')} hash_size)"
        # Report the actual file path found in the log message
        log_msg += f" from watched database: {actual_file_path}"
        logging.info(log_msg)

    except Exception as e:
        # Report the base path used and the actual file path found in the error
        logging.error(
            f"Error loading watched database (base path '{base_path}', file '{actual_file_path}'): {e}. Proceeding with empty data and no metadata."
        )
        return {}, None  # Return empty dict and None metadata on error

    return watched_videos_data, metadata


def add_video_to_watched_db(
    db_path, video_identifier, video_hashes_set, num_frames, hash_size
):
    """
    Adds or updates a single video's entry (identifier and hash set) and associated
    metadata in the watched video database.

    Args:
        db_path (str): Path to the watched database file (base path, without extension).
        video_identifier (str): A unique identifier for the video (e.g., absolute path).
        video_hashes_set (set): A set of hash strings for this specific video.
        num_frames (int): The number of frames used to generate these hashes.
        hash_size (int): The hash size used to generate these hashes.
    """
    if not video_identifier:
        logging.error("Internal error: video_identifier must be provided.")
        return
    if not isinstance(video_hashes_set, set):
        logging.error("Internal error: video_hashes_set must be provided as a set.")
        return
    if num_frames is None or hash_size is None:
        logging.error(
            "Internal error: num_frames and hash_size must be provided to store metadata."
        )
        return
    # Allow adding videos with empty hash sets if needed, though usually they'd have hashes.
    # if not video_hashes_set:
    #     logging.info(f"No hashes provided for video '{video_identifier}'. Adding entry without hashes.")
    #     # Continue to add the entry, potentially with an empty set

    logging.info(
        f"Attempting to add/update entry for video '{os.path.basename(video_identifier)}' in watched database: {db_path}"
    )
    video_added = False
    video_updated = False
    try:
        # Use 'c' flag: open for read/write, create if doesn't exist
        with closing(shelve.open(db_path, flag="c")) as db:
            # Get the current dictionary, default to empty dict if key doesn't exist
            current_videos_data = db.get(_WATCHED_VIDEOS_DATA_KEY, {})

            # Ensure it's a dictionary before proceeding
            if not isinstance(current_videos_data, dict):
                logging.warning(
                    f"Existing data under key '{_WATCHED_VIDEOS_DATA_KEY}' in {db_path} is not a dictionary. Overwriting with new data."
                )
                current_videos_data = {}  # Reset if data is corrupted

            # Convert hashes in the input set to string representation
            # Ensure all stored hashes are strings.
            hashes_to_store_str = {str(h) for h in video_hashes_set}

            # Check if video exists and if hashes are different
            if video_identifier not in current_videos_data:
                video_added = True
            elif current_videos_data[video_identifier] != hashes_to_store_str:
                video_updated = True

            # Add or update the video entry
            current_videos_data[video_identifier] = hashes_to_store_str
            final_video_count = len(current_videos_data)

            # Prepare metadata dictionary - always update/store it
            metadata_to_store = {"num_frames": num_frames, "hash_size": hash_size}

            # Save the updated dictionary and metadata back to the database
            db[_WATCHED_VIDEOS_DATA_KEY] = current_videos_data
            db[_METADATA_KEY] = metadata_to_store  # Store/overwrite metadata

        action_str = ""
        if video_added:
            action_str = "added new entry for"
        elif video_updated:
            action_str = "updated entry for"
        else:
            action_str = "refreshed entry for"  # Hashes were the same

        logging.info(
            f"Successfully {action_str} video '{os.path.basename(video_identifier)}' ({len(hashes_to_store_str)} hashes) "
            f"and updated metadata (frames={num_frames}, hash_size={hash_size}) in {db_path}. "
            f"Total watched videos: {final_video_count}."
        )

    except Exception as e:
        logging.error(
            f"Error updating watched database {db_path} for video '{video_identifier}': {e}"
        )
