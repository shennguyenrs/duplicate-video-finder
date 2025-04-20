import logging
import shelve
import os
from contextlib import closing

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
    possible_extensions = [".db", ".dat", ".bak", ""]
    base_path = db_path_arg
    actual_file_path = None

    # Detect the correct file path and base path for shelve
    for ext in possible_extensions:
        if ext and db_path_arg.endswith(ext):
            base_path = db_path_arg[: -len(ext)]
            if os.path.exists(db_path_arg):
                actual_file_path = db_path_arg
            break

    if actual_file_path is None:
        for ext in possible_extensions:
            check_path = f"{base_path}{ext}"
            if os.path.exists(check_path):
                actual_file_path = check_path
                break

    if actual_file_path is None:
        logging.info(f"Watched database not found near '{db_path_arg}'.")
        return watched_videos_data, metadata

    logging.debug(
        f"Attempting to open shelve with base path '{base_path}' (found file: {actual_file_path})"
    )

    try:
        with closing(shelve.open(base_path, flag="r")) as db:
            watched_videos_data = db.get(_WATCHED_VIDEOS_DATA_KEY, {})
            if not isinstance(watched_videos_data, dict):
                logging.warning(
                    f"Data under key '{_WATCHED_VIDEOS_DATA_KEY}' in {actual_file_path} is not a dictionary. Treating as empty."
                )
                watched_videos_data = {}

            metadata = db.get(_METADATA_KEY, None)
            if metadata is not None and not isinstance(metadata, dict):
                logging.warning(
                    f"Data under key '{_METADATA_KEY}' in {actual_file_path} is not a dictionary. Ignoring metadata."
                )
                metadata = None

        log_msg = f"Loaded {len(watched_videos_data)} video entries"
        if metadata:
            log_msg += f" and metadata ({metadata.get('num_frames')} frames, {metadata.get('hash_size')} hash_size)"
        log_msg += f" from watched database: {actual_file_path}"
        logging.info(log_msg)

    except Exception as e:
        logging.error(
            f"Error loading watched database (base path '{base_path}', file '{actual_file_path}'): {e}. Proceeding with empty data and no metadata."
        )
        return {}, None

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

    logging.info(
        f"Attempting to add/update entry for video '{os.path.basename(video_identifier)}' in watched database: {db_path}"
    )
    video_added = False
    video_updated = False
    try:
        with closing(shelve.open(db_path, flag="c")) as db:
            current_videos_data = db.get(_WATCHED_VIDEOS_DATA_KEY, {})
            if not isinstance(current_videos_data, dict):
                logging.warning(
                    f"Existing data under key '{_WATCHED_VIDEOS_DATA_KEY}' in {db_path} is not a dictionary. Overwriting with new data."
                )
                current_videos_data = {}

            hashes_to_store_str = {str(h) for h in video_hashes_set}

            if video_identifier not in current_videos_data:
                video_added = True
            elif current_videos_data[video_identifier] != hashes_to_store_str:
                video_updated = True

            current_videos_data[video_identifier] = hashes_to_store_str
            final_video_count = len(current_videos_data)

            metadata_to_store = {"num_frames": num_frames, "hash_size": hash_size}

            db[_WATCHED_VIDEOS_DATA_KEY] = current_videos_data
            db[_METADATA_KEY] = metadata_to_store

        if video_added:
            action_str = "added new entry for"
        elif video_updated:
            action_str = "updated entry for"
        else:
            action_str = "refreshed entry for"

        logging.info(
            f"Successfully {action_str} video '{os.path.basename(video_identifier)}' ({len(hashes_to_store_str)} hashes) "
            f"and updated metadata (frames={num_frames}, hash_size={hash_size}) in {db_path}. "
            f"Total watched videos: {final_video_count}."
        )

    except Exception as e:
        logging.error(
            f"Error updating watched database {db_path} for video '{video_identifier}': {e}"
        )
