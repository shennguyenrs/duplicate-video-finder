import logging
import shelve
import os
from contextlib import closing

# Keys used within the shelve database
_WATCHED_HASHES_KEY = "watched_hashes"
_METADATA_KEY = "metadata"


def load_watched_hashes(db_path_arg):
    """
    Loads the set of watched video hash strings from a shelve database.

    Args:
        db_path_arg (str): Path to the watched database file. Can include common
                           shelve extensions (.db, .dat, .bak) or be the base path.

    Returns:
        tuple: A tuple containing (watched_hashes_set, metadata_dict).
               - watched_hashes_set (set): Watched hash strings. Empty if none found.
               - metadata_dict (dict | None): Dictionary with 'num_frames' and 'hash_size'
                 if found, otherwise None.
    """
    watched_hashes = set()
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
        return watched_hashes, metadata

    logging.debug(
        f"Attempting to open shelve with base path '{base_path}' (found file: {actual_file_path})"
    )

    try:
        # Use closing to ensure the shelve file is closed properly
        # *** Always pass the BASE path to shelve.open ***
        with closing(
            shelve.open(base_path, flag="r")
        ) as db:  # Open read-only using base path
            # Retrieve the set of hashes, default to empty set if key missing
            watched_hashes = db.get(_WATCHED_HASHES_KEY, set())
            # Ensure hashes data is a set
            if not isinstance(watched_hashes, set):
                logging.warning(
                    f"Data under key '{_WATCHED_HASHES_KEY}' in {actual_file_path} is not a set. Treating as empty."
                )
                watched_hashes = set()

            # Retrieve metadata, default to None if key missing
            metadata = db.get(_METADATA_KEY, None)
            if metadata is not None and not isinstance(metadata, dict):
                logging.warning(
                    f"Data under key '{_METADATA_KEY}' in {actual_file_path} is not a dictionary. Ignoring metadata."
                )
                metadata = None  # Ignore invalid metadata

        log_msg = f"Loaded {len(watched_hashes)} hashes"
        if metadata:
            log_msg += f" and metadata ({metadata.get('num_frames')} frames, {metadata.get('hash_size')} hash_size)"
        # Report the actual file path found in the log message
        log_msg += f" from watched database: {actual_file_path}"
        logging.info(log_msg)

    except Exception as e:
        # Report the base path used and the actual file path found in the error
        logging.error(
            f"Error loading watched database (base path '{base_path}', file '{actual_file_path}'): {e}. Proceeding with empty set and no metadata."
        )
        return set(), None  # Return empty set and None metadata on error

    return watched_hashes, metadata


def add_hashes_to_watched_db(db_path, new_hashes_set, num_frames, hash_size):
    """
    Adds a set of new hash strings and associated metadata to the watched video database.

    Args:
        db_path (str): Path to the watched database file (base path, without extension).
        new_hashes_set (set): A set of hash strings to add.
        num_frames (int): The number of frames used to generate these hashes.
        hash_size (int): The hash size used to generate these hashes.
    """
    if not isinstance(new_hashes_set, set):
        logging.error("Internal error: Hashes to add must be provided as a set.")
        return
    if num_frames is None or hash_size is None:
        logging.error(
            "Internal error: num_frames and hash_size must be provided to store metadata."
        )
        return
    if not new_hashes_set:
        logging.info("No new hashes provided to add to the watched database.")
        return

    logging.info(
        f"Attempting to add {len(new_hashes_set)} new hashes to watched database: {db_path}"
    )
    updated_count = 0
    try:
        # Use 'c' flag: open for read/write, create if doesn't exist
        with closing(shelve.open(db_path, flag="c")) as db:
            # Get the current set, default to empty set if key doesn't exist
            current_hashes = db.get(_WATCHED_HASHES_KEY, set())

            # Ensure it's a set before proceeding
            if not isinstance(current_hashes, set):
                logging.warning(
                    f"Existing data under key '{_WATCHED_HASHES_KEY}' in {db_path} is not a set. Overwriting with new set."
                )
                current_hashes = set()  # Reset if data is corrupted

            initial_count = len(current_hashes)
            # Convert hashes in the input set to string representation if they aren't already
            # (Assuming hashes might be complex objects initially, though the plan implies strings)
            # For simplicity now, assume new_hashes_set contains strings or objects convertible to strings
            hashes_to_add_str = {str(h) for h in new_hashes_set}

            current_hashes.update(hashes_to_add_str)  # Add new string hashes
            final_count = len(current_hashes)
            updated_count = final_count - initial_count

            # Prepare metadata dictionary
            metadata_to_store = {"num_frames": num_frames, "hash_size": hash_size}

            # Save the updated set and metadata back to the database
            db[_WATCHED_HASHES_KEY] = current_hashes
            db[_METADATA_KEY] = metadata_to_store  # Store/overwrite metadata

        if updated_count > 0:
            logging.info(
                f"Successfully added {updated_count} unique hashes and updated metadata (frames={num_frames}, hash_size={hash_size}) in {db_path}. Total watched hashes: {final_count}."
            )
        else:
            logging.info(
                f"No new unique hashes were added to {db_path} (they might have already existed). Metadata updated (frames={num_frames}, hash_size={hash_size}). Total watched hashes: {final_count}."
            )

    except Exception as e:
        logging.error(f"Error updating watched database {db_path}: {e}")
