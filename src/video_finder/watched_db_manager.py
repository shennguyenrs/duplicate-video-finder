import logging
import shelve
import os
from contextlib import closing

# Key used within the shelve database to store the set of hashes
_WATCHED_HASHES_KEY = "watched_hashes"


def load_watched_hashes(db_path):
    """
    Loads the set of watched video hash strings from a shelve database.

    Args:
        db_path (str): Path to the watched database file (without extension).

    Returns:
        set: A set containing watched hash strings. Returns an empty set if
             the file doesn't exist or contains no hashes.
    """
    watched_hashes = set()
    # Shelve might add extensions like .db, .dat, .bak. Check common ones.
    possible_extensions = [".db", ".dat", ".bak", ""]  # Check without extension too
    found_path = None
    for ext in possible_extensions:
        check_path = f"{db_path}{ext}"
        if os.path.exists(check_path):
            found_path = db_path  # Use the base path for shelve.open
            logging.debug(f"Found watched database file: {check_path}")
            break

    if found_path is None:
        logging.info(f"Watched database not found near '{db_path}', starting fresh.")
        return watched_hashes

    try:
        # Use closing to ensure the shelve file is closed properly
        with closing(shelve.open(found_path, flag="r")) as db:  # Open read-only
            # Retrieve the set of hashes, default to empty set if key missing
            watched_hashes = db.get(_WATCHED_HASHES_KEY, set())
            # Ensure it's actually a set
            if not isinstance(watched_hashes, set):
                logging.warning(
                    f"Data under key '{_WATCHED_HASHES_KEY}' in {found_path} is not a set. Ignoring."
                )
                return set()
        logging.info(
            f"Loaded {len(watched_hashes)} hashes from watched database: {found_path}"
        )
    except Exception as e:
        logging.error(
            f"Error loading watched database {found_path}: {e}. Proceeding with empty set."
        )
        return set()  # Return empty set on error

    return watched_hashes


def add_hashes_to_watched_db(db_path, new_hashes_set):
    """
    Adds a set of new hash strings to the watched video database.

    Args:
        db_path (str): Path to the watched database file (without extension).
        new_hashes_set (set): A set of hash strings to add.
    """
    if not isinstance(new_hashes_set, set):
        logging.error("Internal error: Hashes to add must be provided as a set.")
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

            # Save the updated set back to the database
            db[_WATCHED_HASHES_KEY] = current_hashes

        if updated_count > 0:
            logging.info(
                f"Successfully added {updated_count} unique hashes to {db_path}. Total watched hashes: {final_count}."
            )
        else:
            logging.info(
                f"No new unique hashes were added to {db_path} (they might have already existed). Total watched hashes: {final_count}."
            )

    except Exception as e:
        logging.error(f"Error updating watched database {db_path}: {e}")
