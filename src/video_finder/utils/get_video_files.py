import logging
import os

from .. import config


def get_video_files(directory, recursive=False):
    """
    Finds video files in a directory.

    Args:
        directory (str): The path to the directory to scan.
        recursive (bool): If True, scans subdirectories recursively, skipping the
                          duplicate directory. If False, scans only the top-level directory.

    Returns:
        list: A list of absolute paths to found video files.
    """
    video_files = []
    duplicate_dir_name = config.DEFAULT_DUPLICATE_DIR_NAME
    try:
        if recursive:
            logging.debug(f"Recursively scanning {directory}...")
            # Prevent descending into the duplicate directory
            for root, dirnames, files in os.walk(directory, topdown=True):
                if duplicate_dir_name in dirnames:
                    logging.debug(
                        f"Skipping duplicate directory: {os.path.join(root, duplicate_dir_name)}"
                    )
                    dirnames.remove(duplicate_dir_name)
                for file in files:
                    if os.path.splitext(file)[1].lower() in config.VIDEO_EXTENSIONS:
                        video_files.append(os.path.abspath(os.path.join(root, file)))
        else:
            logging.debug(f"Scanning non-recursively: {directory}...")
            for item in os.listdir(directory):
                item_path = os.path.join(directory, item)
                if (
                    os.path.isfile(item_path)
                    and os.path.splitext(item)[1].lower() in config.VIDEO_EXTENSIONS
                ):
                    video_files.append(os.path.abspath(item_path))

    except FileNotFoundError:
        logging.error(f"Directory not found: {directory}")
    except PermissionError:
        logging.error(f"Permission denied accessing directory: {directory}")
    except Exception as e:
        logging.error(f"Error scanning directory {directory}: {e}")

    return video_files
