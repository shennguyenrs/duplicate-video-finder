import logging
import os

from .. import config


def get_video_files(directory):
    """Recursively finds all video files in a directory."""
    video_files = []
    try:
        for root, _, files in os.walk(directory):
            for file in files:
                if os.path.splitext(file)[1].lower() in config.VIDEO_EXTENSIONS:
                    # Store absolute paths for reliable caching keys
                    video_files.append(os.path.abspath(os.path.join(root, file)))
    except Exception as e:
        logging.error(f"Error walking directory {directory}: {e}")
    return video_files
