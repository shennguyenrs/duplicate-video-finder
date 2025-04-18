import os

VIDEO_EXTENSIONS = {".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv"}
NUM_FRAMES_TO_SAMPLE = 20
HASH_SIZE = 8
MAX_WORKERS = os.cpu_count()
DEFAULT_THRESHOLD = 90
DEFAULT_CACHE_FILENAME = ".video_hashes_cache"
DEFAULT_DUPLICATE_DIR_NAME = "duplicate_videos"
