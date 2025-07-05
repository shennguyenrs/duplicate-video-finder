import logging
import cv2
import imagehash
import numpy as np
from PIL import Image
import os

from . import config  # Relative import


def calculate_video_hashes(
    video_path,
    num_frames=config.NUM_FRAMES_TO_SAMPLE,
    hash_size=config.HASH_SIZE,
    skip_duration=config.DEFAULT_SKIP_DURATION_SECONDS,
):
    """
    Extracts frames from a video and calculates their average hashes.
    Returns a list of imagehash objects or None if an error occurs.
    """
    cap = None
    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            logging.warning(f"Could not open video file: {video_path}")
            return None

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        duration = total_frames / fps if fps and fps > 0 else 0

        # Skip short or very small videos
        if duration < skip_duration or total_frames < num_frames:
            logging.info(
                f"Skipping video: {os.path.basename(video_path)} (Duration: {duration:.2f}s < {skip_duration}s or Frames: {total_frames} < {num_frames})"
            )
            cap.release()
            return []

        frame_hashes = []
        indices = np.linspace(0, max(0, total_frames - 1), num_frames, dtype=int)

        for frame_idx in indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
            ret, frame = cap.read()
            if ret:
                resized_frame = cv2.resize(
                    frame, (32, 32), interpolation=cv2.INTER_AREA
                )
                img = Image.fromarray(cv2.cvtColor(resized_frame, cv2.COLOR_BGR2RGB))
                h = imagehash.average_hash(img, hash_size=hash_size)
                frame_hashes.append(h)
            else:
                logging.warning(f"Could not read frame {frame_idx} from {video_path}")

        cap.release()

        if not frame_hashes:
            logging.warning(f"No frames could be processed for: {video_path}")
            return []

        if len(frame_hashes) != num_frames:
            logging.warning(
                f"Expected {num_frames} hashes but got {len(frame_hashes)} for {video_path}. Discarding result."
            )
            return []

        return frame_hashes

    except Exception as e:
        logging.error(f"Error processing video {video_path}: {e}")
        if cap is not None and cap.isOpened():
            cap.release()
        return None


def compare_hashes(hashes1, hashes2, hash_size=config.HASH_SIZE):
    """
    Compares two lists of hashes. Returns a similarity percentage.
    """
    if not hashes1 or not hashes2 or len(hashes1) != len(hashes2):
        if hashes1 and hashes2:
            logging.warning(
                f"Hash list length mismatch during comparison: {len(hashes1)} vs {len(hashes2)}"
            )
        return 0.0

    total_distance = 0
    num_hashes = len(hashes1)
    hash_len_bits = hash_size * hash_size

    for i, (h1, h2) in enumerate(zip(hashes1, hashes2)):
        try:
            if h1 is None or h2 is None:
                logging.warning(
                    f"Invalid hash object (None) encountered during comparison at index {i}."
                )
                distance = hash_len_bits
            else:
                distance = h1 - h2
            total_distance += distance
        except TypeError as e:
            logging.warning(
                f"TypeError during hash comparison at index {i}: {e}. Hashes: {h1}, {h2}"
            )
            total_distance += hash_len_bits
        except Exception as e:
            logging.error(f"Unexpected error during hash comparison at index {i}: {e}")
            total_distance += hash_len_bits

    if num_hashes == 0 or hash_len_bits == 0:
        logging.error("Invalid hash or frame count, cannot calculate similarity.")
        return 0.0

    average_distance = total_distance / num_hashes
    similarity = max(0.0, (hash_len_bits - average_distance) / hash_len_bits) * 100
    return similarity
