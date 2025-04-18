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
    cap = None  # Initialize cap to None
    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            logging.warning(f"Could not open video file: {video_path}")
            return None

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        # Avoid division by zero if fps is 0
        fps = cap.get(cv2.CAP_PROP_FPS)
        duration = total_frames / fps if fps and fps > 0 else 0

        # --- Combined check for skipping based on duration or frame count ---
        if duration < skip_duration or total_frames < num_frames:
            logging.info(
                f"Skipping video: {os.path.basename(video_path)} (Duration: {duration:.2f}s < {skip_duration}s or Frames: {total_frames} < {num_frames})"
            )
            cap.release()
            return []  # Return empty list for intentionally skipped videos

        frame_hashes = []
        # Ensure indices are within bounds
        indices = np.linspace(0, max(0, total_frames - 1), num_frames, dtype=int)

        for frame_idx in indices:
            # Setting frame position can be slow/inaccurate; reading sequentially might be better if performance is an issue
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
            ret, frame = cap.read()
            if ret:
                # Convert color space before creating PIL image
                img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
                h = imagehash.average_hash(img, hash_size=hash_size)
                frame_hashes.append(h)
            else:
                # Log specific frame read failures
                logging.warning(f"Could not read frame {frame_idx} from {video_path}")

        cap.release()  # Ensure release even if loop finishes normally

        if not frame_hashes:
            logging.warning(f"No frames could be processed for: {video_path}")
            return []  # Return empty list if no hashes generated

        # Check if the number of hashes matches the requested number
        if len(frame_hashes) != num_frames:
            logging.warning(
                f"Expected {num_frames} hashes but got {len(frame_hashes)} for {video_path}. Discarding result."
            )
            return []  # Treat as failure if count mismatch

        return frame_hashes

    except Exception as e:
        logging.error(f"Error processing video {video_path}: {e}")
        # Ensure cap is released in case of exception
        if cap is not None and cap.isOpened():
            cap.release()
        return None  # Indicate a processing error


def compare_hashes(hashes1, hashes2, hash_size=config.HASH_SIZE):
    """
    Compares two lists of hashes. Returns a similarity percentage.
    """
    # Ensure both lists are valid and have the same length
    if not hashes1 or not hashes2 or len(hashes1) != len(hashes2):
        # Log if the lengths mismatch, which shouldn't happen with prior checks
        if hashes1 and hashes2:
            logging.warning(
                f"Hash list length mismatch during comparison: {len(hashes1)} vs {len(hashes2)}"
            )
        return 0.0

    total_distance = 0
    num_hashes = len(hashes1)
    # Hash length in bits (e.g., 8x8 = 64 bits)
    hash_len_bits = hash_size * hash_size

    for i, (h1, h2) in enumerate(zip(hashes1, hashes2)):
        try:
            # Ensure h1 and h2 are valid imagehash objects before subtraction
            if h1 is None or h2 is None:
                logging.warning(
                    f"Invalid hash object (None) encountered during comparison at index {i}."
                )
                # Penalize similarity heavily if hashes are invalid
                distance = hash_len_bits
            else:
                # Hamming distance calculation is built into imagehash
                distance = h1 - h2
            total_distance += distance
        except TypeError as e:
            # Handle unexpected types
            logging.warning(
                f"TypeError during hash comparison at index {i}: {e}. Hashes: {h1}, {h2}"
            )
            # Penalize similarity heavily
            total_distance += hash_len_bits
        except Exception as e:
            # Catch other potential errors during comparison
            logging.error(f"Unexpected error during hash comparison at index {i}: {e}")
            total_distance += hash_len_bits

    # Avoid division by zero if num_hashes is somehow 0
    if num_hashes == 0:
        return 0.0

    average_distance = total_distance / num_hashes

    # Ensure hash_len_bits is not zero
    if hash_len_bits == 0:
        logging.error("Hash size resulted in zero bits, cannot calculate similarity.")
        return 0.0

    # Convert average distance to similarity percentage
    # Ensure similarity doesn't go below 0 due to potential floating point issues
    similarity = max(0.0, (hash_len_bits - average_distance) / hash_len_bits) * 100
    return similarity
