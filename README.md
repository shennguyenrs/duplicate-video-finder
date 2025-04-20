# Duplicate Video Finder

A command-line tool to efficiently find and manage duplicate or similar video files in your directories using perceptual hashing. It also supports maintaining a "watched" videos database to help you filter out videos you've already seen.

---

## Features

- **Find Similar Videos:** Scans a directory for video files and groups them by visual similarity.
- **Automatic Duplicate Management:** Optionally moves duplicates to a separate folder for review or deletion.
- **Watched Videos Database:** Maintain a database of videos you've already watched and filter them out from future scans.
- **Create/Update Watched DB:** Easily create or update a watched database from a directory of known videos.
- **Flexible Hashing Options:** Configure the number of frames sampled and hash size for fine-tuned similarity detection.
- **Recursive Scanning:** Optionally scan subdirectories.
- **Performance:** Multi-threaded hashing for fast processing of large collections.
- **Verbose Logging:** Enable debug output for troubleshooting.

---

## Installation

Clone the repository and install dependencies:

```bash
git clone https://github.com/yourusername/duplicate-video-finder.git
cd duplicate-video-finder
pip install -r requirements.txt
```

---

## Usage

Run the CLI using Python:

```bash
python -m src.video_finder.cli <directory> [options]
```

### Main Modes

#### 1. Find Similar Videos (Default Mode)

Scan a directory for similar videos and manage duplicates.

```bash
python -m src.video_finder.cli /path/to/videos --threshold 85
```

**Key Options:**

- `-t, --threshold <int>`: Similarity threshold percentage (default: 90).
- `-f, --frames <int>`: Number of frames to sample per video (default: 20).
- `-s, --hash-size <int>`: Hash grid size (default: 8).
- `-c, --cache-file <name>`: Name for the hash cache file (default: .video_hashes_cache).
- `-w, --workers <int>`: Number of worker threads (default: all CPU cores).
- `-r, --recursive`: Enable recursive directory scanning.
- `--skip-duration <int>`: Minimum video duration in seconds (default: 10).
- `--watched-db <db_path>`: Path to a watched videos database (filters out watched videos).
- `-v, --verbose`: Enable verbose logging.

#### 2. Create/Update Watched Videos Database

Create a watched DB from a directory of known videos:

```bash
python -m src.video_finder.cli --create-watched-db-from /path/to/watched --watched-db /path/to/watched.db
```

- `--create-watched-db-from <dir>`: Source directory of watched videos (default: current directory).
- `--watched-db <db_path>`: Path to the database file to create/update (optional).

---

## Example Workflows

**Find and Move Duplicates:**

```bash
python -m src.video_finder.cli /videos --threshold 90 --recursive
```

**Filter Out Watched Videos:**

```bash
python -m src.video_finder.cli /videos --watched-db /watched/watched.db
```

**Create a Watched DB:**

```bash
python -m src.video_finder.cli --create-watched-db-from /watched --watched-db /watched/watched.db
```

---

## Notes

- Supported video formats: `.mp4`, `.mkv`, `.avi`, etc.
- The tool will prompt before moving duplicates.
- The watched DB stores perceptual hashes and metadata for robust filtering.

---

## License

MIT License

---

## Contributing

Pull requests and issues are welcome!
