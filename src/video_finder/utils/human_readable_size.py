def human_readable_size(size_bytes):
    """Convert a file size in bytes to a human-readable string."""
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = 0
    p = 1024
    try:
        size_bytes_num = float(size_bytes)
    except ValueError:
        return "Invalid size"

    while size_bytes_num >= p and i < len(size_name) - 1:
        size_bytes_num /= p
        i += 1
    return f"{size_bytes_num:.2f} {size_name[i]}"
