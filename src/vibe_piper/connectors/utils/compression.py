"""
Compression utilities for file connectors.

This module provides compression and decompression utilities supporting
gzip, zip, and snappy formats.
"""

import gzip
import shutil
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

# Try to import snappy - it's optional
try:
    import snappy

    HAS_SNAPPY = True
except ImportError:
    HAS_SNAPPY = False


# =============================================================================
# Compression Constants
# =============================================================================#

SUPPORTED_COMPRESSION: tuple[str, ...] = ("gzip", "zip", "snappy", "gz")

# =============================================================================
# Compression Detection
# =============================================================================#


def detect_compression(path: str | Path) -> str | None:
    """
    Detect compression type from file extension.

    Args:
        path: Path to the file.

    Returns:
        Compression type ('gzip', 'zip', 'snappy') or None if uncompressed.

    Examples:
        >>> detect_compression("data.csv.gz")
        'gzip'
        >>> detect_compression("data.csv.zip")
        'zip'
        >>> detect_compression("data.csv")
        None
    """
    path_str = str(path).lower()

    for compression in SUPPORTED_COMPRESSION:
        if path_str.endswith(f".{compression}"):
            # Normalize 'gz' to 'gzip'
            if compression == "gz":
                return "gzip"
            return compression

    return None


def get_compression_extension(compression: str | None) -> str:
    """
    Get the file extension for a compression type.

    Args:
        compression: Compression type.

    Returns:
        File extension including the dot (e.g., '.gz').

    Raises:
        ValueError: If compression type is not supported.

    Examples:
        >>> get_compression_extension("gzip")
        '.gz'
        >>> get_compression_extension(None)
        ''
    """
    if compression is None:
        return ""

    mapping: dict[str, str] = {
        "gzip": ".gz",
        "gz": ".gz",
        "zip": ".zip",
        "snappy": ".snappy",
    }

    if compression not in mapping:
        msg = f"Unsupported compression type: {compression!r}. Supported: {SUPPORTED_COMPRESSION}"
        raise ValueError(msg)

    return mapping[compression]


# =============================================================================
# Compression Functions
# =============================================================================#


def compress_data(
    data: bytes | str | bytearray,
    compression: str,
) -> bytes:
    """
    Compress data using the specified compression algorithm.

    Args:
        data: Data to compress (bytes or string).
        compression: Compression type ('gzip', 'snappy').

    Returns:
        Compressed data as bytes.

    Raises:
        ValueError: If compression type is not supported or required library missing.

    Examples:
        >>> compress_data(b"hello world", "gzip")
        b'\\x1f\\x8b\\x08\\x00...'
    """
    if isinstance(data, str):
        data = data.encode("utf-8")

    if compression in ("gzip", "gz"):
        return gzip.compress(data)

    if compression == "snappy":
        if not HAS_SNAPPY:
            msg = "Snappy compression requires 'python-snappy'. Install: pip install python-snappy"
            raise ValueError(msg)
        return snappy.compress(data)

    msg = (
        f"Unsupported compression type: {compression!r}. "
        "Supported for in-memory compression: gzip, snappy"
    )
    raise ValueError(msg)


def decompress_file(
    input_path: str | Path,
    output_path: str | Path | None = None,
    compression: str | None = None,
) -> Path:
    """
    Decompress a file.

    Args:
        input_path: Path to the compressed file.
        output_path: Path for the decompressed file. If None, removes
                    the compression extension from input_path.
        compression: Compression type. If None, auto-detect from file extension.

    Returns:
        Path to the decompressed file.

    Raises:
        ValueError: If compression type is not supported.
        FileNotFoundError: If input file doesn't exist.

    Examples:
        >>> decompress_file("data.csv.gz")
        Path('data.csv')
        >>> decompress_file("archive.zip", "extracted/")
        Path('extracted/')
    """
    input_path = Path(input_path)

    if not input_path.exists():
        msg = f"Input file not found: {input_path}"
        raise FileNotFoundError(msg)

    # Auto-detect compression if not specified
    if compression is None:
        compression = detect_compression(input_path)

    if compression is None:
        # No compression, just return the input path
        return input_path

    # Determine output path
    if output_path is None:
        # Remove compression extension
        output_path = input_path.with_suffix("")
    else:
        output_path = Path(output_path)

    # Decompress based on type
    if compression in ("gzip", "gz"):
        with gzip.open(input_path, "rb") as f_in:
            with open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        return output_path

    if compression == "zip":
        with zipfile.ZipFile(input_path, "r") as zip_ref:
            # Extract to output path (or directory if it's a directory)
            if output_path.suffix:
                # Single file extraction - extract the first file
                names = zip_ref.namelist()
                if names:
                    # Extract to parent directory with target filename
                    zip_ref.extract(names[0], output_path.parent)
                    extracted = output_path.parent / names[0]
                    if extracted != output_path:
                        extracted.rename(output_path)
            else:
                # Extract entire archive
                zip_ref.extractall(output_path)
        return output_path

    if compression == "snappy":
        if not HAS_SNAPPY:
            msg = "Snappy decompression requires the 'python-snappy' package. Install with: pip install python-snappy"
            raise ValueError(msg)
        with open(input_path, "rb") as f_in:
            compressed = f_in.read()
        decompressed = snappy.decompress(compressed)
        with open(output_path, "wb") as f_out:
            f_out.write(decompressed)
        return output_path

    msg = f"Unsupported compression type: {compression!r}"
    raise ValueError(msg)


def compress_file(
    input_path: str | Path,
    output_path: str | Path | None = None,
    compression: str = "gzip",
) -> Path:
    """
    Compress a file.

    Args:
        input_path: Path to the file to compress.
        output_path: Path for the compressed file. If None, adds the
                    compression extension to input_path.
        compression: Compression type ('gzip', 'snappy').

    Returns:
        Path to the compressed file.

    Raises:
        ValueError: If compression type is not supported.
        FileNotFoundError: If input file doesn't exist.

    Examples:
        >>> compress_file("data.csv", compression="gzip")
        Path('data.csv.gz')
    """
    input_path = Path(input_path)

    if not input_path.exists():
        msg = f"Input file not found: {input_path}"
        raise FileNotFoundError(msg)

    # Determine output path
    if output_path is None:
        ext = get_compression_extension(compression)
        output_path = input_path.with_suffix(input_path.suffix + ext)
    else:
        output_path = Path(output_path)

    # Compress based on type
    if compression in ("gzip", "gz"):
        with open(input_path, "rb") as f_in:
            with gzip.open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        return output_path

    if compression == "snappy":
        if not HAS_SNAPPY:
            msg = "Snappy compression requires the 'python-snappy' package. Install with: pip install python-snappy"
            raise ValueError(msg)
        with open(input_path, "rb") as f_in:
            data = f_in.read()
        compressed = snappy.compress(data)
        with open(output_path, "wb") as f_out:
            f_out.write(compressed)
        return output_path

    msg = f"Unsupported compression type: {compression!r}"
    raise ValueError(msg)


# =============================================================================
# Streaming Compression
# =============================================================================#


class CompressedFileReader:
    """
    Context manager for reading compressed files.

    Automatically handles decompression based on file extension or
    specified compression type.

    Example:
        >>> with CompressedFileReader("data.csv.gz") as f:
        ...     data = f.read()
    """

    def __init__(
        self, path: str | Path, compression: str | None = None, mode: str = "rt"
    ) -> None:
        """
        Initialize the compressed file reader.

        Args:
            path: Path to the compressed file.
            compression: Compression type. If None, auto-detect from extension.
            mode: File mode ('rt' for text, 'rb' for binary).
        """
        self.path = Path(path)
        self.compression = compression or detect_compression(self.path)
        self.mode = mode
        self.file: Any = None

    def __enter__(self) -> Any:
        """Open and return the file handle."""
        if self.compression in ("gzip", "gz"):
            self.file = gzip.open(self.path, self.mode)
        elif self.compression == "zip":
            # For zip, we need to extract the first file
            with zipfile.ZipFile(self.path, "r") as zip_ref:
                names = zip_ref.namelist()
                if not names:
                    msg = f"Zip file is empty: {self.path}"
                    raise ValueError(msg)
                # Open the first file
                self.file = zip_ref.open(names[0], "r")
        elif self.compression is None:
            # No compression
            self.file = open(self.path, self.mode)
        else:
            msg = f"Unsupported compression type: {self.compression!r}"
            raise ValueError(msg)

        return self.file

    def __exit__(self, *args: Any) -> None:
        """Close the file handle."""
        if self.file:
            self.file.close()


class CompressedFileWriter:
    """
    Context manager for writing compressed files.

    Example:
        >>> with CompressedFileWriter("output.csv.gz", "gzip") as f:
        ...     f.write("data")
    """

    def __init__(
        self, path: str | Path, compression: str = "gzip", mode: str = "wt"
    ) -> None:
        """
        Initialize the compressed file writer.

        Args:
            path: Path to the output file.
            compression: Compression type ('gzip', 'snappy').
            mode: File mode ('wt' for text, 'wb' for binary).
        """
        self.path = Path(path)
        self.compression = compression
        self.mode = mode
        self.file: Any = None

    def __enter__(self) -> Any:
        """Open and return the file handle."""
        if self.compression in ("gzip", "gz"):
            self.file = gzip.open(self.path, self.mode)
        elif self.compression is None:
            # No compression
            self.file = open(self.path, self.mode)
        else:
            msg = f"Unsupported compression type for writing: {self.compression!r}"
            raise ValueError(msg)

        return self.file

    def __exit__(self, *args: Any) -> None:
        """Close the file handle."""
        if self.file:
            self.file.close()


def iter_compressed_lines(
    path: str | Path,
    compression: str | None = None,
    encoding: str = "utf-8",
) -> Iterator[str]:
    """
    Iterate over lines in a compressed file.

    Useful for processing large compressed files without loading
    the entire file into memory.

    Args:
        path: Path to the compressed file.
        compression: Compression type. If None, auto-detect.
        encoding: Text encoding for the file.

    Yields:
        Lines from the file (with newlines preserved).

    Examples:
        >>> for line in iter_compressed_lines("data.csv.gz"):
        ...     print(line.strip())
    """
    with CompressedFileReader(path, compression, mode="rt") as f:
        for line in f:
            yield line
