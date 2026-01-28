#!/usr/bin/env python3
"""
Google Photos Takeout Metadata Processor

Processes media files from Google Photos Takeout export, extracting metadata
from JSON sidecar files and applying it to photos/videos using exiftool.
"""

import argparse
import json
import os
import random
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# Media file extensions to process
MEDIA_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif", ".bmp", ".tiff", ".gif", ".avif", ".jxl", ".jfif", ".raw", ".cr2", ".nef", ".orf", ".sr2", ".arw", ".dng", ".pef", ".raf", ".rw2", ".srw", ".3fr", ".erf", ".k25", ".kdc", ".mef", ".mos", ".mrw", ".nrw", ".srf", ".x3f", ".mp4", ".mov", ".mkv", ".avi", ".webm", ".3gp", ".m4v", ".mpg", ".mpeg", ".mts", ".m2ts", ".ts", ".flv", ".f4v", ".wmv", ".asf", ".rm", ".rmvb", ".vob", ".ogv", ".mxf", ".dv", ".divx", ".xvid"}

# JSON filename patterns (Google Takeout inconsistencies)
JSON_PATTERNS = [
    ".supplemental-metadata.json",
    ".supplemental-metada.json",
    ".supplemental-me.json",
]

# Statistics counters
stats = {
    "total_files": 0,
    "processed": 0,
    "with_json": 0,
    "without_json": 0,
    "metadata_success": 0,
    "metadata_failed": 0,
    "gps_applied": 0,
    "errors": [],
}


def find_exiftool() -> Optional[str]:
    """Find exiftool in system PATH or common locations."""
    # Try to find in PATH
    try:
        result = subprocess.run(
            ["which", "exiftool"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except:
        pass

    # Check common locations
    common_paths = [
        "/opt/homebrew/bin/exiftool",
        "/usr/local/bin/exiftool",
        "/usr/bin/exiftool",
    ]

    for path in common_paths:
        if Path(path).exists():
            return path

    return None


def get_directory_size(path: Path) -> int:
    """Calculate total size of directory in bytes."""
    total = 0
    try:
        for entry in path.rglob("*"):
            if entry.is_file():
                total += entry.stat().st_size
    except:
        pass
    return total


def check_disk_space(source_dir: Path, safety_margin: float = 1.2) -> bool:
    """Verify sufficient disk space is available (source size * safety_margin)."""
    source_size = get_directory_size(source_dir)
    required_bytes = source_size * safety_margin
    required_gb = required_bytes / (1024 ** 3)

    stat = os.statvfs(".")
    available_gb = (stat.f_bavail * stat.f_frsize) / (1024 ** 3)

    if available_gb < required_gb:
        print(f"ERROR: Insufficient disk space.")
        print(f"  Source size: {source_size / (1024 ** 3):.1f} GB")
        print(f"  Required (with {int((safety_margin - 1) * 100)}% margin): {required_gb:.1f} GB")
        print(f"  Available: {available_gb:.1f} GB")
        return False
    return True


def find_json_for_media(media_file: Path) -> Optional[Path]:
    """
    Find JSON metadata file for media file.
    Checks all three possible naming patterns due to Google Takeout inconsistencies.
    """
    for pattern in JSON_PATTERNS:
        json_file = media_file.parent / (media_file.name + pattern)
        if json_file.exists():
            return json_file
    return None


def timestamp_to_exif_format(timestamp: str) -> str:
    """Convert Unix timestamp to EXIF date format (YYYY:MM:DD HH:MM:SS)."""
    dt = datetime.fromtimestamp(int(timestamp))
    return dt.strftime("%Y:%m:%d %H:%M:%S")


def extract_metadata(json_file: Path) -> Dict:
    """Extract relevant metadata from JSON file."""
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        metadata = {}

        # Extract timestamp
        if "photoTakenTime" in data and "timestamp" in data["photoTakenTime"]:
            timestamp = data["photoTakenTime"]["timestamp"]
            metadata["datetime"] = timestamp_to_exif_format(timestamp)
            metadata["timestamp"] = int(timestamp)

        # Extract GPS data (prefer geoDataExif over geoData)
        gps_source = data.get("geoDataExif") or data.get("geoData")
        if gps_source:
            lat = gps_source.get("latitude", 0.0)
            lon = gps_source.get("longitude", 0.0)
            alt = gps_source.get("altitude", 0.0)

            # Only use GPS if coordinates are non-zero
            if lat != 0.0 and lon != 0.0:
                metadata["latitude"] = lat
                metadata["longitude"] = lon
                if alt != 0.0:
                    metadata["altitude"] = alt

        # Extract description
        if "description" in data and data["description"]:
            metadata["description"] = data["description"]

        return metadata

    except Exception as e:
        stats["errors"].append(f"Failed to parse {json_file}: {e}")
        return {}


def build_exiftool_command(exiftool_path: str, media_file: Path, metadata: Dict, is_video: bool) -> List[str]:
    """Build exiftool command with appropriate tags."""
    cmd = [exiftool_path, "-overwrite_original"]

    # Apply datetime metadata
    if "datetime" in metadata:
        dt = metadata["datetime"]
        if is_video:
            cmd.extend([
                f"-CreateDate={dt}",
                f"-ModifyDate={dt}",
                f"-TrackCreateDate={dt}",
                f"-TrackModifyDate={dt}",
                f"-MediaCreateDate={dt}",
                f"-MediaModifyDate={dt}",
            ])
        else:
            cmd.extend([
                f"-DateTimeOriginal={dt}",
                f"-CreateDate={dt}",
                f"-ModifyDate={dt}",
            ])

    # Apply GPS metadata
    if "latitude" in metadata and "longitude" in metadata:
        lat = metadata["latitude"]
        lon = metadata["longitude"]

        lat_ref = "N" if lat >= 0 else "S"
        lon_ref = "E" if lon >= 0 else "W"

        cmd.extend([
            f"-GPSLatitude={abs(lat)}",
            f"-GPSLatitudeRef={lat_ref}",
            f"-GPSLongitude={abs(lon)}",
            f"-GPSLongitudeRef={lon_ref}",
        ])

        if "altitude" in metadata:
            alt = metadata["altitude"]
            alt_ref = "0" if alt >= 0 else "1"  # 0 = above sea level, 1 = below
            cmd.extend([
                f"-GPSAltitude={abs(alt)}",
                f"-GPSAltitudeRef={alt_ref}",
            ])

    # Apply description
    if "description" in metadata:
        cmd.append(f"-ImageDescription={metadata['description']}")

    cmd.append(str(media_file))
    return cmd


def apply_metadata(exiftool_path: str, media_file: Path, metadata: Dict, dry_run: bool, verbose: bool) -> bool:
    """Apply metadata to media file using exiftool."""
    is_video = media_file.suffix.lower() in {".mp4", ".mov"}
    cmd = build_exiftool_command(exiftool_path, media_file, metadata, is_video)

    if dry_run:
        if verbose:
            print(f"  [DRY RUN] Would run: {' '.join(cmd)}")
        return True

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            stats["errors"].append(f"exiftool failed for {media_file.name}: {result.stderr}")
            return False

        # Update file modification time to match photo taken time
        if "timestamp" in metadata:
            os.utime(media_file, (metadata["timestamp"], metadata["timestamp"]))

        return True

    except Exception as e:
        stats["errors"].append(f"Failed to apply metadata to {media_file.name}: {e}")
        return False


def discover_media_files(source_dir: Path) -> List[Path]:
    """Recursively discover all media files in source directory."""
    media_files = []

    for file_path in source_dir.rglob("*"):
        if file_path.is_file() and file_path.suffix.lower() in MEDIA_EXTENSIONS:
            # Skip metadata.json files (album-level, not file-level)
            if file_path.name == "metadata.json":
                continue
            media_files.append(file_path)

    return sorted(media_files)


def process_file(
    exiftool_path: str,
    source_file: Path,
    source_root: Path,
    output_root: Path,
    dry_run: bool,
    verbose: bool,
    skip_no_json: bool
) -> bool:
    """Process a single media file."""

    # Calculate relative path and output path
    rel_path = source_file.relative_to(source_root)
    output_file = output_root / rel_path

    # Find JSON metadata
    json_file = find_json_for_media(source_file)

    if json_file:
        stats["with_json"] += 1
        metadata = extract_metadata(json_file)

        if verbose:
            print(f"  Found JSON: {json_file.name}")
            if metadata:
                print(f"    Metadata: {list(metadata.keys())}")
    else:
        stats["without_json"] += 1
        metadata = {}

        if verbose:
            print(f"  No JSON metadata found")

        if skip_no_json:
            if verbose:
                print(f"  Skipping (--skip-no-json)")
            return True

    # Create output directory
    if not dry_run:
        output_file.parent.mkdir(parents=True, exist_ok=True)

    # Copy file
    if dry_run:
        if verbose:
            print(f"  [DRY RUN] Would copy to: {output_file}")
    else:
        try:
            shutil.copy2(source_file, output_file)
        except Exception as e:
            stats["errors"].append(f"Failed to copy {source_file.name}: {e}")
            return False

    # Apply metadata if available
    if metadata:
        success = apply_metadata(exiftool_path, output_file, metadata, dry_run, verbose)

        if success:
            stats["metadata_success"] += 1
            if "latitude" in metadata:
                stats["gps_applied"] += 1
        else:
            stats["metadata_failed"] += 1
            return False

    stats["processed"] += 1
    return True


def verify_output(source_root: Path, output_root: Path) -> Tuple[bool, List[str]]:
    """Verify all source files were processed."""
    if not output_root.exists():
        return False, ["Output directory does not exist"]

    # Get all source media files (relative paths)
    source_files = set()
    for file_path in discover_media_files(source_root):
        rel_path = file_path.relative_to(source_root)
        source_files.add(rel_path)

    # Get all output media files (relative paths)
    output_files = set()
    for file_path in discover_media_files(output_root):
        rel_path = file_path.relative_to(output_root)
        output_files.add(rel_path)

    # Find missing files
    missing = source_files - output_files

    if missing:
        missing_list = [f"  MISSING: {f}" for f in sorted(missing)]
        return False, missing_list

    return True, []


def verify_sample_metadata(exiftool_path: str, output_root: Path, sample_count: int = 5) -> List[str]:
    """Verify metadata on random sample of processed files."""
    output_files = discover_media_files(output_root)

    if not output_files:
        return ["No files found in output directory"]

    # Sample random files
    sample_size = min(sample_count, len(output_files))
    sample_files = random.sample(output_files, sample_size)

    results = []
    for file_path in sample_files:
        try:
            result = subprocess.run(
                [exiftool_path, "-DateTimeOriginal", "-CreateDate", str(file_path)],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0 and result.stdout.strip():
                results.append(f"  ✓ {file_path.name}: metadata verified")
            else:
                results.append(f"  ? {file_path.name}: no metadata found (may be expected)")

        except Exception as e:
            results.append(f"  ✗ {file_path.name}: verification failed ({e})")

    return results


def print_summary(dry_run: bool):
    """Print final summary report."""
    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE" if not dry_run else "DRY RUN COMPLETE")
    print("=" * 60)
    print(f"Total files found: {stats['total_files']}")
    print(f"Files processed: {stats['processed']}")
    print(f"Files with JSON metadata: {stats['with_json']}")
    print(f"Files without JSON: {stats['without_json']}")

    if not dry_run:
        print(f"Metadata application successful: {stats['metadata_success']}")
        print(f"Metadata application failed: {stats['metadata_failed']}")
        print(f"GPS coordinates applied: {stats['gps_applied']}")

    if stats["errors"]:
        print(f"\nErrors encountered: {len(stats['errors'])}")
        for error in stats["errors"][:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(stats["errors"]) > 10:
            print(f"  ... and {len(stats['errors']) - 10} more errors")

    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Process Google Photos Takeout metadata and apply to media files"
    )
    parser.add_argument(
        "source",
        nargs="?",
        default="Google Photos",
        help="Source directory containing media files (default: 'Google Photos')"
    )
    parser.add_argument(
        "-o", "--output",
        default="Output",
        help="Output directory for processed files (default: 'Output')"
    )
    parser.add_argument(
        "--exiftool",
        help="Path to exiftool binary (default: auto-detect)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed progress for each file"
    )
    parser.add_argument(
        "--skip-no-json",
        action="store_true",
        help="Don't copy files that lack JSON metadata"
    )
    parser.add_argument(
        "--skip-disk-check",
        action="store_true",
        help="Skip disk space verification"
    )

    args = parser.parse_args()

    # Convert to Path objects
    source_dir = Path(args.source)
    output_dir = Path(args.output)

    # Preliminary checks
    print("Google Photos Takeout Metadata Processor")
    print("=" * 60)

    # Find exiftool
    exiftool_path = args.exiftool or find_exiftool()
    if not exiftool_path:
        print("ERROR: exiftool not found")
        print("Please install exiftool:")
        print("  macOS: brew install exiftool")
        print("  Linux: apt install libimage-exiftool-perl")
        print("Or specify path with --exiftool")
        sys.exit(1)

    print(f"Using exiftool: {exiftool_path}")

    if not source_dir.exists():
        print(f"ERROR: Source directory not found: {source_dir}")
        sys.exit(1)

    if not args.dry_run and not args.skip_disk_check and not check_disk_space(source_dir):
        sys.exit(1)

    # Discover files
    print(f"\nScanning {source_dir} for media files...")
    media_files = discover_media_files(source_dir)
    stats["total_files"] = len(media_files)

    print(f"Found {stats['total_files']} media files")

    if args.dry_run:
        print("\n*** DRY RUN MODE - No changes will be made ***\n")

    # Process files
    print(f"\nProcessing files...")
    for i, source_file in enumerate(media_files, 1):
        progress = (i / stats["total_files"]) * 100

        if args.verbose:
            print(f"\n[{i}/{stats['total_files']} - {progress:.1f}%] {source_file.name}")
        elif i % 10 == 0 or i == stats["total_files"]:
            print(f"[{i}/{stats['total_files']} - {progress:.1f}%]", end="\r")

        process_file(
            exiftool_path,
            source_file,
            source_dir,
            output_dir,
            args.dry_run,
            args.verbose,
            args.skip_no_json
        )

    print(f"\n[{stats['total_files']}/{stats['total_files']} - 100.0%] Processing complete")

    # Verification (skip for dry-run)
    if not args.dry_run:
        print("\n" + "=" * 60)
        print("VERIFICATION")
        print("=" * 60)

        # Verify all files processed
        success, missing = verify_output(source_dir, output_dir)

        print(f"Source: {stats['total_files']} media files")
        print(f"Output: {stats['processed']} media files")

        if success:
            print("✓ All files accounted for")
        else:
            print(f"\n❌ ERROR: {len(missing)} files not processed!")
            for msg in missing[:20]:  # Show first 20
                print(msg)
            if len(missing) > 20:
                print(f"  ... and {len(missing) - 20} more files")

        # Sample metadata verification
        if stats["with_json"] > 0:
            print("\nSample metadata verification (5 random files):")
            sample_results = verify_sample_metadata(exiftool_path, output_dir)
            for result in sample_results:
                print(result)

    # Print summary
    print_summary(args.dry_run)

    # Exit with appropriate code
    if stats["errors"] and stats["processed"] < stats["total_files"]:
        print("\n❌ FAILED: Some files were not processed")
        sys.exit(1)
    elif stats["errors"]:
        print("\n⚠️  COMPLETED WITH ERRORS")
        print("All files copied but some metadata may be incomplete")
        sys.exit(0)
    else:
        if not args.dry_run:
            print(f"\n✅ SUCCESS: All {stats['total_files']} media files processed!")
            print(f"✅ Output directory: {output_dir.absolute()}")
        sys.exit(0)


if __name__ == "__main__":
    main()
