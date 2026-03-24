"""
Convert a directory of GeoJSON files to Geobuf (.pbf) format.

Geobuf is a compact binary encoding for GeoJSON using Protocol Buffers.
It provides lossless compression with typically 6-8x size reduction.

Usage:
    python geojson_to_geobuf.py <input_dir> [output_dir]

If output_dir is omitted, files are written to <input_dir>/pbf/

Requirements:
    pip install geobuf
"""

import json
import sys
from pathlib import Path

import geobuf


def main():
    if len(sys.argv) < 2:
        print(__doc__.strip())
        sys.exit(1)

    input_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else input_dir / "pbf"

    if not input_dir.is_dir():
        print(f"Error: {input_dir} is not a directory")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(input_dir.glob("*.geojson"))
    if not files:
        files = sorted(input_dir.glob("*.json"))
    if not files:
        print(f"No .geojson or .json files found in {input_dir}")
        sys.exit(1)

    print(f"Converting {len(files)} file(s) to Geobuf")

    for filepath in files:
        with open(filepath, "r") as f:
            data = json.load(f)

        pbf = geobuf.encode(data)

        out_path = output_dir / (filepath.stem + ".pbf")
        with open(out_path, "wb") as f:
            f.write(pbf)

        orig_kb = filepath.stat().st_size / 1024
        pbf_kb = len(pbf) / 1024
        ratio = orig_kb / pbf_kb if pbf_kb > 0 else 0

        print(f"  {filepath.name} ({orig_kb:.1f} KB) -> {out_path.name} ({pbf_kb:.1f} KB) [{ratio:.1f}x]")

    print("Done.")


if __name__ == "__main__":
    main()