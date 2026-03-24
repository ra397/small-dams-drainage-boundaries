"""
Reproject a directory of GeoJSON files from EPSG:5070 to EPSG:4326.
Coordinates in the output are rounded to 4 decimal places.

Usage:
    python reproject_geojson.py <input_dir> [output_dir]

If output_dir is omitted, files are written to <input_dir>/reprojected/
"""

import json
import sys
from pathlib import Path
from pyproj import Transformer

transformer = Transformer.from_crs("EPSG:5070", "EPSG:4326", always_xy=True)


def reproject_coords(coords, depth=0):
    """Recursively reproject coordinate arrays to any nesting depth."""
    if not coords:
        return coords
    # Base case: a single coordinate pair [x, y] or [x, y, z]
    if isinstance(coords[0], (int, float)):
        x, y = transformer.transform(coords[0], coords[1])
        result = [round(x, 4), round(y, 4)]
        if len(coords) > 2:
            result.append(coords[2])  # preserve Z if present
        return result
    # Recursive case: list of coordinate arrays
    return [reproject_coords(c, depth + 1) for c in coords]


def reproject_geometry(geometry):
    """Reproject a GeoJSON geometry object in place."""
    if geometry is None:
        return None
    geom = dict(geometry)
    geom_type = geom.get("type")

    if geom_type == "GeometryCollection":
        geom["geometries"] = [reproject_geometry(g) for g in geom["geometries"]]
    elif "coordinates" in geom:
        geom["coordinates"] = reproject_coords(geom["coordinates"])
    return geom


def reproject_geojson(data):
    """Reproject an entire GeoJSON object (Feature, FeatureCollection, or bare Geometry)."""
    data = dict(data)
    obj_type = data.get("type")

    if obj_type == "FeatureCollection":
        data["features"] = [reproject_geojson(f) for f in data["features"]]
    elif obj_type == "Feature":
        data["geometry"] = reproject_geometry(data.get("geometry"))
    else:
        # Bare geometry
        data = reproject_geometry(data)

    # Update or remove CRS field if present
    data.pop("crs", None)
    return data


def main():
    if len(sys.argv) < 2:
        print(__doc__.strip())
        sys.exit(1)

    input_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else input_dir / "reprojected"

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

    print(f"Reprojecting {len(files)} file(s): EPSG:5070 -> EPSG:4326")

    for filepath in files:
        with open(filepath, "r") as f:
            data = json.load(f)

        reprojected = reproject_geojson(data)

        out_path = output_dir / filepath.name
        with open(out_path, "w") as f:
            json.dump(reprojected, f)

        print(f"  {filepath.name} -> {out_path}")

    print("Done.")


if __name__ == "__main__":
    main()