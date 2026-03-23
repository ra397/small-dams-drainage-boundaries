import pyarrow.parquet as pq
import shapely
from shapely import unary_union, to_geojson
import json
import csv
import os
from multiprocessing import Pool
from functools import partial

CATCHMENTS_DIR = "catchments"
OUTPUT_DIR = "outputs"
csv.field_size_limit(10 * 1024 * 1024)  # 10 MB

def load_catchment(region: str) -> tuple[list[int], list[bytes]]:
    path = os.path.join(CATCHMENTS_DIR, region)
    table = pq.read_table(path, columns=["flowpath_id", "geom"])
    ids = table.column("flowpath_id").to_pylist()
    geom_wkb = table.column("geom").to_pylist()
    return ids, geom_wkb

def process_dam(row: dict, catchment_data: tuple[list[int], list[bytes]]):
    dam_id = row["dam_id"]
    target_ids = set(int(x) for x in row["flowpath_ids"].split("|"))

    ids, geom_wkb = catchment_data
    matched_wkb = [geom_wkb[i] for i, fid in enumerate(ids) if fid in target_ids]

    if not matched_wkb:
        print(f"WARNING: {dam_id} - no matching geometries found")
        return

    geometries = shapely.from_wkb(matched_wkb)
    geometries = shapely.make_valid(geometries)
    merged = unary_union(geometries)

    geojson = {
        "type": "FeatureCollection",
        "crs": {
            "type": "name",
            "properties": {"name": "urn:ogc:def:crs:EPSG::5070"}
        },
        "features": [
            {"type": "Feature", "geometry": json.loads(to_geojson(merged))}
        ],
    }

    out_path = os.path.join(OUTPUT_DIR, f"{dam_id}.geojson")
    with open(out_path, "w") as f:
        json.dump(geojson, f)

def process_batch(args):
    rows, region = args
    catchment_data = load_catchment(region)
    for row in rows:
        process_dam(row, catchment_data)
    return len(rows)

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Group rows by region so each worker loads a catchment file once
    by_region: dict[str, list[dict]] = {}
    with open("dam_flowpath_descendants.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            region = row["region"]
            by_region.setdefault(region, []).append(row)

    tasks = [(rows, region) for region, rows in by_region.items()]
    print(f"Processing {sum(len(r) for r in by_region.values())} dams across {len(by_region)} regions")

    with Pool() as pool:
        for count in pool.imap_unordered(process_batch, tasks):
            print(f"Finished batch of {count} dams")

    print("Done")

if __name__ == "__main__":
    main()