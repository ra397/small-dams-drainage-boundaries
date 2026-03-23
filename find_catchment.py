from shapely.geometry import Point
from shapely import STRtree, from_wkb
import pyarrow.parquet as pq
import csv
import multiprocessing as mp

# Shared data paths
PARQUET_PATH = "catchments/illinois.parquet"
CSV_PATH = "small_dams_5070.csv"
OUTPUT_PATH = "dam_flowpaths.csv"

def init_worker(geom_wkb, flowpath_ids):
    """Each worker builds its own STRtree from shared WKB bytes."""
    global _tree, _flowpath_ids, _geometries
    _geometries = [from_wkb(wkb) for wkb in geom_wkb]
    _tree = STRtree(_geometries)
    _flowpath_ids = flowpath_ids

def query_dam(dam):
    """Query a single dam point against the tree."""
    point = Point(dam["x"], dam["y"])
    idx = _tree.query(point, predicate="within")
    if len(idx) > 0:
        return (dam["dam_id"], int(_flowpath_ids[idx[0]]))
    return None

if __name__ == "__main__":
    # Load parquet
    table = pq.read_table(PARQUET_PATH)
    geom_wkb = table.column("geom").to_pylist()
    flowpath_ids = table.column("flowpath_id").to_pylist()

    # Read high-hazard dams
    dams = []
    with open(CSV_PATH, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["Hazard Potential Classification"] == "High":
                dams.append({
                    "dam_id": row["Federal ID"],
                    "x": float(row["x_5070"]),
                    "y": float(row["y_5070"])
                })

    # Process in parallel
    with mp.Pool(
        processes=mp.cpu_count(),
        initializer=init_worker,
        initargs=(geom_wkb, flowpath_ids)
    ) as pool:
        results = pool.map(query_dam, dams, chunksize=100)

    # Filter out misses and write
    results = [r for r in results if r is not None]

    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["dam_id", "flowpath_id"])
        writer.writeheader()
        for dam_id, flowpath_id in results:
            writer.writerow({"dam_id": dam_id, "flowpath_id": flowpath_id})

    print(f"Matched {len(results)} dams")