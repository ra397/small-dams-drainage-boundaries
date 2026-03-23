from shapely.geometry import Point, box
from shapely import STRtree, from_wkb
import pyarrow.parquet as pq
import csv
import multiprocessing as mp
import os

CATCHMENTS_DIR = "catchments/"
CSV_PATH = "small_dams_5070.csv"
OUTPUT_PATH = "dam_flowpaths.csv"

def process_state(args):
    parquet_path, dams = args

    # Load this state's catchments
    table = pq.read_table(parquet_path)
    geometries = [from_wkb(wkb) for wkb in table.column("geom").to_pylist()]
    flowpath_ids = table.column("flowpath_id").to_pylist()

    tree = STRtree(geometries)

    results = []
    for dam in dams:
        point = Point(dam["x"], dam["y"])
        idx = tree.query(point, predicate="within")
        if len(idx) > 0:
            results.append((dam["dam_id"], int(flowpath_ids[idx[0]])))

    return results

if __name__ == "__main__":
    # Read all high-hazard dams
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
    print(f"Total dams: {len(dams)}")

    # Build work list: for each parquet, find dams within its bbox
    parquet_files = [
        os.path.join(CATCHMENTS_DIR, f)
        for f in os.listdir(CATCHMENTS_DIR) if f.endswith(".parquet")
    ]

    work = []
    for pf in parquet_files:
        table = pq.read_table(pf, columns=["geom"])
        bounds = [from_wkb(wkb).bounds for wkb in table.column("geom").to_pylist()]
        minx = min(b[0] for b in bounds)
        miny = min(b[1] for b in bounds)
        maxx = max(b[2] for b in bounds)
        maxy = max(b[3] for b in bounds)
        envelope = box(minx, miny, maxx, maxy)

        state_dams = [
            d for d in dams if envelope.contains(Point(d["x"], d["y"]))
        ]
        if state_dams:
            work.append((pf, state_dams))
            print(f"{os.path.basename(pf)}: {len(state_dams)} dams")

    # Process each state in parallel
    with mp.Pool(processes=mp.cpu_count()) as pool:
        all_results = pool.map(process_state, work)

    # Flatten and write
    results = [r for batch in all_results for r in batch]

    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["dam_id", "flowpath_id"])
        writer.writeheader()
        for dam_id, flowpath_id in results:
            writer.writerow({"dam_id": dam_id, "flowpath_id": flowpath_id})

    print(f"Matched {len(results)} / {len(dams)} dams")