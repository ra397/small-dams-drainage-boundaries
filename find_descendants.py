from collections import deque
import pyarrow.parquet as pq
import csv
import os
import multiprocessing as mp

NETWORK_DIR = "network/"
DAM_FLOWPATHS_CSV = "dam_flowpaths.csv"
OUTPUT_CSV = "dam_flowpath_descendants.csv"

def build_children_lookup(table):
    lookup = {}
    parents = table.column("flowpath_toid").to_pylist()
    children = table.column("flowpath_id").to_pylist()
    for p, c in zip(parents, children):
        p, c = int(p), int(c)
        if p in lookup:
            lookup[p].append(c)
        else:
            lookup[p] = [c]
    return lookup

def get_all_descendants(lookup, start_id):
    visited = set()
    queue = deque([start_id])
    while queue:
        node = queue.popleft()
        if node in visited:
            continue
        visited.add(node)
        children = lookup.get(node)
        if children:
            queue.extend(children)
    return visited

def process_state(args):
    parquet_path, flowpath_ids = args

    table = pq.read_table(parquet_path, columns=["flowpath_id", "flowpath_toid"])
    lookup = build_children_lookup(table)

    # Only process dams whose flowpath_id exists in this state's network
    all_ids = set(table.column("flowpath_id").to_pylist()) | set(table.column("flowpath_toid").to_pylist())

    results = []
    for dam_id, fp_id in flowpath_ids:
        if fp_id in all_ids:
            descendants = get_all_descendants(lookup, fp_id)
            results.append((dam_id, sorted(int(i) for i in descendants), os.path.basename(parquet_path)))

    return results

if __name__ == "__main__":
    # Read dam_flowpaths.csv
    dams = []
    with open(DAM_FLOWPATHS_CSV, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dams.append((row["dam_id"], int(row["flowpath_id"])))
    print(f"Total dams: {len(dams)}")

    # Build work list per state
    parquet_files = [
        os.path.join(NETWORK_DIR, f)
        for f in os.listdir(NETWORK_DIR) if f.endswith(".parquet")
    ]

    work = []
    for pf in parquet_files:
        table = pq.read_table(pf, columns=["flowpath_id", "flowpath_toid"])
        all_ids = set(table.column("flowpath_id").to_pylist()) | set(table.column("flowpath_toid").to_pylist())

        state_dams = [(dam_id, fp_id) for dam_id, fp_id in dams if fp_id in all_ids]
        if state_dams:
            work.append((pf, state_dams))
            print(f"{os.path.basename(pf)}: {len(state_dams)} dams")

    # Process in parallel
    with mp.Pool(processes=mp.cpu_count()) as pool:
        all_results = pool.map(process_state, work)

    results = [r for batch in all_results for r in batch]

    # Write output
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["dam_id", "flowpath_ids", "region"])
        writer.writeheader()
        for dam_id, fp_ids, region in results:
            writer.writerow({
                "dam_id": dam_id,
                "flowpath_ids": "|".join(str(i) for i in fp_ids),
                "region": region,
            })

    print(f"Processed {len(results)} / {len(dams)} dams")