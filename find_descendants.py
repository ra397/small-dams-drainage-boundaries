import pandas as pd
from collections import deque

df = pd.read_parquet(
    "network/illinois.parquet",
    columns=["flowpath_id", "flowpath_toid"],
).astype({"flowpath_id": "int32", "flowpath_toid": "int32"})

def build_children_lookup(network: pd.DataFrame) -> dict[int, list[int]]:
    lookup: dict[int, list[int]] = {}
    for p, c in zip(network["flowpath_toid"].values, network["flowpath_id"].values):
        if p in lookup:
            lookup[p].append(c)
        else:
            lookup[p] = [c]
    return lookup

def get_all_descendants(lookup: dict[int, list[int]], start_id: int) -> frozenset[int]:
    visited: set[int] = set()
    queue = deque([start_id])
    while queue:
        node = queue.popleft()
        if node in visited:
            continue
        visited.add(node)
        children = lookup.get(node)
        if children:
            queue.extend(children)
    return frozenset(visited)

tree = build_children_lookup(df)

output = [int(i) for i in list(get_all_descendants(tree, 17540805))]
print(output)