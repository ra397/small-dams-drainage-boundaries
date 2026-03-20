import pandas as pd

df = pd.read_parquet(
    "network/illinois.parquet",
    columns=["flowpath_id", "flowpath_toid"],
).astype({"flowpath_id": "int32", "flowpath_toid": "int32"})

def get_all_descendants(dataset: pd.DataFrame, start_id: int) -> frozenset[int]:
    parents = dataset["flowpath_toid"].values
    children = dataset["flowpath_id"].values

    children_lookup: dict[int, set[int]] = {}
    for p, c in zip(parents, children):
        if p in children_lookup:
            children_lookup[p].add(c)
        else:
            children_lookup[p] = {c}

    visited: set[int] = set()
    pending: set[int] = {start_id}

    while pending:
        visited |= pending
        # expand all pending nodes at once
        next_pending: set[int] = set()
        for node in pending:
            next_pending.update(children_lookup.get(node, ()))
        pending = next_pending - visited

    return frozenset(visited)

print(get_all_descendants(df, 11915629))
print(get_all_descendants(df, 11915723))
print(get_all_descendants(df, 17540805))