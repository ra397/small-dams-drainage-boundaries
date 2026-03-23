from shapely.geometry import Point
from shapely import STRtree, from_wkb
import pyarrow.parquet as pq

# Load parquet
table = pq.read_table("catchments/illinois.parquet")
geometries = [from_wkb(wkb) for wkb in table.column("geom").to_pylist()]
attributes = table.drop(["geom"]).to_pydict()

# Build spatial index
tree = STRtree(geometries)

# Query
point = Point(365723.04440697434, 2078767.7338320352)
idx = tree.query(point, predicate="within")

print(int(attributes["flowpath_id"][idx[0]]))