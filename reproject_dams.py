import csv
from pyproj import Transformer

INPUT_CSV = "small_dams.csv"
OUTPUT_CSV = "small_dams_5070.csv"

transformer = Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=False)

with open(INPUT_CSV, newline="", encoding="utf-8-sig") as infile, open(OUTPUT_CSV, "w", newline="") as outfile:
    reader = csv.DictReader(infile)
    writer = csv.DictWriter(outfile, fieldnames=["Federal ID", "Hazard Potential Classification", "x_5070", "y_5070"])
    writer.writeheader()

    for row in reader:
        lat_raw = row["Latitude"].strip()
        lng_raw = row["Longitude"].strip()
        if not lat_raw or not lng_raw:
            continue
        lat = float(lat_raw)
        lng = float(lng_raw)
        # pyproj with always_xy=False: input order is (lat, lng)
        x, y = transformer.transform(lat, lng)
        writer.writerow({
            "Federal ID": row["Federal ID"],
            "Hazard Potential Classification": row["Hazard Potential Classification"],
            "x_5070": x,
            "y_5070": y,
        })