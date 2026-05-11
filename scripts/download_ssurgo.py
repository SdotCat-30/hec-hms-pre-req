"""
Download SSURGO Hydrologic Soil Group (HSG) data for the watershed.

Run this from any machine with open internet access (the sandbox used to
build this repo blocks sdmdataaccess.nrcs.usda.gov). Writes:

    inputs/ssurgo_hsg.gpkg   — polygons with mukey, musym, hydgrpdcd
    inputs/ssurgo_hsg.tif    — rasterised HSG (1=A, 2=B, 3=C, 4=D, 5=A/D, 6=B/D, 7=C/D)

Requirements:
    pip install pygeohydro geopandas rioxarray rasterio shapely
"""

from __future__ import annotations

import io
import textwrap
import zipfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import requests
from rasterio.features import rasterize
from rasterio.transform import from_bounds

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "inputs"
OUT_DIR.mkdir(exist_ok=True)
WATERSHED = REPO / "watershed.gpkg"

HSG_TO_CODE = {"A": 1, "B": 2, "C": 3, "D": 4, "A/D": 5, "B/D": 6, "C/D": 7}

SDA_REST = "https://sdmdataaccess.nrcs.usda.gov/Tabular/SDMTabularService/post.rest"


def main() -> None:
    gdf = gpd.read_file(WATERSHED)
    assert gdf.crs.to_epsg() == 4326
    west, south, east, north = gdf.total_bounds
    print(f"Watershed bbox: W={west:.5f} S={south:.5f} E={east:.5f} N={north:.5f}")

    # 1) Fetch SSURGO polygons within the watershed bbox (WKT-based spatial query)
    wkt = (
        f"polygon(({west} {south}, {east} {south}, "
        f"{east} {north}, {west} {north}, {west} {south}))"
    )
    sql_spatial = textwrap.dedent(f"""\
        SELECT mu.mukey, mu.musym, mu.muname,
               muag.hydgrpdcd,
               mp.mupolygongeo.STAsText() AS wkt
        FROM mapunit mu
        JOIN muaggatt muag ON mu.mukey = muag.mukey
        JOIN mupolygon mp ON mu.mukey = mp.mukey
        WHERE mu.mukey IN (
            SELECT mukey FROM SDA_Get_Mukey_from_intersection_with_WktWgs84('{wkt}')
        )
    """)

    print("Querying USDA Soil Data Access (SDA REST) …")
    resp = requests.post(
        SDA_REST,
        data={"query": sql_spatial, "format": "JSON+COLUMNNAME"},
        timeout=300,
    )
    resp.raise_for_status()
    rows = resp.json()["Table"]
    cols, *records = rows
    df = pd.DataFrame(records, columns=cols)
    print(f"  Got {len(df)} polygons across {df['mukey'].nunique()} map units.")

    # 2) Build GeoDataFrame from WKT
    from shapely import wkt as shapely_wkt
    df["geometry"] = df["wkt"].apply(shapely_wkt.loads)
    gdf_soil = gpd.GeoDataFrame(df.drop(columns=["wkt"]), geometry="geometry", crs="EPSG:4326")
    gdf_soil["hsg_code"] = gdf_soil["hydgrpdcd"].map(HSG_TO_CODE).fillna(0).astype("int16")

    gpkg_path = OUT_DIR / "ssurgo_hsg.gpkg"
    gdf_soil.to_file(gpkg_path, driver="GPKG")
    print(f"  Wrote {gpkg_path}")

    # 3) Rasterize to 30 m grid in EPSG:4326
    res_deg = 30 / 111_320  # ~30 m at this latitude
    width = int(np.ceil((east - west) / res_deg))
    height = int(np.ceil((north - south) / res_deg))
    transform = from_bounds(west, south, east, north, width, height)

    shapes = ((geom, code) for geom, code in zip(gdf_soil.geometry, gdf_soil["hsg_code"]))
    raster = rasterize(
        shapes=shapes, out_shape=(height, width), transform=transform,
        fill=0, dtype="uint8",
    )
    tif_path = OUT_DIR / "ssurgo_hsg.tif"
    with rasterio.open(
        tif_path, "w", driver="GTiff", height=height, width=width, count=1,
        dtype="uint8", crs="EPSG:4326", transform=transform, compress="deflate",
        nodata=0,
    ) as dst:
        dst.write(raster, 1)
    print(f"  Wrote {tif_path}  ({tif_path.stat().st_size / 1e6:.1f} MB)")

    # 4) Write legend
    legend_path = OUT_DIR / "ssurgo_hsg_legend.csv"
    legend_path.write_text(
        "code,hsg\n0,nodata\n" + "\n".join(f"{v},{k}" for k, v in HSG_TO_CODE.items())
    )
    print(f"  Wrote {legend_path}")


if __name__ == "__main__":
    main()
