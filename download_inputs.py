"""
Download HEC-HMS prerequisite input files for a watershed.

Files downloaded (all clipped to the watershed bounding box):
  1. DEM         - USGS 3DEP 10 m  (AWS S3 COG → inputs/dem_10m.tif)
  2. Land cover  - ESA WorldCover 2021 10 m (AWS S3 COG → inputs/esa_worldcover_2021_10m.tif)
  3. Soil / HSG  - SSURGO via USDA SDM REST → inputs/ssurgo_hsg.gpkg
                   (falls back to instructions if network is restricted)

Notes
-----
- DEM CRS output: EPSG:5070 (Albers Equal Area CONUS, meters) — the native
  output of py3dep.get_dem for 10/30/60 m requests; reproject to EPSG:4326 if
  needed downstream.
- ESA WorldCover 2021 classes follow the ESA scheme (10=Trees, 20=Shrubs,
  30=Grassland, 40=Cropland, 50=Built-up, 60=Bare/sparse, 70=Snow/ice,
  80=Water, 90=Herbaceous wetland, 95=Mangroves, 100=Moss/lichen).
  A NLCD-to-ESA crosswalk table is saved alongside for CN lookup.
- SSURGO HSG download requires access to sdmdataaccess.nrcs.usda.gov (port 443).
  If blocked, follow the manual instructions written to inputs/ssurgo_manual_steps.txt.
"""

from __future__ import annotations

import json
import os
import textwrap
from pathlib import Path

import geopandas as gpd
import numpy as np
import py3dep
import requests
import rioxarray as rxr
import xarray as xr

# ── Settings ────────────────────────────────────────────────────────────────
os.environ["GDAL_HTTP_UNSAFESSL"] = "YES"   # allow AWS S3 COG access
os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = ".tif,.vrt,.img"

WATERSHED = Path("/home/user/hec-hms-pre-req/watershed.gpkg")
OUT_DIR = Path("/home/user/hec-hms-pre-req/inputs")
OUT_DIR.mkdir(exist_ok=True)

# ESA WorldCover 2021 v200 tile URL template (3°×3° tiles, named by SW corner)
ESA_WC_BASE = (
    "https://esa-worldcover.s3.eu-central-1.amazonaws.com"
    "/v200/2021/map/ESA_WorldCover_10m_2021_v200_{tile}_Map.tif"
)

# ── Load watershed ──────────────────────────────────────────────────────────
gdf = gpd.read_file(WATERSHED)
assert gdf.crs.to_epsg() == 4326, "Expected EPSG:4326"
west, south, east, north = gdf.total_bounds
geometry = gdf.geometry.iloc[0]

print(f"Watershed bbox (WGS84): W={west:.5f} S={south:.5f} E={east:.5f} N={north:.5f}\n")


# ════════════════════════════════════════════════════════════════════════════
# 1. DEM — USGS 3DEP 10 m (via AWS S3 COG, routed by py3dep.get_dem)
# ════════════════════════════════════════════════════════════════════════════
dem_path = OUT_DIR / "dem_10m.tif"
if dem_path.exists():
    print(f"[1/3] DEM already exists — skipping download ({dem_path.stat().st_size / 1e6:.1f} MB)")
else:
    print("[1/3] Downloading DEM — USGS 3DEP 10 m (AWS S3 COG) …")
    dem: xr.DataArray = py3dep.get_dem(geometry, resolution=10, crs="EPSG:4326")
    dem.rio.to_raster(dem_path, compress="deflate")
    print(f"  Saved  → {dem_path}  ({dem_path.stat().st_size / 1e6:.1f} MB)")
    print(f"  Shape  : {dem.shape}  CRS: {dem.rio.crs}\n")
print()


# ════════════════════════════════════════════════════════════════════════════
# 2. Land cover — ESA WorldCover 2021 10 m (AWS S3 COG)
# ════════════════════════════════════════════════════════════════════════════
lc_path = OUT_DIR / "esa_worldcover_2021_10m.tif"
if lc_path.exists():
    print(f"[2/3] Land cover already exists — skipping download ({lc_path.stat().st_size / 1e6:.1f} MB)")
else:
    print("[2/3] Downloading land cover — ESA WorldCover 2021 10 m (AWS S3 COG) …")

    def _esa_tile_name(lat: float, lon: float) -> str:
        """Return ESA WorldCover v200 tile name for a given lat/lon point.
        Tiles are 3°×3°; name uses the SW corner rounded to multiples of 3.
        """
        lat_sw = int(np.floor(lat / 3) * 3)
        lon_sw = int(np.floor(lon / 3) * 3)
        ns = "N" if lat_sw >= 0 else "S"
        ew = "W" if lon_sw < 0 else "E"
        return f"{ns}{abs(lat_sw):02d}{ew}{abs(lon_sw):03d}"

    tiles_needed: set[str] = set()
    for lat in (south, north):
        for lon in (west, east):
            tiles_needed.add(_esa_tile_name(lat, lon))

    print(f"  ESA WorldCover tiles needed: {sorted(tiles_needed)}")

    tile_arrays: list[xr.DataArray] = []
    for tile in sorted(tiles_needed):
        url = ESA_WC_BASE.format(tile=tile)
        print(f"  Reading tile {tile} from S3 …")
        da = rxr.open_rasterio(url, masked=True).squeeze("band", drop=True)
        tile_arrays.append(da)

    lc = tile_arrays[0] if len(tile_arrays) == 1 else xr.concat(tile_arrays, dim="x").sortby("x")

    buf = 0.01
    lc = lc.rio.clip_box(west - buf, south - buf, east + buf, north + buf)
    lc.rio.to_raster(lc_path, compress="deflate", dtype="uint8")
    print(f"  Saved  → {lc_path}  ({lc_path.stat().st_size / 1e6:.1f} MB)")
    print(f"  Shape  : {lc.shape}  CRS: {lc.rio.crs}")

# Write ESA→HEC-HMS land use crosswalk
CROSSWALK = {
    10: ("Trees",              "Forest"),
    20: ("Shrubland",          "Brush"),
    30: ("Grassland",          "Meadow"),
    40: ("Cropland",           "Row crops"),
    50: ("Built-up",           "Impervious"),
    60: ("Bare/sparse veg.",   "Fallow"),
    70: ("Snow and ice",       "Snow/ice"),
    80: ("Open water",         "Water"),
    90: ("Herbaceous wetland", "Wetland"),
    95: ("Mangroves",          "Forest"),
    100: ("Moss/lichen",       "Meadow"),
}
xwalk_lines = ["ESA_Code,ESA_Class,HMS_LandUse"]
for code, (cls, hms) in CROSSWALK.items():
    xwalk_lines.append(f"{code},{cls},{hms}")
(OUT_DIR / "esa_to_hms_landuse_crosswalk.csv").write_text("\n".join(xwalk_lines))
print("\n  Crosswalk → inputs/esa_to_hms_landuse_crosswalk.csv\n")


# ════════════════════════════════════════════════════════════════════════════
# 3. Soil / HSG — USDA SSURGO (SDM REST API)
# ════════════════════════════════════════════════════════════════════════════
print("[3/3] Downloading soil hydrologic soil groups — USDA SSURGO (SDM REST) …")

SSURGO_URL = "https://sdmdataaccess.nrcs.usda.gov/tabular/post.rest"
SSURGO_QUERY = textwrap.dedent(f"""\
    SELECT mu.mukey, mu.musym, mu.muname,
           c.hydgrpdcd, c.comppct_r
    FROM mapunit mu
    JOIN component c ON mu.mukey = c.mukey
    WHERE mu.mukey IN (
        SELECT mukey FROM SDA_Get_Mukey_from_intersection_with_WktWgs84(
            'polygon(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))'
        )
    )
    AND c.majcompflag = 'Yes'
    ORDER BY mu.mukey, c.comppct_r DESC
""")

def try_ssurgo_download() -> bool:
    try:
        resp = requests.post(
            SSURGO_URL,
            data={"query": SSURGO_QUERY, "format": "JSON+COLUMNNAME"},
            timeout=60,
            verify=False,
        )
        if resp.status_code != 200 or "Host not in allowlist" in resp.text:
            return False
        data = resp.json()
        rows = data.get("Table", [])
        if not rows:
            return False
        cols = rows[0]
        records = [dict(zip(cols, r)) for r in rows[1:]]
        hsg_map: dict[str, str] = {}
        for rec in records:
            mk = rec.get("mukey", "")
            hsg = rec.get("hydgrpdcd", "") or ""
            pct = float(rec.get("comppct_r") or 0)
            if mk not in hsg_map or pct > float(hsg_map.get(mk + "_pct", 0)):
                hsg_map[mk] = hsg
                hsg_map[mk + "_pct"] = str(pct)
        out = {"mukeys": hsg_map}
        (OUT_DIR / "ssurgo_hsg_tabular.json").write_text(json.dumps(out, indent=2))
        print(f"  Saved tabular HSG → inputs/ssurgo_hsg_tabular.json  ({len(hsg_map)//2} map units)")
        return True
    except Exception as ex:
        print(f"  SSURGO attempt failed: {ex}")
        return False

ssurgo_ok = try_ssurgo_download()

if not ssurgo_ok:
    print("  SSURGO service is not reachable from this environment.")
    instructions = textwrap.dedent(f"""\
        SSURGO Hydrologic Soil Group — Manual Download Instructions
        ===========================================================

        The USDA SSURGO soil service (sdmdataaccess.nrcs.usda.gov) is
        blocked in this environment. Use any of the methods below to obtain
        HSG data for the watershed and place the output in the inputs/ folder.

        Watershed bounding box (WGS84): W={west:.5f} S={south:.5f} E={east:.5f} N={north:.5f}

        METHOD 1 — Web Soil Survey (manual)
        ------------------------------------
        1. Go to https://websoilsurvey.nrcs.usda.gov/
        2. Navigate to the watershed area (use the bounding box above).
        3. Define an AOI by drawing the watershed boundary.
        4. Go to Soil Data Explorer → Soil Properties → Hydrologic Soil Group.
        5. Download as Thematic Map → export the raster or tabular data.
        6. Save to inputs/ssurgo_hsg.tif (raster) or inputs/ssurgo_hsg.gpkg (vector).

        METHOD 2 — USDA Geospatial Data Gateway (bulk download)
        --------------------------------------------------------
        1. Go to https://gdg.sc.egov.usda.gov/
        2. Select your state (Kentucky) → SSURGO → Download.
        3. Import with geopandas/pyogrio and extract 'hydgrpdcd' field.

        METHOD 3 — Python script (run from unrestricted network)
        ---------------------------------------------------------
        Run this from a machine with internet access:

            import pygeohydro as gh
            import geopandas as gpd

            gdf = gpd.read_file("watershed.gpkg")
            geometry = gdf.geometry.iloc[0]
            soil = gh.soil_gnatsgo("hydgrpdcd", geometry, crs=4326)
            soil["hydgrpdcd"].rio.to_raster("inputs/gnatsgo_hsg.tif")

        SSURGO SDM SQL query (usable at https://sdmdataaccess.nrcs.usda.gov/Query.aspx):
        ----------------------------------------------------------------------------------
        {SSURGO_QUERY}
    """)
    manual_path = OUT_DIR / "ssurgo_manual_steps.txt"
    manual_path.write_text(instructions)
    print(f"  Manual instructions → {manual_path}\n")


# ════════════════════════════════════════════════════════════════════════════
# Summary
# ════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("Download summary")
print("=" * 60)
for f in sorted(OUT_DIR.iterdir()):
    size_mb = f.stat().st_size / 1e6
    print(f"  {f.name:<45} {size_mb:>6.2f} MB")
print()
if not ssurgo_ok:
    print("ACTION REQUIRED: Soil / HSG data not downloaded.")
    print("  See inputs/ssurgo_manual_steps.txt for instructions.")
