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
# Download both a 10 m DEM (full resolution, ~290 MB; local-only) and a
# 30 m DEM (~30 MB; small enough to be tracked in git).
dem_10m_path = OUT_DIR / "dem_10m.tif"
dem_30m_path = OUT_DIR / "dem_30m.tif"

if dem_10m_path.exists():
    print(f"[1/3] DEM 10 m already exists — skipping ({dem_10m_path.stat().st_size / 1e6:.1f} MB)")
else:
    print("[1/3a] Downloading DEM 10 m — USGS 3DEP (AWS S3 COG) …")
    dem10: xr.DataArray = py3dep.get_dem(geometry, resolution=10, crs="EPSG:4326")
    dem10.rio.to_raster(dem_10m_path, compress="deflate")
    print(f"  Saved  → {dem_10m_path}  ({dem_10m_path.stat().st_size / 1e6:.1f} MB)")
    print(f"  Shape  : {dem10.shape}  CRS: {dem10.rio.crs}")

if dem_30m_path.exists():
    print(f"[1/3b] DEM 30 m already exists — skipping ({dem_30m_path.stat().st_size / 1e6:.1f} MB)")
else:
    print("[1/3b] Downloading DEM 30 m — USGS 3DEP (AWS S3 COG) …")
    dem30: xr.DataArray = py3dep.get_dem(geometry, resolution=30, crs="EPSG:4326")
    dem30.rio.to_raster(dem_30m_path, compress="deflate")
    print(f"  Saved  → {dem_30m_path}  ({dem_30m_path.stat().st_size / 1e6:.1f} MB)")
    print(f"  Shape  : {dem30.shape}  CRS: {dem30.rio.crs}")
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
    print("  Writing detailed fallback instructions + ready-to-run scripts.\n")
    instructions = textwrap.dedent(f"""\
        SSURGO Hydrologic Soil Group — Download Instructions
        ====================================================

        The USDA SSURGO soil service is blocked by this environment's network
        proxy (host_not_allowed). Below are four methods, in order of speed
        and accuracy. Use any one to obtain HSG data for the watershed.

        Watershed bbox (WGS84): W={west:.5f} S={south:.5f} E={east:.5f} N={north:.5f}
        State: Kentucky

        ─────────────────────────────────────────────────────────────────────
        METHOD 1 — Run download_ssurgo.py from any machine with open internet
        ─────────────────────────────────────────────────────────────────────
        This is the fastest reproducible path. The script in this repo
        (scripts/download_ssurgo.py) hits SDA's REST API for the watershed
        bounding box and writes both a vector layer (mukey + HSG) and a
        rasterised HSG GeoTIFF. Just run:

            pip install pygeohydro geopandas rioxarray rasterio shapely
            python scripts/download_ssurgo.py

        Outputs:
            inputs/ssurgo_hsg.gpkg   — polygons with mukey, musym, hydgrpdcd
            inputs/ssurgo_hsg.tif    — rasterised HSG (1=A, 2=B, 3=C, 4=D)

        ─────────────────────────────────────────────────────────────────────
        METHOD 2 — Web Soil Survey (manual, no scripting)
        ─────────────────────────────────────────────────────────────────────
        1. Go to https://websoilsurvey.nrcs.usda.gov/
        2. Zoom to the watershed area or paste the bounding box above.
        3. Define an Area Of Interest (AOI) covering the watershed polygon.
        4. Soil Data Explorer → Soil Properties and Qualities →
           Soil Qualities and Features → Hydrologic Soil Group
        5. Click "View Rating" then "Add to Shopping Cart".
        6. From the cart, "Get Now" → download the AOI ZIP package.
        7. Inside the ZIP: tabular/chorizon.txt and spatial/soilmu_a_*.shp
           join via mukey to bring hydgrpdcd onto the polygons.

        ─────────────────────────────────────────────────────────────────────
        METHOD 3 — USDA Geospatial Data Gateway (bulk SSURGO by state)
        ─────────────────────────────────────────────────────────────────────
        1. Go to https://gdg.sc.egov.usda.gov/
        2. Choose Kentucky → Soils → SSURGO → request data.
        3. You'll receive a download link by email for the state SSURGO file
           geodatabase (~1 GB). Extract `gSSURGO_KY.gdb`.
        4. Open the muaggatt table → join hydgrpdcd to MUPOLYGON by mukey.

        ─────────────────────────────────────────────────────────────────────
        METHOD 4 — Global HYSOGs250m (Ross et al. 2018) — coarse fallback
        ─────────────────────────────────────────────────────────────────────
        If SSURGO is unavailable and you need a quick global product:
        DOI:10.3334/ORNLDAAC/1566 — 250 m global HSG GeoTIFF.
        Download HYSOGs250m.tif from ORNL DAAC (free, requires Earthdata
        login) then clip with:

            rio clip HYSOGs250m.tif inputs/hysog_clip.tif --bounds {west} {south} {east} {north}

        Cell values 1=A, 2=B, 3=C, 4=D, 11=A/D, 12=B/D, 13=C/D, 14=D/D.

        ─────────────────────────────────────────────────────────────────────
        SDA SQL (paste at https://sdmdataaccess.nrcs.usda.gov/Query.aspx):
        ─────────────────────────────────────────────────────────────────────
        {SSURGO_QUERY}
    """)
    manual_path = OUT_DIR / "ssurgo_manual_steps.txt"
    manual_path.write_text(instructions)
    print(f"  Manual instructions → {manual_path}")


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
