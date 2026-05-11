# HEC-HMS Prerequisite Inputs

Reproducible download of the spatial input data needed to build a HEC-HMS
model for the watershed defined in `watershed.gpkg` (Kentucky, EPSG:4326,
bbox ≈ W=-85.97, S=37.40, E=-84.80, N=38.41).

The user is providing meteorological forcing separately (PRISM
precipitation). This repo handles the **terrain + land use + soil** inputs.

## Datasets

| Dataset | Source | Resolution | File | Tracked in git? |
|---|---|---|---|---|
| DEM (10 m) | USGS 3DEP, AWS S3 COG | 10 m | `inputs/dem_10m.tif` | No — 294 MB, exceeds GitHub limit. Run `download_inputs.py` to regenerate. |
| DEM (30 m) | USGS 3DEP, AWS S3 COG | 30 m | `inputs/dem_30m.tif` | Yes (~30 MB) |
| Land cover | ESA WorldCover 2021 v2, AWS S3 COG | 10 m | `inputs/esa_worldcover_2021_10m.tif` | Yes (~12 MB) |
| Land-cover crosswalk | This repo | — | `inputs/esa_to_hms_landuse_crosswalk.csv` | Yes |
| Soil HSG | USDA SSURGO (SDA REST) | ~30 m | `inputs/ssurgo_hsg.{gpkg,tif}` | When network allows — see below |

### Why ESA WorldCover instead of NLCD?

The sandbox where this repo was built blocks `www.mrlc.gov` (NLCD WMS).
ESA WorldCover 2021 is the recommended substitute:

- 10 m resolution (better than NLCD's 30 m)
- Hosted on a publicly accessible AWS S3 bucket
- 11-class scheme that maps cleanly to HEC-HMS land-use categories — see
  `inputs/esa_to_hms_landuse_crosswalk.csv`

If you need true NLCD instead, re-run `download_inputs.py` from a network
that can reach `mrlc.gov` and replace the call to `_download_esa_worldcover`
with `pygeohydro.nlcd_bygeom(...)`.

### Why is the soil layer not yet downloaded?

`sdmdataaccess.nrcs.usda.gov` is blocked here. The companion script
`scripts/download_ssurgo.py` will fetch SSURGO HSG polygons and rasterise
them once run from a machine with open internet. Full instructions are
written to `inputs/ssurgo_manual_steps.txt`. Four fallback paths are
included (SDA REST, Web Soil Survey, Geospatial Data Gateway, and a global
HYSOGs250m fallback).

## Reproducing the downloads

```bash
pip install py3dep pygeohydro geopandas rioxarray rasterio requests
python download_inputs.py        # DEM 10/30 m + ESA WorldCover + soil docs
python scripts/download_ssurgo.py   # SSURGO HSG — needs sdmdataaccess.nrcs.usda.gov access
```

Both scripts skip files that already exist, so they are safe to re-run.

## CRS notes

- DEMs come back in **EPSG:5070** (Albers Equal Area CONUS, metres). This
  is the native CRS for `py3dep.static_3dep_dem` and is recommended for
  HEC-HMS terrain processing because cell sizes are isotropic in metres.
- ESA WorldCover is **EPSG:4326**.
- SSURGO outputs (when downloaded) are **EPSG:4326**.

Reproject to your project CRS as needed inside HEC-HMS or with `rio warp`.
