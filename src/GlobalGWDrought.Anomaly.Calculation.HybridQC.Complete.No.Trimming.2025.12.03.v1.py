"""
GlobalGWDrought – Anomaly Calculation with Hybrid QC (CSV + XLSX outputs)

Purpose
-------
This script computes station-level year-to-year groundwater anomalies from annual records
(depth-to-water and/or groundwater elevation), applies a Hybrid QC procedure to exclude
physically implausible station-year Level measurements, and then produces:

  (i) QC-clean station anomalies (consecutive-year differences),
 (ii) station-level decadal summaries,
(iii) aquifer-linked station decadal tables via spatial join,
 (iv) aquifer-level decadal aggregates,
  (v) annual and decadal map products (TIFF + optional vector formats),
 (vi) optional plot-range diagnostics (e.g., if trimming is enabled for visualization).

All tabular outputs are written as BOTH CSV and Excel (XLSX) using the save_table() helper.
If a table exceeds Excel’s row limit, it is automatically split across multiple sheets.

---------------------------------------------------------------------
0) WORKFLOW OVERVIEW (CACHING BEHAVIOR)
---------------------------------------------------------------------
Primary “cache” inputs/outputs live in:
  - ./in_anomalies/station_anomalies_all.csv
  - ./in_anomalies/station_decadal_stats.csv

If these exist, the script LOADS them (does not recompute anomalies/decadal stats).
This prevents repeated recomputation and enables stable handoffs to post-analysis scripts.

Derived products (joins, aggregates, maps, diagnostics) are written under:
  - ./out_anomalies/

---------------------------------------------------------------------
1) INPUT FILES (CSV)
---------------------------------------------------------------------
A) AnnualDepthToGroundwater.csv  (optional)
   Default path: ./in_anomalies/AnnualDepthToGroundwater.csv
   Expected columns (minimum):
     - StnID : station/well identifier
     - Lat, Lon : coordinates (decimal degrees, WGS84 / EPSG:4326)
     - Year or IntegerYear : year
     - DepthToWater_m : depth to groundwater (meters, positive downward)
   Conversion to unified “wetness-like” Level:
     Level = -DepthToWater_m
   Interpretation:
     Shallower water table => larger Level (less negative / more positive)

B) AnnualGroundwaterElevation.csv  (optional)
   Default path: ./in_anomalies/AnnualGroundwaterElevation.csv
   Expected columns (minimum):
     - StnID : station/well identifier
     - Lat, Lon : coordinates
     - Year or IntegerYear : year
     - GroundwaterElevation_masl : groundwater elevation/head (m a.s.l.)
   Conversion:
     Level = GroundwaterElevation_masl

If both input tables exist, they are **NOT** averaged across sources.
Anomalies are computed per station primarily from AnnualDepthToGroundwater (depth), and
AnnualGroundwaterElevation (elev) is used only to ADD anomalies for years that are missing
from the depth-based anomaly series for that station.

C) Aquifer polygons (required for aquifer join/aggregates)
   Default path: ./in_shapefile/jasechko_et_al_2024_aquifers.shp
   Required/expected:
     - AQUIFER_ID (if missing, constructed from an available id/name field or index)

D) World basemap polygons (optional, for map aesthetics)
   Default path: ./in_shapefile/ne_110m_admin_0_countries.shp
   If missing, maps are still produced but without country outlines.

---------------------------------------------------------------------
2) OUTPUT FILES (ALL TABLES SAVED AS CSV + XLSX)
---------------------------------------------------------------------
A) Cache-style outputs (written to ./in_anomalies/ when computed)
1) ./in_anomalies/station_anomalies_all.csv + .xlsx
   Station-year anomaly table AFTER QC (if QC was applied during computation):
     - StnID, Year (later year), Lat, Lon, Anomaly (= Level_t - Level_{t-1})
   Notes:
     - Only consecutive-year pairs are used (Year_t - Year_{t-1} == 1).
     - Stations with < min_anoms_per_well anomalies are dropped.

2) ./in_anomalies/station_decadal_stats.csv + .xlsx
   Station-decade summaries computed from QC-clean anomalies.

B) QC report outputs (written to ./out_anomalies/)
3) ./out_anomalies/station_flagged_years.csv + .xlsx
   Station-year Level measurements excluded by Hybrid QC, with reasons and trigger info.
   This file is created only when anomalies are computed from annual Level data (i.e., when
   station_anomalies_all.csv is NOT already present in ./in_anomalies/).

C) Aquifer join & aggregation outputs (written to ./out_anomalies/)
4) ./out_anomalies/station_decadal_stats_with_aquifer.csv + .xlsx
   Station-decade summaries with aquifer IDs added via spatial join.

5) ./out_anomalies/aquifer_decadal_stats.csv + .xlsx
   Aquifer-decade aggregates computed from the station-decadal-with-aquifer table.

D) Diagnostics / transparency outputs (written to ./out_anomalies/)
6) ./out_anomalies/global_outlier_anomalies.csv + .xlsx
   Very extreme anomalies (by |Anomaly| quantile, e.g., 0.999) listed for inspection.
   These records are NOT removed automatically by this step (informational listing).

7) ./out_anomalies/annual_max_min_anomalies.csv + .xlsx
   Annual anomaly ranges and plotted range metadata used in annual maps.
   By default, maps use the FULL anomaly range (p0–p100), i.e., all finite values.

8) (Optional; only if visualization trimming is enabled)
   ./out_anomalies/annual_excluded_trimmed_anomalies.csv + .xlsx
   ./out_anomalies/decadal_excluded_trimmed_mean_anomalies.csv + .xlsx
   Records excluded FROM PLOTS ONLY when a trimming range is requested (e.g., 5–95%).
   This does NOT alter the anomaly computation itself; it documents visual filtering.

E) Map outputs (written under ./out_anomalies/)
9)  ./out_anomalies/maps_annual/<region>/
      - map_anomalies_<year>_<region>.tif (+ optional vector format)
10) ./out_anomalies/maps_decadal/<region>/
      - map_decadal_mean_anom_<decade>s_<region>.tif (+ optional vector format)

Regions for mapping are controlled by REGION_BOUNDS and typically include:
  global, US, Europe

---------------------------------------------------------------------
3) HYBRID QUALITY CONTROL (QC) – OUTLIER DETECTION AND EXCLUSION
---------------------------------------------------------------------
Motivation
- Some wells show physically implausible interannual changes (very large jumps),
  typically due to data entry/transfer/unit/decimal errors in one or a few years.

Critical principle
- QC is applied at the Level-year measurement stage (Year, Level).
  A single erroneous Level(y) can distort TWO consecutive anomalies:
      A_y     = L_y     - L_{y-1}
      A_{y+1} = L_{y+1} - L_y
  Therefore, removing only A_y is insufficient; the suspicious Level(y) record is excluded,
  and anomalies are recomputed from the cleaned Level series.

Hybrid QC logic (summary)
(A) Hard global cap on anomaly magnitude (always candidate):
    If |A_t| > QC_ANOM_CAP (default ~100 m/year), the jump is treated as implausible.
    The script drops the more suspicious endpoint year by comparing local Level inconsistency
    (Hampel-style score). If confirmation is not available, it drops the later year.

(B) Station-wise robust anomaly screening (Median/MAD):
    For each station:
        z_rob = (A_t - median(A)) / (1.4826 * MAD(A))
    Candidate condition:
        |z_rob| >= QC_ZROB
    AND a magnitude floor:
        |A_t| >= max(QC_MIN_ABS_FOR_Z, QC_STN_ABS_MULT * (1.4826*MAD(A)))

(C) Local confirmation on Level (Hampel-style):
    Candidates from (B) are excluded only if the year’s Level is locally inconsistent:
        LevelLocalScore(y) = |L_y - median(L in [y-W..y+W])| / (1.4826*MAD in [y-W..y+W])
    Exclude only if:
        LevelLocalScore >= QC_LEVEL_CONFIRM_K
    If local stats cannot be computed (insufficient points), a conservative fallback applies:
        exclude only if |A_t| >= QC_ROBUST_FALLBACK_ABS.

(D) Spike-pair rule (strong evidence):
    If two consecutive anomalies are both “large” and opposite-signed (A_y and A_{y+1}),
    the middle year Level(y) is a classic spike pattern. The script excludes Level(y) if
    it is cap-triggered OR locally inconsistent (LevelLocalScore >= QC_LEVEL_CONFIRM_K).

All excluded station-year Level measurements are recorded in station_flagged_years.*.

---------------------------------------------------------------------
4) OPTIONAL MAP VISUALIZATION TRIMMING (PLOT RANGE CONTROL)
---------------------------------------------------------------------
Default behavior (recommended when QC and pre-cleaning are trusted):
- Maps use the FULL anomaly range (p0–p100): all finite anomaly values contribute to
  the plotted distribution. No trimming-based exclusion is applied to plots.

Optional behavior (visual emphasis):
- For enhanced visual contrast in the presence of heavy tails, percentile trimming
  can be enabled for PLOTTING ONLY by setting a trimming range, e.g.:
      trim_low=0.05, trim_high=0.95
  In that case, points outside the requested percentile bounds are not shown on the
  map (or are documented as excluded), and exclusion listings are written under
  ./out_anomalies/ for transparency.

---------------------------------------------------------------------
5) REQUIREMENTS
---------------------------------------------------------------------
pip install pandas numpy geopandas shapely matplotlib tqdm openpyxl

---------------------------------------------------------------------
Author: Fatih DİKBAŞ
Spyder-ready, Python >= 3.10
"""

from __future__ import annotations
import os
import time
import shutil
from dataclasses import dataclass
from typing import Tuple

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm, BoundaryNorm, Normalize

# Tüm figürler ve kayıtlar için 400 dpi + TIFF
# Raster harita çıktıları için önerilen çözünürlük (dpi)
RASTER_DPI = 400  # 300'den yüksek, 600'e göre daha hafif ve makale için yeterli

plt.rcParams.update({
    "figure.dpi": RASTER_DPI,
    "savefig.dpi": RASTER_DPI,
    "savefig.format": "tiff",
})


def safe_save_figure(fig, out_path: str, dpi: int = RASTER_DPI, fmt: str = "tiff") -> str:
    """
    Figürü öncelikle TIFF olarak kaydetmeye çalışır.
    Eğer Windows/Pillow kaynaklı bir OSError (örn. Errno 22) oluşursa,
    aynı adı PNG olarak kaydedip yolu döndürür.

    out_path: uzantılı ya da uzantısız verilebilir; fonksiyon .tif olarak normalize eder.
    """
    # Kesinlikle tam ve normalize edilmiş bir yol kullan
    out_path = os.path.abspath(out_path)
    root, _ = os.path.splitext(out_path)
    tiff_path = root + ".tif"

    try:
        fig.savefig(tiff_path, dpi=dpi, format=fmt, bbox_inches="tight")
        return tiff_path
    except OSError as e:
        # TIFF kaydı başarısız olursa PNG'ye düş
        print(f"[WARN] TIFF save failed for {tiff_path} ({e}). Falling back to PNG.")
        png_path = root + ".png"
        fig.savefig(png_path, dpi=dpi, format="png", bbox_inches="tight")
        return png_path


# =========================
# CSV + EXCEL (XLSX) SAVE HELPER
# =========================
EXCEL_MAX_ROWS = 1_048_576  # Excel row limit


MAD_SCALE = 1.4826  # convert MAD to sigma-like robust scale
def save_table(df: pd.DataFrame, csv_path: str, *, sheet_base: str = "data") -> None:
    """
    Always save both:
      - CSV to csv_path
      - XLSX to same base name (csv_path with .xlsx)

    Handles Excel row limit by splitting into multiple sheets if needed.

    Requires: openpyxl
    """
    # -------------------------------------------------
    # Console visibility for long saves (CSV + XLSX)
    # -------------------------------------------------
    t0 = time.perf_counter()
    n_rows, n_cols = df.shape
    # 1) CSV
    print(f"[SAVE] CSV -> {csv_path} | rows={n_rows:,}, cols={n_cols} (starting)", flush=True)
    df.to_csv(csv_path, index=False)
    t_csv = time.perf_counter()
    print(f"[SAVE] CSV done in {t_csv - t0:,.1f}s.", flush=True)

    # 2) XLSX
    root, ext = os.path.splitext(csv_path)
    xlsx_path = root + ".xlsx"
    print(f"[SAVE] XLSX -> {xlsx_path} (starting)", flush=True)

    # Sheet names limited to 31 chars; keep a safe base
    sheet_base = (sheet_base or "data")[:20]

    n = len(df)
    chunk_size = EXCEL_MAX_ROWS - 1  # leave one row for header
    n_sheets = max(1, (n + chunk_size - 1) // chunk_size)
    print(f"[SAVE] XLSX details: sheets={n_sheets}, chunk_size={chunk_size:,} rows/sheet (header excluded)", flush=True)

    try:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            if n <= chunk_size:
                print(f"[SAVE] XLSX sheet 1/1: '{sheet_base}' (rows=0–{n-1:,})", flush=True)
                df.to_excel(writer, index=False, sheet_name=sheet_base)
                print(f"[SAVE] XLSX sheet 1/1 done.", flush=True)
            else:
                # Split into multiple sheets
                for i, start in enumerate(range(0, n, chunk_size), 1):
                    end_row = min(start + chunk_size, n)
                    print(f"[SAVE] XLSX sheet {i}/{n_sheets}: '{sheet_base}_{i}' (rows={start:,}–{end_row-1:,})", flush=True)
                    part = df.iloc[start:start + chunk_size]
                    part.to_excel(writer, index=False, sheet_name=f"{sheet_base}_{i}")
                    print(f"[SAVE] XLSX sheet {i}/{n_sheets} done.", flush=True)
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            "Excel output requested but 'openpyxl' is not installed. "
            "Install it with: pip install openpyxl"
        ) from e

    t_end = time.perf_counter()
    try:
        # xlsx_path is defined above; print summary for visibility.
        print(f"[SAVE] Completed: {csv_path} + {xlsx_path} | total {t_end - t0:,.1f}s", flush=True)
    except Exception:
        # Never fail the pipeline due to logging.
        pass


def save_csv_and_xlsx(df: pd.DataFrame, csv_path: str, *, sheet_base: str = "data") -> str:
    """
    Backward-compatible helper.

    Some parts of the codebase call save_csv_and_xlsx(df, csv_path, sheet_base=...).
    In this script, the canonical writer is save_table(), which always writes both CSV and XLSX.
    This wrapper keeps older calls working without changing/removing any existing code.

    Returns
    -------
    xlsx_path : str
        The path of the generated .xlsx file (same base name as csv_path).
    """
    save_table(df, csv_path, sheet_base=sheet_base)
    root, _ = os.path.splitext(csv_path)
    return root + ".xlsx"
def save_csv_and_xlsx(df: pd.DataFrame, csv_path: str, *, sheet_base: str = "data") -> str:
    """
    Backward-compatible helper.

    Some parts of the codebase call save_csv_and_xlsx(df, csv_path, sheet_base=...).
    In this script, the canonical writer is save_table(), which always writes both CSV and XLSX.
    This wrapper keeps older calls working without changing/removing any existing code.

    Returns
    -------
    xlsx_path : str
        The path of the generated .xlsx file (same base name as csv_path).
    """
    save_table(df, csv_path, sheet_base=sheet_base)
    root, _ = os.path.splitext(csv_path)
    return root + ".xlsx"




# =========================
# CACHE MIRRORING (in_anomalies -> out_anomalies)
# =========================
def mirror_cache_files_to_out_dir(cfg: "Config", cache_csv_path: str, *, label: str = "") -> None:
    """
    Post-analysis scripts expect key cache-style tables to be available under ./out_anomalies as well.
    This helper mirrors (copies) the cache outputs written under ./in_anomalies into ./out_anomalies.

    It mirrors both:
      - CSV  (cache_csv_path)
      - XLSX (same base name, .xlsx)

    Copy strategy (fast):
      - If destination does not exist, copy.
      - If destination exists but is older than source, overwrite by copying.
    """
    try:
        os.makedirs(cfg.out_dir, exist_ok=True)
    except Exception:
        pass

    base_csv = os.path.basename(cache_csv_path)
    out_csv_path = os.path.join(cfg.out_dir, base_csv)

    cache_xlsx_path = os.path.splitext(cache_csv_path)[0] + ".xlsx"
    out_xlsx_path = os.path.splitext(out_csv_path)[0] + ".xlsx"

    pairs = [
        (cache_csv_path, out_csv_path),
        (cache_xlsx_path, out_xlsx_path),
    ]

    any_copied = False
    t0 = time.perf_counter()

    for src, dst in pairs:
        try:
            if not os.path.isfile(src):
                continue

            if (not os.path.isfile(dst)) or (os.path.getmtime(src) > os.path.getmtime(dst)):
                shutil.copy2(src, dst)
                any_copied = True
        except Exception as e:
            print(f"[WARN] Could not mirror file {src} -> {dst}: {e}")

    if any_copied:
        tag = f" ({label})" if label else ""
        print(
            f"   [INFO] Mirrored cache outputs{tag} to out_dir in {time.perf_counter() - t0:,.1f}s: "
            f"{out_csv_path} and {out_xlsx_path}",
            flush=True,
        )
# =========================
# CONFIG
# =========================
@dataclass
class Config:
    # Klasörler (çalışma dizinine göre)
    anomalies_dir: str = r"./in_anomalies"
    shapefile_dir: str = r"./in_shapefile"

    # Haritalar için vektör çıktı (PDF vb.) kaydedilsin mi?
    save_vector_maps: bool = True
    vector_format: str = "pdf"   # "pdf", "svg" veya "eps" olabilir

    # Girdi yolları (gerekirse yeniden hesap için; çoğu durumda kullanılmayacak)
    path_depth_csv: str = r"./in_anomalies/AnnualDepthToGroundwater.csv"
    path_elev_csv: str  = r"./in_anomalies/AnnualGroundwaterElevation.csv"

    # Akifer sınırları shapefile (tüm .shp/.shx/.dbf/.prj dosyaları bu klasörde olmalı)
    path_aquifer_shp: str = r"./in_shapefile/jasechko_et_al_2024_aquifers.shp"

    # Dünya altlık haritası (Natural Earth admin_0 countries veya benzeri)
    # Örn: https://www.naturalearthdata.com/downloads/110m-cultural-vectors/
    path_world_shp: str = r"./in_shapefile/ne_110m_admin_0_countries.shp"

    # Çıktı klasörü
    out_dir: str = r"./out_anomalies"

    # Minimum eşik ve diğer ayarlar
    min_anoms_per_well: int = 2
    min_anoms_per_decade: int = 2

    data_crs: str = "EPSG:4326"
    demo_decade: int = 2000


    # -------------------------------------------------
    # PROGRESS / LOGGING (for long-running in-memory steps)
    # -------------------------------------------------
    enable_processing_progress: bool = True   # master switch for extra progress/log outputs
    decadal_station_progress_mode: str = "stations"  # "stations" (tqdm per station) or "steps" (coarse step bar)
    decadal_station_progress_desc: str = "Decadal station stats"


    # -------------------------------------------------
    # HYBRID QC PARAMETERS (Level-year outlier exclusion)
    # -------------------------------------------------
    qc_enabled: bool = True

    # (A) Hard cap: always exclude if |anomaly| > cap (m/year)
    qc_anom_cap_m: float = 100.0

    # (B) Robust station-wise screening on anomalies (Median/MAD)
    qc_zrob_thresh: float = 6.0
    qc_min_abs_anom_for_z_m: float = 10.0   # global absolute floor (prevents tiny fluctuations from being flagged)
    qc_station_abs_mult: float = 10.0       # station-scaled floor multiplier: QC_STN_ABS_MULT * (1.4826*MAD(A))

    # (C) Local Level confirmation window (±W years)
    qc_level_window_years: int = 2
    qc_level_min_points: int = 3

    # Robust candidates are excluded only if LevelLocalScore >= confirm_k
    qc_level_confirm_k: float = 6.0

    # If local Level score cannot be computed, robust candidate is excluded only if |anomaly| >= fallback_abs
    qc_robust_fallback_abs_m: float = 50.0

    def __post_init__(self):
        os.makedirs(self.out_dir, exist_ok=True)
        os.makedirs(self.anomalies_dir, exist_ok=True)
        os.makedirs(self.shapefile_dir, exist_ok=True)


CFG = Config()

def get_map_output_dir(cfg: Config, map_type: str, region: str) -> str:
    """
    Haritaları map_type ('decadal' veya 'annual') ve region'a göre
    farklı klasörlere kaydetmek için yardımcı fonksiyon.

    Örnekler:
        ./out_anomalies/maps_decadal/global/
        ./out_anomalies/maps_annual/US/
    """
    if map_type not in {"decadal", "annual"}:
        raise ValueError(f"Unsupported map_type: {map_type}")

    base = "maps_decadal" if map_type == "decadal" else "maps_annual"
    out_dir = os.path.join(cfg.out_dir, base, region)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


# Basit coğrafi alt-bölgeler (cropped haritalar için)
REGION_BOUNDS = {
    "global": {  # referans için, tüm dünya
        "lon_min": -180.0,
        "lon_max": 180.0,
        "lat_min": -90.0,
        "lat_max": 90.0,
    },
    # ABD (yaklaşık; 48 eyalet + biraz Kanada sınırı)
    "US": {
        "lon_min": -130.0,
        "lon_max": -60.0,
        "lat_min": 20.0,
        "lat_max": 55.0,
    },
    # Batı / Orta Avrupa + İskandinavya
    "Europe": {
        "lon_min": -15.0,
        "lon_max": 35.0,
        "lat_min": 30.0,
        "lat_max": 72.0,
    },
}


def _region_from_lonlat(lon: float, lat: float) -> str:
    """
    Basit bbox'a göre akifer centroid'ini US / Europe / Other world
    olarak sınıflandırır.
    """
    b_us = REGION_BOUNDS["US"]
    if (
        b_us["lon_min"] <= lon <= b_us["lon_max"]
        and b_us["lat_min"] <= lat <= b_us["lat_max"]
    ):
        return "US"

    b_eu = REGION_BOUNDS["Europe"]
    if (
        b_eu["lon_min"] <= lon <= b_eu["lon_max"]
        and b_eu["lat_min"] <= lat <= b_eu["lat_max"]
    ):
        return "Europe"

    return "Other world"


# =========================
# DATA LOADING
# =========================
def load_level_data(cfg: Config) -> pd.DataFrame:
    """
    Load and harmonize groundwater level data from Depth and Elevation tables.

    Output columns:
        StnID, Lat, Lon, Year, Level, SourceType

    Where:
        Level is a "wetness-like" level in meters (higher = wetter):
            Level = -DepthToWater_m   if Depth file is used
            Level = GroundwaterElevation_masl  if Elevation file is used

    If both tables are available, they are concatenated BUT NOT averaged across sources.
    Duplicate years are only averaged within the SAME source (depth or elev).
    """
    frames = []

    # ---- DEPTH FILE ----
    if os.path.isfile(cfg.path_depth_csv):
        df_d = pd.read_csv(cfg.path_depth_csv)

        # Yıl kolonu: IntegerYear veya Year olabilir
        if "Year" not in df_d.columns:
            if "IntegerYear" in df_d.columns:
                df_d = df_d.rename(columns={"IntegerYear": "Year"})
            else:
                raise ValueError("Could not find Year / IntegerYear column in depth CSV.")

        # Derinlik kolonu: DepthToWater_m
        if "DepthToWater_m" not in df_d.columns:
            cand = [c for c in df_d.columns if "depth" in c.lower()]
            if cand:
                df_d = df_d.rename(columns={cand[0]: "DepthToWater_m"})
            else:
                raise ValueError("Could not find DepthToWater_m column in depth CSV.")

        depth_cols = ["StnID", "Lat", "Lon", "Year", "DepthToWater_m"]
        for c in depth_cols:
            if c not in df_d.columns:
                raise ValueError(f"Missing column in depth CSV: {c}")

        df_d = df_d[depth_cols].dropna()
        df_d["Year"] = df_d["Year"].astype(int)

        # W = -Depth (higher W = shallower water table = wetter)
        df_d["Level"] = -df_d["DepthToWater_m"].astype(float)
        df_d["SourceType"] = "depth"

        # -------------------------------------------------
        # Station-level descriptive statistics (RAW DEPTH)
        # -------------------------------------------------
        try:
            df_d_stats = df_d[(df_d["Year"] >= 1800) & (df_d["Year"] <= 2100)].copy()
            depth_desc = compute_station_descriptive_stats_raw(
                df_d_stats,
                raw_col="DepthToWater_m",
                source_name="AnnualDepthToGroundwater",
                raw_unit="m",
            )
            if len(depth_desc):
                depth_desc_csv = os.path.join(cfg.out_dir, "station_descriptive_stats_depth.csv")
                depth_desc_xlsx = save_csv_and_xlsx(depth_desc, depth_desc_csv, sheet_base="desc_depth")
                print(f"   Saved depth descriptive stats: {depth_desc_csv} and {depth_desc_xlsx}")
        except Exception as e:
            print(f"[WARN] Depth descriptive stats could not be computed/saved: {e}")

        df_d = df_d[["StnID", "Lat", "Lon", "Year", "Level", "SourceType"]]
        frames.append(df_d)

    # ---- ELEVATION FILE ----
    if os.path.isfile(cfg.path_elev_csv):
        df_e = pd.read_csv(cfg.path_elev_csv)

        # Yıl kolonu: IntegerYear veya Year
        if "Year" not in df_e.columns:
            if "IntegerYear" in df_e.columns:
                df_e = df_e.rename(columns={"IntegerYear": "Year"})
            else:
                raise ValueError("Could not find Year / IntegerYear column in elevation CSV.")

        # Elevation kolonu: GroundwaterElevation_masl (HydroShare tanımı)
        if "GroundwaterElevation_masl" not in df_e.columns:
            cand = [c for c in df_e.columns if "elev" in c.lower()]
            if cand:
                df_e = df_e.rename(columns={cand[0]: "GroundwaterElevation_masl"})
            else:
                raise ValueError("Could not find GroundwaterElevation_masl column in elevation CSV.")

        elev_cols = ["StnID", "Lat", "Lon", "Year", "GroundwaterElevation_masl"]
        for c in elev_cols:
            if c not in df_e.columns:
                raise ValueError(f"Missing column in elevation CSV: {c}")

        df_e = df_e[elev_cols].dropna()
        df_e["Year"] = df_e["Year"].astype(int)

        df_e["Level"] = df_e["GroundwaterElevation_masl"].astype(float)
        df_e["SourceType"] = "elev"

        # -------------------------------------------------
        # Station-level descriptive statistics (RAW ELEVATION)
        # -------------------------------------------------
        try:
            df_e_stats = df_e[(df_e["Year"] >= 1800) & (df_e["Year"] <= 2100)].copy()
            elev_desc = compute_station_descriptive_stats_raw(
                df_e_stats,
                raw_col="GroundwaterElevation_masl",
                source_name="AnnualGroundwaterElevation",
                raw_unit="m asl",
            )
            if len(elev_desc):
                elev_desc_csv = os.path.join(cfg.out_dir, "station_descriptive_stats_elevation.csv")
                elev_desc_xlsx = save_csv_and_xlsx(elev_desc, elev_desc_csv, sheet_base="desc_elev")
                print(f"   Saved elevation descriptive stats: {elev_desc_csv} and {elev_desc_xlsx}")
        except Exception as e:
            print(f"[WARN] Elevation descriptive stats could not be computed/saved: {e}")


        df_e = df_e[["StnID", "Lat", "Lon", "Year", "Level", "SourceType"]]
        frames.append(df_e)

    if not frames:
        raise FileNotFoundError("Neither depth nor elevation CSVs found. Check CONFIG paths.")
    # Birleştir (depth + elev), fakat KESİNLİKLE birbirleriyle ortalama alma
    df = pd.concat(frames, ignore_index=True)
    df = df[(df["Year"] >= 1800) & (df["Year"] <= 2100)].copy()

    # CSV okuma sonrası (coverage + within-source de-dup) büyük veri setlerinde uzun sürebilir.
    # Burada "adım bazlı" bir progress bar veriyoruz; ağır pandas işlemleri sırasında bar ancak adım bitince ilerler.
    with tqdm(total=2, desc="Post-read processing", unit="step") as pbar:

        # --- Coverage (depth vs elev) for transparency (no source mixing) ---
        try:
            src_counts = (
                df.groupby(["StnID", "SourceType"])["Year"]
                .nunique()
                .unstack(fill_value=0)
            )

            # Ensure both columns exist
            if "depth" not in src_counts.columns:
                src_counts["depth"] = 0
            if "elev" not in src_counts.columns:
                src_counts["elev"] = 0

            src_counts = (
                src_counts
                .reset_index()
                .rename(columns={"depth": "N_years_depth", "elev": "N_years_elev"})
                .sort_values("StnID")
                .reset_index(drop=True)
            )

            save_table(
                src_counts,
                os.path.join(cfg.out_dir, "station_source_coverage.csv"),
                sheet_base="source_coverage",
            )
        except Exception as e:
            print(f"[WARN] station_source_coverage table could not be saved: {e}")

        pbar.update(1)

        # Now it is safe to average duplicates within the SAME source (same variable)
        df = (
            df.groupby(["StnID", "Year", "SourceType"], as_index=False)
              .agg(Lat=("Lat", "first"), Lon=("Lon", "first"), Level=("Level", "mean"))
        )

        pbar.update(1)

    return df


def load_aquifers(cfg: Config) -> gpd.GeoDataFrame:
    """
    Load aquifer polygons and ensure they have AQUIFER_ID, Region and correct CRS.
    """
    aq = gpd.read_file(cfg.path_aquifer_shp)

    # 1) Temel CRS: veri CRS'i (EPSG:4326)
    if aq.crs is None:
        aq = aq.set_crs(cfg.data_crs)
    elif aq.crs.to_string() != cfg.data_crs:
        aq = aq.to_crs(cfg.data_crs)

    # 2) AQUIFER_ID yoksa, mevcut bir ID/name alanından veya index'ten üret
    if "AQUIFER_ID" not in aq.columns:
        cand = [c for c in aq.columns if "id" in c.lower() or "name" in c.lower()]
        if cand:
            aq["AQUIFER_ID"] = aq[cand[0]].astype(str)
        else:
            aq["AQUIFER_ID"] = aq.index.astype(str)

    # 3) Centroid'i coğrafi CRS'te değil, projeksiyonlu bir CRS'te hesapla
    #    (uyarıyı kaldırmak ve geometrik tutarlılığı artırmak için).
    try:
        # Global eşit-alan projeksiyon (varsa tercihimiz)
        aq_proj = aq.to_crs("EPSG:6933")
    except Exception as e:
        print(
            f"[WARN] Could not reproject aquifers to EPSG:6933 ({e}); "
            "falling back to EPSG:3857 for centroid computation."
        )
        aq_proj = aq.to_crs("EPSG:3857")

    # Projeksiyonlu uzayda centroid
    cent_proj = aq_proj.geometry.centroid
    # Centroidleri tekrar WGS84 (EPSG:4326) koordinatlarına döndür
    cent_lonlat = cent_proj.to_crs(cfg.data_crs)

    lons = cent_lonlat.x.values
    lats = cent_lonlat.y.values

    # 4) Basit bbox'a göre Region sınıflaması (US / Europe / Other world)
    aq["Region"] = [
        _region_from_lonlat(lo, la) for lo, la in zip(lons, lats)
    ]

    # Sadece ihtiyacımız olan kolonları tut
    return aq[["AQUIFER_ID", "Region", "geometry"]].copy()


def compute_station_descriptive_stats_raw(
    df_raw: pd.DataFrame,
    *,
    raw_col: str,
    source_name: str,
    raw_unit: str,
) -> pd.DataFrame:
    """
    Compute per-station descriptive statistics from raw annual observations.

    Notes
    -----
    - Works on station-year annual values.
    - Within this function, duplicate (StnID, Year) rows are averaged to ensure one value per year
      (consistent with the later harmonization logic).
    - Uses a station-loop with tqdm to provide transparent progress for potentially long runs.
    """
    required = {"StnID", "Lat", "Lon", "Year", raw_col, "Level"}
    missing = required - set(df_raw.columns)
    if missing:
        raise ValueError(f"Descriptive stats input is missing columns: {sorted(missing)}")

    d0 = df_raw[list(required)].copy()

    # Ensure numeric types
    d0["Year"] = pd.to_numeric(d0["Year"], errors="coerce").astype("Int64")
    d0[raw_col] = pd.to_numeric(d0[raw_col], errors="coerce")
    d0["Level"] = pd.to_numeric(d0["Level"], errors="coerce")

    d0 = d0.dropna(subset=["StnID", "Lat", "Lon", "Year", raw_col, "Level"]).copy()
    if d0.empty:
        return pd.DataFrame()

    d0["Year"] = d0["Year"].astype(int)

    # Count raw records per station (before de-duplication by Year)
    n_records_raw = d0.groupby("StnID").size()

    # Average duplicates at (StnID, Year) to get unique annual values per station
    d = (
        d0.groupby(["StnID", "Year"], as_index=False)
        .agg(
            Lat=("Lat", "first"),
            Lon=("Lon", "first"),
            **{raw_col: (raw_col, "mean")},
            Level=("Level", "mean"),
        )
    )

    if d.empty:
        return pd.DataFrame()

    d = d.sort_values(["StnID", "Year"]).reset_index(drop=True)

    def _safe_stats(arr: np.ndarray) -> dict:
        a = np.asarray(arr, dtype=float)
        a = a[np.isfinite(a)]
        if a.size == 0:
            return {
                "min": np.nan, "max": np.nan, "mean": np.nan, "median": np.nan, "std": np.nan,
                "p05": np.nan, "p25": np.nan, "p75": np.nan, "p95": np.nan,
            }
        q05, q25, q75, q95 = np.quantile(a, [0.05, 0.25, 0.75, 0.95])
        std = float(np.std(a, ddof=1)) if a.size >= 2 else np.nan
        return {
            "min": float(np.min(a)),
            "max": float(np.max(a)),
            "mean": float(np.mean(a)),
            "median": float(np.median(a)),
            "std": std,
            "p05": float(q05),
            "p25": float(q25),
            "p75": float(q75),
            "p95": float(q95),
        }

    rows: list[dict] = []
    n_stn = int(d["StnID"].nunique())
    for stn_id, g in tqdm(d.groupby("StnID", sort=False), total=n_stn, desc=f"Desc stats ({source_name})"):
        g = g.sort_values("Year")
        lat = float(g["Lat"].iloc[0])
        lon = float(g["Lon"].iloc[0])

        years = g["Year"].to_numpy(dtype=int)
        raw_vals = g[raw_col].to_numpy(dtype=float)
        lvl_vals = g["Level"].to_numpy(dtype=float)

        start_year = int(years[0])
        end_year = int(years[-1])
        n_years = int(g.shape[0])
        span_years = int(end_year - start_year + 1)
        missing_years = int(span_years - n_years)

        raw_s = _safe_stats(raw_vals)
        lvl_s = _safe_stats(lvl_vals)

        raw_start = float(raw_vals[0]) if np.isfinite(raw_vals[0]) else np.nan
        raw_end = float(raw_vals[-1]) if np.isfinite(raw_vals[-1]) else np.nan
        lvl_start = float(lvl_vals[0]) if np.isfinite(lvl_vals[0]) else np.nan
        lvl_end = float(lvl_vals[-1]) if np.isfinite(lvl_vals[-1]) else np.nan

        raw_net_change = raw_end - raw_start if (np.isfinite(raw_end) and np.isfinite(raw_start)) else np.nan
        lvl_net_change = lvl_end - lvl_start if (np.isfinite(lvl_end) and np.isfinite(lvl_start)) else np.nan

        denom = float(end_year - start_year) if end_year != start_year else np.nan
        raw_rate = raw_net_change / denom if (np.isfinite(raw_net_change) and np.isfinite(denom)) else np.nan
        lvl_rate = lvl_net_change / denom if (np.isfinite(lvl_net_change) and np.isfinite(denom)) else np.nan

        n_rec = int(n_records_raw.get(stn_id, 0))
        n_dup = int(n_rec - n_years)

        rows.append(
            {
                "Source": source_name,
                "RawVariable": raw_col,
                "RawUnit": raw_unit,
                "StnID": stn_id,
                "Lat": lat,
                "Lon": lon,
                "StartYear": start_year,
                "EndYear": end_year,
                "SpanYears": span_years,
                "N_years": n_years,
                "MissingYears": missing_years,
                "N_records_raw": n_rec,
                "N_duplicate_year_records": n_dup,
                "Raw_start": raw_start,
                "Raw_end": raw_end,
                "Raw_net_change": raw_net_change,
                "Raw_rate_per_year": raw_rate,
                "Level_start": lvl_start,
                "Level_end": lvl_end,
                "Level_net_change": lvl_net_change,
                "Level_rate_per_year": lvl_rate,
                "Raw_min": raw_s["min"],
                "Raw_max": raw_s["max"],
                "Raw_mean": raw_s["mean"],
                "Raw_median": raw_s["median"],
                "Raw_std": raw_s["std"],
                "Raw_p05": raw_s["p05"],
                "Raw_p25": raw_s["p25"],
                "Raw_p75": raw_s["p75"],
                "Raw_p95": raw_s["p95"],
                "Level_min": lvl_s["min"],
                "Level_max": lvl_s["max"],
                "Level_mean": lvl_s["mean"],
                "Level_median": lvl_s["median"],
                "Level_std": lvl_s["std"],
                "Level_p05": lvl_s["p05"],
                "Level_p25": lvl_s["p25"],
                "Level_p75": lvl_s["p75"],
                "Level_p95": lvl_s["p95"],
            }
        )

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)

    # Final column order (optional; keep readable)
    preferred_front = [
        "Source", "RawVariable", "RawUnit", "StnID", "Lat", "Lon",
        "StartYear", "EndYear", "SpanYears", "N_years", "MissingYears",
        "N_records_raw", "N_duplicate_year_records",
        "Raw_start", "Raw_end", "Raw_net_change", "Raw_rate_per_year",
        "Level_start", "Level_end", "Level_net_change", "Level_rate_per_year",
    ]
    remaining = [c for c in out.columns if c not in preferred_front]
    out = out[preferred_front + remaining]

    return out


# ===========================
# ROBUST HELPERS (Hybrid QC)
# ===========================
def _median_and_mad(arr: np.ndarray) -> Tuple[float, float]:
    """Return (median, MAD) from finite values; MAD = median(|x - median|)."""
    a = np.asarray(arr, dtype=float)
    a = a[np.isfinite(a)]
    if a.size == 0:
        return np.nan, np.nan
    med = float(np.median(a))
    mad = float(np.median(np.abs(a - med)))
    return med, mad


def _robust_scale_from_mad(mad: float) -> float:
    """Convert MAD to a sigma-like robust scale (1.4826 * MAD)."""
    return float(MAD_SCALE * mad) if np.isfinite(mad) else np.nan


def _sign_nonzero(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def qc_and_compute_yearly_anomalies(df_level: pd.DataFrame, cfg: Config, *, apply_min_filter: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply hybrid QC at the Level-year measurement stage to detect and exclude suspicious years,
    then compute consecutive-year anomalies from the cleaned series.

    Returns
    -------
    anoms : DataFrame
        Columns: StnID, Year, Lat, Lon, Anomaly (Year is the later year in the pair)
    flagged : DataFrame
        Station-year Level records excluded by QC (with reasons and triggers).

    Notes
    -----
    A single erroneous Level(y) contaminates two consecutive anomalies:
        A_y     = L_y     - L_{y-1}
        A_{y+1} = L_{y+1} - L_y
    Therefore, QC is performed on Level-year records and anomalies are recomputed afterwards.
    """
    anom_rows: list[dict] = []
    flagged_rows: list[dict] = []

    cap = float(cfg.qc_anom_cap_m)
    zthr = float(cfg.qc_zrob_thresh)
    min_abs_global = float(cfg.qc_min_abs_anom_for_z_m)
    stn_mult = float(cfg.qc_station_abs_mult)

    w = int(cfg.qc_level_window_years)
    min_pts = int(cfg.qc_level_min_points)
    confirm_k = float(cfg.qc_level_confirm_k)
    fallback_abs = float(cfg.qc_robust_fallback_abs_m)

    for stn_id, g in tqdm(df_level.groupby("StnID"), total=df_level["StnID"].nunique(), desc="Hybrid QC + anomalies"):
        g = g.sort_values("Year").reset_index(drop=True)
        years = g["Year"].to_numpy(dtype=int)
        levels = g["Level"].to_numpy(dtype=float)

        if len(g) < 2:
            continue

        # Local (year-based) median/MAD for Level (Hampel-style)
        local_med = np.full(len(g), np.nan, dtype=float)
        local_scale = np.full(len(g), np.nan, dtype=float)  # 1.4826*MAD

        for i in range(len(g)):
            y = years[i]
            m = (years >= (y - w)) & (years <= (y + w))
            vals = levels[m]
            vals = vals[np.isfinite(vals)]
            if vals.size >= min_pts:
                med, mad = _median_and_mad(vals)
                sc = _robust_scale_from_mad(mad)
                local_med[i] = med
                local_scale[i] = sc if np.isfinite(sc) and sc > 0 else np.nan

        def level_score(idx: int) -> float:
            val = levels[idx]
            med = local_med[idx]
            sc = local_scale[idx]
            if not (np.isfinite(val) and np.isfinite(med)):
                return np.nan
            if not np.isfinite(sc) or sc <= 0:
                return np.inf if val != med else 0.0
            return float(abs(val - med) / sc)

        # Build consecutive-year anomalies (pre-QC)
        anom_year: list[int] = []
        anom_val: list[float] = []
        prev_idx: list[int] = []
        cur_idx: list[int] = []

        for i in range(1, len(g)):
            if years[i] - years[i - 1] == 1:
                anom_year.append(int(years[i]))
                anom_val.append(float(levels[i] - levels[i - 1]))
                prev_idx.append(i - 1)
                cur_idx.append(i)

        if len(anom_val) == 0:
            continue

        anom_val_arr = np.asarray(anom_val, dtype=float)

        # Robust station-wise stats for anomalies
        medA, madA = _median_and_mad(anom_val_arr)
        scaleA = _robust_scale_from_mad(madA)
        if np.isfinite(scaleA) and scaleA > 0:
            zrob = (anom_val_arr - medA) / scaleA
        else:
            zrob = np.full_like(anom_val_arr, np.nan, dtype=float)

        stn_floor = (stn_mult * scaleA) if (np.isfinite(scaleA) and scaleA > 0) else 0.0
        abs_min_effective = max(min_abs_global, float(stn_floor))

        cap_hit = np.abs(anom_val_arr) > cap
        robust_hit = (np.abs(anom_val_arr) >= abs_min_effective) & np.isfinite(zrob) & (np.abs(zrob) >= zthr)
        big = cap_hit | robust_hit

        year_to_i = {anom_year[i]: i for i in range(len(anom_year))}
        flagged_years: dict[int, dict] = {}

        # Spike-pair rule: two consecutive big anomalies with opposite sign
        for y in anom_year:
            if (y + 1) not in year_to_i:
                continue
            i0 = year_to_i[y]
            i1 = year_to_i[y + 1]
            if not (big[i0] and big[i1]):
                continue

            s0 = _sign_nonzero(float(anom_val_arr[i0]))
            s1 = _sign_nonzero(float(anom_val_arr[i1]))
            if s0 == 0 or s1 == 0 or s0 != -s1:
                continue

            idx_mid = cur_idx[i0]  # year y (middle level)
            mid_score = level_score(idx_mid)
            spike_cap = bool(cap_hit[i0] or cap_hit[i1])

            if spike_cap or (np.isfinite(mid_score) and mid_score >= confirm_k):
                if y not in flagged_years:
                    flagged_years[y] = {
                        "Reason": "spike_pair_year_level",
                        "TriggerAnomYear": int(y),
                        "TriggerAnomValue": float(anom_val_arr[i0]),
                        "TriggerZrob": float(zrob[i0]) if np.isfinite(zrob[i0]) else np.nan,
                        "NextAnomYear": int(y + 1),
                        "NextAnomValue": float(anom_val_arr[i1]),
                        "NextZrob": float(zrob[i1]) if np.isfinite(zrob[i1]) else np.nan,
                        "Cap": cap,
                        "MedianA": medA,
                        "MAD_A": madA,
                        "ScaleA": scaleA,
                        "AbsMinEffective": abs_min_effective,
                        "ConfirmK": confirm_k,
                    }

        # Single big anomaly handling
        for i in range(len(anom_year)):
            if not big[i]:
                continue

            y = int(anom_year[i])
            t_idx = cur_idx[i]
            prev_idx_i = prev_idx[i]

            score_t = level_score(t_idx)
            score_prev = level_score(prev_idx_i)

            if np.isfinite(score_t) and np.isfinite(score_prev):
                drop_idx = t_idx if score_t >= score_prev else prev_idx_i
            elif np.isfinite(score_t):
                drop_idx = t_idx
            elif np.isfinite(score_prev):
                drop_idx = prev_idx_i
            else:
                drop_idx = t_idx

            drop_year = int(years[drop_idx])

            if drop_year in flagged_years:
                continue

            if cap_hit[i]:
                reason = "cap_jump_year_level"
                confirmed = True
            else:
                if not robust_hit[i]:
                    continue

                drop_score = level_score(drop_idx)
                if np.isfinite(drop_score):
                    confirmed = bool(drop_score >= confirm_k)
                else:
                    confirmed = bool(abs(float(anom_val_arr[i])) >= fallback_abs)

                if not confirmed:
                    continue

                reason = "robust_jump_year_level"

            flagged_years[drop_year] = {
                "Reason": reason,
                "TriggerAnomYear": int(y),
                "TriggerAnomValue": float(anom_val_arr[i]),
                "TriggerZrob": float(zrob[i]) if np.isfinite(zrob[i]) else np.nan,
                "NextAnomYear": np.nan,
                "NextAnomValue": np.nan,
                "NextZrob": np.nan,
                "Cap": cap,
                "MedianA": medA,
                "MAD_A": madA,
                "ScaleA": scaleA,
                "AbsMinEffective": abs_min_effective,
                "ConfirmK": confirm_k,
                "Confirmed": confirmed,
            }

        # Collect flagged Level-year rows
        if flagged_years:
            for yr, meta in sorted(flagged_years.items(), key=lambda kv: kv[0]):
                idx = np.where(years == int(yr))[0]
                if idx.size == 0:
                    continue
                idx0 = int(idx[0])

                flagged_rows.append({
                    "StnID": stn_id,
                    "Year": int(yr),
                    "Lat": float(g.loc[idx0, "Lat"]),
                    "Lon": float(g.loc[idx0, "Lon"]),
                    "Level": float(g.loc[idx0, "Level"]),
                    "Reason": meta.get("Reason", ""),
                    "TriggerAnomYear": meta.get("TriggerAnomYear", np.nan),
                    "TriggerAnomValue": meta.get("TriggerAnomValue", np.nan),
                    "TriggerZrob": meta.get("TriggerZrob", np.nan),
                    "NextAnomYear": meta.get("NextAnomYear", np.nan),
                    "NextAnomValue": meta.get("NextAnomValue", np.nan),
                    "NextZrob": meta.get("NextZrob", np.nan),
                    "Cap": meta.get("Cap", np.nan),
                    "MedianA": meta.get("MedianA", np.nan),
                    "MAD_A": meta.get("MAD_A", np.nan),
                    "ScaleA": meta.get("ScaleA", np.nan),
                    "AbsMinEffective": meta.get("AbsMinEffective", np.nan),
                    "QC_Zrob": zthr,
                    "QC_MinAbsGlobal": min_abs_global,
                    "QC_StnAbsMult": stn_mult,
                    "QC_LevelConfirmK": confirm_k,
                    "QC_RobustFallbackAbs": fallback_abs,
                    "LevelLocalMedian": float(local_med[idx0]) if np.isfinite(local_med[idx0]) else np.nan,
                    "LevelLocalScale": float(local_scale[idx0]) if np.isfinite(local_scale[idx0]) else np.nan,
                    "LevelLocalScore": float(level_score(idx0)) if np.isfinite(level_score(idx0)) else np.nan,
                    "QC_LevelWindowYears": w,
                    "QC_LevelMinPoints": min_pts,
                })

        # Compute anomalies AFTER excluding flagged years
        if flagged_years:
            keep_mask = ~np.isin(years, np.array(list(flagged_years.keys()), dtype=int))
            g2 = g.loc[keep_mask].sort_values("Year").reset_index(drop=True)
        else:
            g2 = g

        if len(g2) < 2:
            continue

        years2 = g2["Year"].to_numpy(dtype=int)
        levels2 = g2["Level"].to_numpy(dtype=float)

        for j in range(1, len(g2)):
            if years2[j] - years2[j - 1] != 1:
                continue
            anom_rows.append({
                "StnID": stn_id,
                "Year": int(years2[j]),
                "Lat": float(g2.loc[j, "Lat"]),
                "Lon": float(g2.loc[j, "Lon"]),
                "Anomaly": float(levels2[j] - levels2[j - 1])
            })

    if anom_rows:
        anoms = pd.DataFrame(anom_rows)
    else:
        anoms = pd.DataFrame(
            {
                "StnID": pd.Series(dtype="object"),
                "Year": pd.Series(dtype="int64"),
                "Lat": pd.Series(dtype="float64"),
                "Lon": pd.Series(dtype="float64"),
                "Anomaly": pd.Series(dtype="float64"),
            }
        )

    if flagged_rows:
        flagged = pd.DataFrame(flagged_rows)
    else:
        flagged = pd.DataFrame(
            {
                "StnID": pd.Series(dtype="object"),
                "Year": pd.Series(dtype="int64"),
                "Lat": pd.Series(dtype="float64"),
                "Lon": pd.Series(dtype="float64"),
                "Level": pd.Series(dtype="float64"),
                "Reason": pd.Series(dtype="object"),
            }
        )

    if apply_min_filter and len(anoms):
        counts = anoms.groupby("StnID")["Anomaly"].size().rename("n_anom")
        keep_ids = counts[counts >= cfg.min_anoms_per_well].index
        anoms = anoms[anoms["StnID"].isin(keep_ids)].copy()

    return anoms, flagged


# =========================
# ANOMALY CALCULATIONS
# =========================
def compute_yearly_anomalies(df: pd.DataFrame, cfg: Config, *, apply_min_filter: bool = True) -> pd.DataFrame:
    """
    For each station, compute year-to-year anomalies only for consecutive years.

    anomaly_t = Level_t - Level_{t-1}

    Output columns:
        StnID, Year, Lat, Lon, Anomaly
    (Year is the later year in the pair, i.e., t)
    """
    rows = []

    for stn_id, g in tqdm(df.groupby("StnID"), total=df["StnID"].nunique(), desc="Computing anomalies"):
        g = g.sort_values("Year")
        # Compute differences
        diff_level = g["Level"].diff()
        diff_years = g["Year"].diff()

        # Only keep where year difference is exactly 1 (consecutive years)
        mask = (diff_years == 1)
        if mask.sum() == 0:
            continue

        anom = diff_level[mask]
        year_t = g["Year"][mask]
        lat = g["Lat"][mask]
        lon = g["Lon"][mask]

        tmp = pd.DataFrame({
            "StnID": stn_id,
            "Year": year_t.values,
            "Lat": lat.values,
            "Lon": lon.values,
            "Anomaly": anom.values
        })
        rows.append(tmp)

    if rows:
        anoms = pd.concat(rows, ignore_index=True)
    else:
        # Boş ama beklenen kolonları/dtype'ları taşıyan DataFrame döndür
        print("   [WARN] No consecutive-year anomalies could be computed (rows is empty).")
        anoms = pd.DataFrame(
            {
                "StnID": pd.Series(dtype="object"),
                "Year": pd.Series(dtype="int64"),
                "Lat": pd.Series(dtype="float64"),
                "Lon": pd.Series(dtype="float64"),
                "Anomaly": pd.Series(dtype="float64"),
            }
        )

    # Filter wells with at least cfg.min_anoms_per_well anomalies
    if apply_min_filter:
        counts = anoms.groupby("StnID")["Anomaly"].size().rename("n_anom")
        keep_ids = counts[counts >= cfg.min_anoms_per_well].index
        anoms = anoms[anoms["StnID"].isin(keep_ids)].copy()

    return anoms


def compute_priority_anomalies_depth_then_elev(df_level_all: pd.DataFrame, cfg: Config) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Source-priority anomaly computation.

    Rules implemented (per user specification):
      1) For each station, anomalies are computed first from AnnualDepthToGroundwater (SourceType='depth').
      2) If the same station also exists in AnnualGroundwaterElevation (SourceType='elev'), elevation-based
         anomalies are computed and only those with a NEW (StnID, Year) not already produced by depth
         anomalies are appended to that station's anomaly list.
      3) No averaging is performed between the two source files (they are different variables).
         Within each source, duplicate (StnID, Year) rows are averaged beforehand.

    Returns
    -------
    anoms : DataFrame
        Columns include: StnID, Year, Lat, Lon, Anomaly, SourceType
        (Year is the later year in the consecutive pair.)
    flagged : DataFrame
        QC-flagged station-year Level records (if qc_enabled); includes SourceType.
        Empty if qc_enabled is False.
    """

    cols = ["StnID", "Year", "Lat", "Lon", "Level"]

    if "SourceType" in df_level_all.columns:
        df_depth = df_level_all[df_level_all["SourceType"] == "depth"][cols].copy()
        df_elev  = df_level_all[df_level_all["SourceType"] == "elev"][cols].copy()
    else:
        # Backward-compat: treat as single source (depth-like)
        df_depth = df_level_all[cols].copy()
        df_elev = pd.DataFrame(columns=cols)

    use_qc = bool(getattr(cfg, "qc_enabled", False))

    # --- Depth anomalies (primary) ---
    if len(df_depth):
        if use_qc:
            anoms_d, flagged_d = qc_and_compute_yearly_anomalies(df_depth, cfg, apply_min_filter=False)
        else:
            anoms_d = compute_yearly_anomalies(df_depth, cfg, apply_min_filter=False)
            flagged_d = pd.DataFrame()
        anoms_d = anoms_d.copy()
        anoms_d["SourceType"] = "depth"
        if len(flagged_d):
            flagged_d = flagged_d.copy()
            flagged_d["SourceType"] = "depth"
    else:
        anoms_d = pd.DataFrame(columns=["StnID", "Year", "Lat", "Lon", "Anomaly", "SourceType"])
        flagged_d = pd.DataFrame()

    # --- Elevation anomalies (secondary, only add missing years) ---
    if len(df_elev):
        if use_qc:
            anoms_e, flagged_e = qc_and_compute_yearly_anomalies(df_elev, cfg, apply_min_filter=False)
        else:
            anoms_e = compute_yearly_anomalies(df_elev, cfg, apply_min_filter=False)
            flagged_e = pd.DataFrame()
        anoms_e = anoms_e.copy()
        anoms_e["SourceType"] = "elev"
        if len(flagged_e):
            flagged_e = flagged_e.copy()
            flagged_e["SourceType"] = "elev"
    else:
        anoms_e = pd.DataFrame(columns=["StnID", "Year", "Lat", "Lon", "Anomaly", "SourceType"])
        flagged_e = pd.DataFrame()

    # Merge anomalies with (StnID, Year) priority: depth first, then elev only if year is new
    if len(anoms_d) and len(anoms_e):
        idx_d = pd.MultiIndex.from_frame(anoms_d[["StnID", "Year"]])
        idx_e = pd.MultiIndex.from_frame(anoms_e[["StnID", "Year"]])
        add_mask = ~idx_e.isin(idx_d)
        anoms_add = anoms_e.loc[add_mask].copy()
        anoms = pd.concat([anoms_d, anoms_add], ignore_index=True)
    elif len(anoms_d):
        anoms = anoms_d.copy()
    else:
        anoms = anoms_e.copy()

    # Final station-level minimum anomaly count filter (applied AFTER merging both sources)
    if len(anoms):
        counts = anoms.groupby("StnID")["Anomaly"].size().rename("n_anom")
        keep_ids = counts[counts >= cfg.min_anoms_per_well].index
        anoms = anoms[anoms["StnID"].isin(keep_ids)].copy()

    anoms = anoms.sort_values(["StnID", "Year", "SourceType"]).reset_index(drop=True)

    # Merge flagged tables (QC only) and optionally keep stations that survive the final anomaly filter
    if use_qc:
        flagged = pd.concat([flagged_d, flagged_e], ignore_index=True) if (len(flagged_d) or len(flagged_e)) else pd.DataFrame()
        if len(flagged) and len(anoms):
            flagged = flagged[flagged["StnID"].isin(anoms["StnID"].unique())].copy()
        return anoms, flagged

    return anoms, pd.DataFrame()


def detect_and_save_global_outliers(anoms: pd.DataFrame, cfg: Config, quantile: float = 0.999) -> pd.DataFrame:
    """
    Tüm dünya için, mutlak anomalisi en uçta olan gözlemleri belirler.

    Yöntem:
      - |Anomaly| için verilen quantile (varsayılan 0.999) değerini bulur.
      - |Anomaly| bu eşikten büyük olanları outlier kabul eder.
      - Sonuçları out_anomalies/global_outlier_anomalies.csv dosyasına kaydeder.

    Not:
      - Outlier'lar HARİÇ tutulmaz; sadece inceleme için listelenir.
    """
    arr = anoms["Anomaly"].to_numpy()
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        print("   [WARN] No anomalies available for global outlier detection.")
        return anoms.iloc[0:0].copy()

    # |Anomaly| için eşik
    thr = np.quantile(np.abs(arr), quantile)

    outliers = anoms[np.abs(anoms["Anomaly"]) > thr].copy()
    out_path = os.path.join(cfg.out_dir, "global_outlier_anomalies.csv")
    save_table(outliers, out_path, sheet_base="global_outliers")

    print(f"   -> {len(outliers):,} global outlier anomalies "
          f"(|Anomaly| > {thr:.2f}) saved to: {out_path} and {os.path.splitext(out_path)[0] + '.xlsx'}")

    return outliers

def assign_decade(year: int) -> int:
    """
    Assign a decade label as starting year, e.g. 2003 -> 2000, 1999 -> 1990.
    """
    return (year // 10) * 10

def compute_decadal_station_stats_with_progress(anoms: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """
    Progress-enabled decadal station statistics.

    Provides progress visibility for large global datasets.
    Two modes:
      - mode='stations': tqdm over stations (true progress), then within-station decade aggregation.
      - mode='steps': coarse step progress (useful if station-loop is undesirable), using vectorized groupby.

    Output schema matches compute_decadal_station_stats().
    """
    t0 = time.perf_counter()

    if anoms is None or len(anoms) == 0:
        return pd.DataFrame(
            {
                "StnID": pd.Series(dtype="object"),
                "Decade": pd.Series(dtype="int64"),
                "n_anom": pd.Series(dtype="int64"),
                "sum_anom": pd.Series(dtype="float64"),
                "sum_neg": pd.Series(dtype="float64"),
                "sum_pos": pd.Series(dtype="float64"),
                "n_neg": pd.Series(dtype="int64"),
                "n_pos": pd.Series(dtype="int64"),
                "Lat": pd.Series(dtype="float64"),
                "Lon": pd.Series(dtype="float64"),
                "ratio_neg": pd.Series(dtype="float64"),
                "ratio_pos": pd.Series(dtype="float64"),
                "frac_neg": pd.Series(dtype="float64"),
                "frac_pos": pd.Series(dtype="float64"),
            }
        )

    df = anoms.copy()

    # Vectorized decade assignment (faster than apply(assign_decade); same result)
    df["Decade"] = (df["Year"].astype(int) // 10) * 10

    mode = str(getattr(cfg, "decadal_station_progress_mode", "stations")).lower().strip()
    desc = str(getattr(cfg, "decadal_station_progress_desc", "Decadal station stats")).strip() or "Decadal station stats"

    n_rows = int(len(df))
    n_stn = int(df["StnID"].nunique())
    print(f"   [INFO] Decadal station stats input: rows={n_rows:,}, stations={n_stn:,} | mode='{mode}'", flush=True)

    # Coarse step progress (vectorized)
    if mode == "steps":
        with tqdm(total=4, desc=desc, unit="step") as pbar:
            df["_neg"] = df["Anomaly"].where(df["Anomaly"] < 0, 0.0)
            df["_pos"] = df["Anomaly"].where(df["Anomaly"] > 0, 0.0)
            df["_n_neg"] = (df["Anomaly"] < 0).astype("int64")
            df["_n_pos"] = (df["Anomaly"] > 0).astype("int64")
            pbar.update(1)

            grp = df.groupby(["StnID", "Decade"])
            agg = grp.agg(
                n_anom=("Anomaly", "size"),
                sum_anom=("Anomaly", "sum"),
                sum_neg=("_neg", "sum"),
                sum_pos=("_pos", "sum"),
                n_neg=("_n_neg", "sum"),
                n_pos=("_n_pos", "sum"),
                Lat=("Lat", "mean"),
                Lon=("Lon", "mean"),
            ).reset_index()
            pbar.update(1)

            agg = agg[agg["n_anom"] >= cfg.min_anoms_per_decade].copy()
            pbar.update(1)

            agg["ratio_neg"] = agg["sum_neg"] / agg["n_anom"]
            agg["ratio_pos"] = agg["sum_pos"] / agg["n_anom"]
            agg["frac_neg"] = agg["n_neg"] / agg["n_anom"]
            agg["frac_pos"] = agg["n_pos"] / agg["n_anom"]
            pbar.update(1)

        # Cleanup helper cols if present
        for c in ["_neg", "_pos", "_n_neg", "_n_pos"]:
            if c in df.columns:
                pass

        if len(agg):
            agg = agg.sort_values(["StnID", "Decade"]).reset_index(drop=True)

        print(f"   [INFO] Decadal station stats (steps) done in {time.perf_counter() - t0:,.1f}s | rows={len(agg):,}", flush=True)
        return agg

    # Default: per-station progress (true progress)
    out_parts: list[pd.DataFrame] = []
    gb = df.groupby("StnID", sort=False)

    for stn_id, g in tqdm(gb, total=n_stn, desc=desc, unit="stn"):
        if len(g) == 0:
            continue

        gg = g[["Decade", "Anomaly", "Lat", "Lon"]].copy()
        gg["sum_neg"] = gg["Anomaly"].where(gg["Anomaly"] < 0, 0.0)
        gg["sum_pos"] = gg["Anomaly"].where(gg["Anomaly"] > 0, 0.0)
        gg["n_neg"] = (gg["Anomaly"] < 0).astype("int64")
        gg["n_pos"] = (gg["Anomaly"] > 0).astype("int64")

        a = (
            gg.groupby("Decade", as_index=False)
            .agg(
                n_anom=("Anomaly", "size"),
                sum_anom=("Anomaly", "sum"),
                sum_neg=("sum_neg", "sum"),
                sum_pos=("sum_pos", "sum"),
                n_neg=("n_neg", "sum"),
                n_pos=("n_pos", "sum"),
                Lat=("Lat", "mean"),
                Lon=("Lon", "mean"),
            )
        )

        if len(a) == 0:
            continue

        a.insert(0, "StnID", stn_id)
        out_parts.append(a)

    if out_parts:
        agg = pd.concat(out_parts, ignore_index=True)
    else:
        agg = pd.DataFrame(
            {
                "StnID": pd.Series(dtype="object"),
                "Decade": pd.Series(dtype="int64"),
                "n_anom": pd.Series(dtype="int64"),
                "sum_anom": pd.Series(dtype="float64"),
                "sum_neg": pd.Series(dtype="float64"),
                "sum_pos": pd.Series(dtype="float64"),
                "n_neg": pd.Series(dtype="int64"),
                "n_pos": pd.Series(dtype="int64"),
                "Lat": pd.Series(dtype="float64"),
                "Lon": pd.Series(dtype="float64"),
            }
        )

    if len(agg):
        agg = agg[agg["n_anom"] >= cfg.min_anoms_per_decade].copy()
        agg["ratio_neg"] = agg["sum_neg"] / agg["n_anom"]
        agg["ratio_pos"] = agg["sum_pos"] / agg["n_anom"]
        agg["frac_neg"] = agg["n_neg"] / agg["n_anom"]
        agg["frac_pos"] = agg["n_pos"] / agg["n_anom"]

        agg = agg.sort_values(["StnID", "Decade"]).reset_index(drop=True)

    print(f"   [INFO] Decadal station stats (stations) done in {time.perf_counter() - t0:,.1f}s | rows={len(agg):,}", flush=True)
    return agg


def compute_decadal_station_stats(anoms: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """
    For each (StnID, decade) pair, compute:

        n_anom      = number of anomalies in that decade
        sum_anom    = sum of anomalies
        sum_neg     = sum of negative anomalies
        sum_pos     = sum of positive anomalies
        n_neg       = count of negatives
        n_pos       = count of positives
        ratio_neg   = sum_neg / n_anom
        ratio_pos   = sum_pos / n_anom
        frac_neg    = n_neg   / n_anom
        frac_pos    = n_pos   / n_anom

    Decade is assigned based on anomaly Year (later year in the pair).
    """

    # -------------------------------------------------
    # Progress-enabled path (requested): provides visibility for large runs.
    # Falls back to the original vectorized implementation if disabled or if an error occurs.
    # -------------------------------------------------
    if bool(getattr(cfg, "enable_processing_progress", True)):
        try:
            return compute_decadal_station_stats_with_progress(anoms, cfg)
        except Exception as e:
            print(f"[WARN] Decadal-station progress path failed ({e}). Falling back to vectorized groupby.", flush=True)

    df = anoms.copy()
    df["Decade"] = df["Year"].apply(assign_decade)

    grp = df.groupby(["StnID", "Decade"])
    agg = grp["Anomaly"].agg(
        n_anom="size",
        sum_anom="sum",
        sum_neg=lambda x: x[x < 0].sum() if (x < 0).any() else 0.0,
        sum_pos=lambda x: x[x > 0].sum() if (x > 0).any() else 0.0,
        n_neg=lambda x: (x < 0).sum(),
        n_pos=lambda x: (x > 0).sum(),
    ).reset_index()

    # Filter by minimum anomalies per (StnID, decade)
    agg = agg[agg["n_anom"] >= cfg.min_anoms_per_decade].copy()

    # Per-decade station location (use mean Lat/Lon over that decade; should be constant)
    coords = df.groupby(["StnID", "Decade"]).agg(
        Lat=("Lat", "mean"),
        Lon=("Lon", "mean"),
    ).reset_index()

    agg = agg.merge(coords, on=["StnID", "Decade"], how="left")

    # Ratios
    agg["ratio_neg"] = agg["sum_neg"] / agg["n_anom"]
    agg["ratio_pos"] = agg["sum_pos"] / agg["n_anom"]
    agg["frac_neg"] = agg["n_neg"] / agg["n_anom"]
    agg["frac_pos"] = agg["n_pos"] / agg["n_anom"]

    return agg


# ==============================
# SPATIAL JOIN & REGIONAL STATS
# ==============================
def spatial_join_aquifers(df: pd.DataFrame, aq: gpd.GeoDataFrame, cfg: Config) -> pd.DataFrame:
    """
    Spatially join stations (Lat, Lon) with aquifer polygons.
    """

    t0 = time.perf_counter()
    try:
        print(f"   [INFO] Spatial join: points={len(df):,}, aquifer_polygons={len(aq):,} (starting)...", flush=True)
    except Exception:
        print("   [INFO] Spatial join: starting...", flush=True)

    gpts = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["Lon"], df["Lat"]),
        crs=cfg.data_crs
    )
    gpts = gpd.sjoin(gpts, aq, how="left", predicate="within")
    gpts = gpts.drop(columns=["index_right"])
    print(f"   [INFO] Spatial join: completed in {time.perf_counter() - t0:,.1f}s.", flush=True)

    return pd.DataFrame(gpts.drop(columns="geometry"))


def compute_aquifer_decadal_stats(stn_decade: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate station-level decadal stats to aquifer-level decadal stats.

    Region bilgisi, akifer shapefile'ından geldiği için her AQUIFER_ID için tekil
    kabul edilir ve decadal istatistiklere taşınır.
    """

    t0 = time.perf_counter()
    try:
        print(f"   [INFO] Aquifer decadal: input rows={len(stn_decade):,} (starting aggregation)...", flush=True)
    except Exception:
        print("   [INFO] Aquifer decadal: starting aggregation...", flush=True)

    df = stn_decade.dropna(subset=["AQUIFER_ID"]).copy()
    if "Region" not in df.columns:
        raise ValueError("station_decadal_stats_with_aquifer must contain 'Region' column.")

    grp = df.groupby(["AQUIFER_ID", "Region", "Decade"])
    agg = grp.agg(
        n_stn_decade=("StnID", "size"),
        mean_ratio_neg=("ratio_neg", "mean"),
        mean_ratio_pos=("ratio_pos", "mean"),
        mean_frac_neg=("frac_neg", "mean"),
        mean_frac_pos=("frac_pos", "mean"),
        mean_sum_anom=("sum_anom", "mean"),
        mean_sum_neg=("sum_neg", "mean"),
        mean_sum_pos=("sum_pos", "mean"),
    ).reset_index()

    print(f"   [INFO] Aquifer decadal: completed in {time.perf_counter() - t0:,.1f}s | rows={len(agg):,}", flush=True)

    return agg


# =========================
# PLOTTING
# =========================

# =========================
# HELPER: DYNAMIC COLOR LIMITS
# =========================

def get_dynamic_limits(values) -> Tuple[float, float]:
    """
    Verilen serinin min/max değerlerini alır,
    veri aralığını esas alarak aşağı ve yukarı yuvarlanmış (vmin, vmax)
    döndürür.
    """
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return None, None

    vmin = float(arr.min())
    vmax = float(arr.max())

    # Tüm değerler neredeyse aynıysa, etrafına küçük bir margin aç
    if np.isclose(vmin, vmax):
        margin = 0.1 * (abs(vmin) if vmin != 0 else 1.0)
        return vmin - margin, vmax + margin

    max_abs = max(abs(vmin), abs(vmax))

    if max_abs == 0:
        # İstisnai durum: tüm değerler 0'a çok yakın
        return -1.0, 1.0

    # max_abs mertebesine göre yuvarlama birimi (unit) seç
    exponent_raw = np.floor(np.log10(max_abs))

    if max_abs < 1000:
        exponent_adj = exponent_raw
    else:
        # Çok büyük değerlerde daha sık aralık için bir mertebe küçült
        exponent_adj = exponent_raw - 1

    unit = 10.0 ** exponent_adj  # örn. 10, 100, 0.1, 0.01 vb.

    vmin_round = np.floor(vmin / unit) * unit
    vmax_round = np.ceil(vmax / unit) * unit

    return float(vmin_round), float(vmax_round)


def make_quantile_cmap(values, n_bins: int = 7, cmap_name: str = "Spectral"):
    """
    Build a discrete colormap and norm based on quantiles of the data.
    """
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return None, None, None

    # Compute quantile boundaries: 0%, 10%, ..., 100%
    q = np.linspace(0.0, 1.0, n_bins + 1)
    edges = np.quantile(arr, q)

    # Remove duplicate boundaries (can happen with many ties)
    edges = np.unique(edges)

    # If we have fewer than 2 unique edges, fall back to simple Normalize
    if edges.size <= 2:
        vmin = float(arr.min())
        vmax = float(arr.max())
        if np.isclose(vmin, vmax):
            # Slight margin if essentially constant
            margin = 0.1 * (abs(vmin) if vmin != 0 else 1.0)
            vmin -= margin
            vmax += margin
        cmap = plt.get_cmap(cmap_name)
        norm = Normalize(vmin=vmin, vmax=vmax)
        return cmap, norm, np.array([vmin, vmax])

    # We have at least 3 unique edges -> build discrete colormap
    n_colors = edges.size - 1  # one color per interval
    cmap = plt.get_cmap(cmap_name, n_colors)  # discrete version
    norm = BoundaryNorm(edges, n_colors, clip=True)

    return cmap, norm, edges


def prepare_trimmed_quantile_colormap(
    values: np.ndarray,
    n_bins: int = 7,
    cmap_name: str = "Spectral",
    trim_low: float = 0.0,
    trim_high: float = 1.0,
    min_points_for_trim: int = 20,
):
    """
    For a given 1D array of values, build:
      - a mask of points to plot (after optional quantile trimming; default is full-range 0–100%),
      - a quantile-based discrete colormap and norm,
      - and the quantile thresholds used (if trimming applied).
    """
    arr = np.asarray(values, dtype=float)
    finite_mask = np.isfinite(arr)
    n_valid = finite_mask.sum()

    if n_valid == 0:
        return np.zeros_like(arr, dtype=bool), None, None, None, None, None

    # Default: plot all finite values
    mask_plot = finite_mask.copy()
    q_low = None
    q_high = None

    # Apply trimming only if requested (trim_low/high not spanning full 0–100%) and enough points
    if (n_valid >= min_points_for_trim) and ((trim_low > 0.0) or (trim_high < 1.0)):
        finite_vals = arr[finite_mask]
        q_low, q_high = np.quantile(finite_vals, [trim_low, trim_high])

        trim_mask = finite_mask & (arr >= q_low) & (arr <= q_high)
        n_trim = trim_mask.sum()

        # Only accept trimming if at least 2 points remain
        if n_trim >= 2:
            mask_plot = trim_mask
        else:
            # If trimming removes almost everything, revert to no trimming
            q_low = None
            q_high = None

    # Build colormap based on the values that will actually be plotted
    plot_vals = arr[mask_plot]
    cmap, norm, edges = make_quantile_cmap(plot_vals, n_bins=n_bins, cmap_name=cmap_name)

    if cmap is None or norm is None or edges is None:
        return mask_plot, None, None, None, q_low, q_high

    return mask_plot, cmap, norm, edges, q_low, q_high


def get_marker_size(n_points: int) -> float:
    """
    Scatter noktalarının boyutunu (Matplotlib 's', points^2) çizilecek nokta
    sayısına göre belirler.
    """
    if n_points >= 200_000:
        return 0.2
    elif n_points >= 100_000:
        return 0.3
    elif n_points >= 50_000:
        return 0.5
    elif n_points >= 10_000:
        return 1.0
    elif n_points >= 1_000:
        return 4.0
    else:
        return 8.0


def plot_decadal_station_map(
    stn_decade: pd.DataFrame,
    cfg: Config,
    decade: int | None = None,
    excluded_collector: list | None = None,
    region: str = "global",
    world_gdf: gpd.GeoDataFrame | None = None,
):
    """
    Dünya haritasında, her (StnID, Decade) için decadal ortalama yıllık
    anomalinin (mean_anom) dağılımı.

    mean_anom = sum_anom / n_anom
              = ratio_neg + ratio_pos
    """
    if decade is None:
        decade = cfg.demo_decade

    subset = stn_decade[stn_decade["Decade"] == decade].copy()
    if subset.empty:
        print(f"[WARN] No station decadal records for decade {decade}.")
        return

    # Bölge filtresi (global dışı için)
    if region not in REGION_BOUNDS:
        raise ValueError(f"Unknown region key: {region}")
    bounds = REGION_BOUNDS[region]

    if region != "global":
        subset = subset[
            (subset["Lon"] >= bounds["lon_min"]) &
            (subset["Lon"] <= bounds["lon_max"]) &
            (subset["Lat"] >= bounds["lat_min"]) &
            (subset["Lat"] <= bounds["lat_max"])
        ].copy()
        if subset.empty:
            print(f"[INFO] No decadal records for decade {decade} in region {region}.")
            return

    # Decadal ortalama yıllık anomaly (m/year)
    subset["mean_anom"] = subset["sum_anom"] / subset["n_anom"]

    values = subset["mean_anom"].to_numpy()

    # 5–95% trimming + 7 quantile sınıfı
    mask_plot, cmap, norm, edges, q_low, q_high = prepare_trimmed_quantile_colormap(
        values,
        n_bins=7,
        cmap_name="Spectral",
        trim_low=0.0,
        trim_high=1.0,
        min_points_for_trim=20,
    )

    if cmap is None or norm is None or edges is None:
        print(f"[WARN] Could not build quantile colormap for decade {decade}.")
        return

    subset_plot = subset[mask_plot].copy()
    if subset_plot.empty:
        print(f"[WARN] No points left to plot for decade {decade} after trimming.")
        return

    # Trimming uygulandıysa (q_low/q_high None değilse) exclude edilenleri kaydet
    if excluded_collector is not None and q_low is not None and q_high is not None:
        subset_excl = subset[(~mask_plot) & np.isfinite(values)].copy()
        for _, row in subset_excl.iterrows():
            val = float(row["mean_anom"])
            side = "low" if val < q_low else "high"
            excluded_collector.append({
                "MapType": "decadal",
                "Region": region,
                "Decade": int(decade),
                "StnID": row["StnID"],
                "Lat": float(row["Lat"]),
                "Lon": float(row["Lon"]),
                "value": val,
                "side": side,
                "q05_excl_low": float(q_low),
                "q95_excl_high": float(q_high),
            })

    fig, ax = plt.subplots()

    # Çizilecek nokta sayısına göre dinamik nokta boyutu
    n_points = len(subset_plot)
    marker_size = get_marker_size(n_points)

    # US haritalarında noktaları biraz büyüt
    if region == "US":
        marker_size *= 2.0  # gerekirse 1.5 veya 3.0 ile oynayabilirsiniz

    sc = ax.scatter(
        subset_plot["Lon"],
        subset_plot["Lat"],
        c=subset_plot["mean_anom"],
        s=marker_size,
        cmap=cmap,
        norm=norm,
        marker=".",        # daha kompakt marker
        linewidths=0,
        edgecolors="none",
        zorder=2,
    )

    # Basemap: basit vektör dünya haritası (her zaman tüm dünya çizilir, eksen kırpar)
    if world_gdf is not None and not world_gdf.empty:
        world_gdf.plot(
            ax=ax,
            facecolor="none",
            edgecolor="0.3",   # daha koyu
            linewidth=0.7,     # biraz daha kalın
            zorder=1,          # grid'in üstünde, noktaların altında
        )

    # Eksen sınırları
    if region == "global":
        ax.set_xlim(-180, 180)
        ax.set_ylim(-90, 90)
    else:
        ax.set_xlim(bounds["lon_min"], bounds["lon_max"])
        ax.set_ylim(bounds["lat_min"], bounds["lat_max"])

    ax.set_aspect("equal", adjustable="box")

    start_year = int(decade)
    end_year = int(decade) + 9
    ax.text(
        0.99,
        0.99,
        f"{start_year}–{end_year}",
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=8,
        fontweight="bold",
        bbox=dict(facecolor="white", edgecolor="none", alpha=0.6, pad=1.5),
    )

    # ax.set_xlabel("Longitude")
    # ax.set_ylabel("Latitude")

    ax.set_axisbelow(True)
    ax.grid(True, alpha=0.3, zorder=0)

    cbar = fig.colorbar(
        sc,
        ax=ax,
        boundaries=edges,
        spacing="uniform",
        shrink=0.7,
        pad=0.04,
        fraction=0.046,
    )

    if edges.size <= 8:
        tick_edges = edges
    else:
        tick_edges = edges[::2]
    cbar.set_ticks(tick_edges)
    cbar.set_ticklabels([f"{t:,.2f}" for t in tick_edges])

    cbar.ax.tick_params(labelsize=6)
    cbar.set_label("Mean anomaly (m/year) in decade", fontsize=7)

    suffix = "" if region == "global" else f"_{region}"

    decadal_dir = get_map_output_dir(cfg, map_type="decadal", region=region)
    out_path = os.path.join(decadal_dir, f"map_decadal_mean_anom_{decade}s{suffix}.tif")

    safe_save_figure(fig, out_path, dpi=RASTER_DPI, fmt="tiff")

    if getattr(cfg, "save_vector_maps", False):
        root, _ = os.path.splitext(out_path)
        vec_ext = getattr(cfg, "vector_format", "pdf")
        vec_path = root + f".{vec_ext}"
        try:
            fig.savefig(vec_path, format=vec_ext, bbox_inches="tight")
        except Exception as e:
            print(f"[WARN] Could not save vector map {vec_path}: {e}")

    plt.close(fig)


def plot_anomaly_sign_map(
    anoms: pd.DataFrame,
    cfg: Config,
    year: int | None = None,
    limits_collector: list | None = None,
    excluded_collector: list | None = None,
    region: str = "global",
    world_gdf: gpd.GeoDataFrame | None = None,
):
    """
    Belirli bir yıl için yıl-yılına anomalilerin (Level_t - Level_(t-1))
    dünya dağılımı.
    """
    df = anoms.copy()
    if year is None:
        year = df["Year"].max()

    df_year = df[df["Year"] == year].copy()
    if df_year.empty:
        print(f"[WARN] No anomalies for year {year}.")
        return

    if region not in REGION_BOUNDS:
        raise ValueError(f"Unknown region key: {region}")
    bounds = REGION_BOUNDS[region]

    if region != "global":
        df_year = df_year[
            (df_year["Lon"] >= bounds["lon_min"]) &
            (df_year["Lon"] <= bounds["lon_max"]) &
            (df_year["Lat"] >= bounds["lat_min"]) &
            (df_year["Lat"] <= bounds["lat_max"])
        ].copy()
        if df_year.empty:
            print(f"[INFO] No anomalies for year {year} in region {region}.")
            return

    values = df_year["Anomaly"].to_numpy()
    data_min = float(np.nanmin(values))
    data_max = float(np.nanmax(values))

    mask_plot, cmap, norm, edges, q_low, q_high = prepare_trimmed_quantile_colormap(
        values,
        n_bins=7,
        cmap_name="Spectral",
        trim_low=0.0,
        trim_high=1.0,
        min_points_for_trim=20,
    )

    if cmap is None or norm is None or edges is None:
        print(f"[WARN] Could not build quantile colormap for year {year}.")
        return

    df_plot = df_year[mask_plot].copy()
    if df_plot.empty:
        print(f"[WARN] No points left to plot for year {year} after trimming.")
        return

    if excluded_collector is not None and q_low is not None and q_high is not None:
        df_excl = df_year[(~mask_plot) & np.isfinite(values)].copy()
        for _, row in df_excl.iterrows():
            val = float(row["Anomaly"])
            side = "low" if val < q_low else "high"
            excluded_collector.append({
                "MapType": "annual",
                "Region": region,
                "Year": int(year),
                "StnID": row["StnID"],
                "Lat": float(row["Lat"]),
                "Lon": float(row["Lon"]),
                "value": val,
                "side": side,
                "q05_excl_low": float(q_low),
                "q95_excl_high": float(q_high),
            })

    fig, ax = plt.subplots()

    n_points = len(df_plot)
    marker_size = get_marker_size(n_points)

    if region == "US":
        marker_size *= 2.0

    sc = ax.scatter(
        df_plot["Lon"],
        df_plot["Lat"],
        c=df_plot["Anomaly"],
        s=marker_size,
        cmap=cmap,
        norm=norm,
        marker=".",
        linewidths=0,
        edgecolors="none",
        zorder=2,
    )

    if world_gdf is not None and not world_gdf.empty:
        world_gdf.plot(
            ax=ax,
            facecolor="none",
            edgecolor="0.3",
            linewidth=0.7,
            zorder=1,
        )

    if region == "global":
        ax.set_xlim(-180, 180)
        ax.set_ylim(-90, 90)
    else:
        ax.set_xlim(bounds["lon_min"], bounds["lon_max"])
        ax.set_ylim(bounds["lat_min"], bounds["lat_max"])

    ax.set_aspect("equal", adjustable="box")

    ax.text(
        0.99,
        0.99,
        f"{int(year)}",
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=8,
        fontweight="bold",
        bbox=dict(facecolor="white", edgecolor="none", alpha=0.6, pad=1.5),
    )

    # ax.set_xlabel("Longitude")
    # ax.set_ylabel("Latitude")

    ax.set_axisbelow(True)
    ax.grid(True, alpha=0.3, zorder=0)

    cbar = fig.colorbar(
        sc,
        ax=ax,
        boundaries=edges,
        spacing="uniform",
        shrink=0.7,
        pad=0.04,
        fraction=0.046,
    )

    if edges.size <= 8:
        tick_edges = edges
    else:
        tick_edges = edges[::2]
    cbar.set_ticks(tick_edges)
    cbar.set_ticklabels([f"{t:,.2f}" for t in tick_edges])

    cbar.ax.tick_params(labelsize=6)
    cbar.set_label("Anomaly (m/year)", fontsize=7)

    suffix = "" if region == "global" else f"_{region}"

    annual_dir = get_map_output_dir(cfg, map_type="annual", region=region)
    out_path = os.path.join(annual_dir, f"map_anomalies_{year}{suffix}.tif")

    safe_save_figure(fig, out_path, dpi=RASTER_DPI, fmt="tiff")

    if getattr(cfg, "save_vector_maps", False):
        root, _ = os.path.splitext(out_path)
        vec_ext = getattr(cfg, "vector_format", "pdf")
        vec_path = root + f".{vec_ext}"
        try:
            fig.savefig(vec_path, format=vec_ext, bbox_inches="tight")
        except Exception as e:
            print(f"[WARN] Could not save vector map {vec_path}: {e}")

    plt.close(fig)

    if limits_collector is not None:
        rec = {
            "Region": region,
            "Year": int(year),
            "data_min": data_min,
            "data_max": data_max,
            "q_min": float(edges[0]),
            "q_max": float(edges[-1]),
            "n_total": int(len(df_year)),
            "n_plotted": int(len(df_plot)),
        }
        if q_low is not None and q_high is not None:
            rec["q05_excl_low"] = float(q_low)
            rec["q95_excl_high"] = float(q_high)
        limits_collector.append(rec)


# =========================
# MAIN (UPDATED: READ FROM in_anomalies & in_shapefile)
# =========================
def main(cfg: Config = CFG):
    anomalies_path = os.path.join(cfg.anomalies_dir, "station_anomalies_all.csv")
    stn_decade_path = os.path.join(cfg.anomalies_dir, "station_decadal_stats.csv")

    # Handoff convenience:
    # Post-analysis scripts may look for these cache-style tables under ./out_anomalies as well.
    anomalies_out_path = os.path.join(cfg.out_dir, "station_anomalies_all.csv")
    stn_decade_out_path = os.path.join(cfg.out_dir, "station_decadal_stats.csv")

    # 1) Anomalileri yükle veya hesapla
    if os.path.isfile(anomalies_path):
        print("[1/6] Loading pre-computed anomalies from in_anomalies...")
        anoms = pd.read_csv(anomalies_path)
        required_cols = {"StnID", "Year", "Lat", "Lon", "Anomaly"}
        missing = required_cols - set(anoms.columns)
        if missing:
            raise ValueError(f"station_anomalies_all.csv is missing columns: {missing}")
        print(f"   -> {len(anoms):,} anomalies, {anoms['StnID'].nunique():,} stations "
              f"(years {anoms['Year'].min()}–{anoms['Year'].max()})")

        # Cache outputs are mirrored to out_dir for post-analysis handoff

        mirror_cache_files_to_out_dir(cfg, anomalies_path, label="station_anomalies_all")


        # Global uç değerleri kaydet
        detect_and_save_global_outliers(anoms, cfg, quantile=0.999)

    else:
        print("[1/6] Loading level data (no existing anomalies CSV found in in_anomalies)...")
        df_level = load_level_data(cfg)
        print(f"   -> {len(df_level):,} records, {df_level['StnID'].nunique():,} stations, "
              f"years {df_level['Year'].min()}–{df_level['Year'].max()}")

        if getattr(cfg, "qc_enabled", False):
            print("[2/6] Computing year-to-year anomalies with Hybrid QC (source-priority: depth first, elev adds only missing years)...")
        else:
            print("[2/6] Computing year-to-year anomalies (source-priority: depth first, elev adds only missing years)...")

        anoms, flagged = compute_priority_anomalies_depth_then_elev(df_level, cfg)
        print(f"   -> {len(anoms):,} anomalies, {anoms['StnID'].nunique():,} stations")

        if getattr(cfg, "qc_enabled", False):
            flagged_path = os.path.join(cfg.out_dir, "station_flagged_years.csv")
            save_table(flagged, flagged_path, sheet_base="station_flagged_years")
            print(f"   Saved flagged station-years to: {flagged_path} and {os.path.splitext(flagged_path)[0] + '.xlsx'}")


        # Hesaplanan anomalileri in_anomalies klasörüne kaydet (CSV + XLSX)
        save_table(anoms, anomalies_path, sheet_base="station_anomalies")
        print(f"   Saved anomalies to: {anomalies_path} and {os.path.splitext(anomalies_path)[0] + '.xlsx'}")

        # Cache outputs are mirrored to out_dir for post-analysis handoff

        mirror_cache_files_to_out_dir(cfg, anomalies_path, label="station_anomalies_all")


        # Global uç değerleri kaydet
        detect_and_save_global_outliers(anoms, cfg, quantile=0.999)

    # 2) İstasyon bazlı decadal istatistikleri yükle veya hesapla
    if os.path.isfile(stn_decade_path):
        print("[3/6] Loading pre-computed decadal station statistics from in_anomalies...")
        stn_decade = pd.read_csv(stn_decade_path)
        required_cols = {
            "StnID", "Decade", "n_anom", "sum_anom",
            "sum_neg", "sum_pos", "n_neg", "n_pos",
            "Lat", "Lon", "ratio_neg", "ratio_pos", "frac_neg", "frac_pos"
        }
        missing = required_cols - set(stn_decade.columns)
        if missing:
            raise ValueError(f"station_decadal_stats.csv is missing columns: {missing}")
        print(f"   -> {len(stn_decade):,} station-decade records "
              f"(decades {stn_decade['Decade'].min()}–{stn_decade['Decade'].max()})")
        # Cache outputs are mirrored to out_dir for post-analysis handoff
        mirror_cache_files_to_out_dir(cfg, stn_decade_path, label="station_decadal_stats")

    else:
        print("[3/6] Computing decadal station statistics (no existing CSV found in in_anomalies)...")
        try:
            print(f"   [INFO] Decadal station stats will be computed from anomalies: rows={len(anoms):,}, stations={anoms['StnID'].nunique():,}", flush=True)
        except Exception:
            pass

        stn_decade = compute_decadal_station_stats(anoms, cfg)
        save_table(stn_decade, stn_decade_path, sheet_base="station_decadal")
        print(f"   -> {len(stn_decade):,} station-decade records")
        print(f"   Saved station-decadal stats to: {stn_decade_path} and {os.path.splitext(stn_decade_path)[0] + '.xlsx'}")
        # Cache outputs are mirrored to out_dir for post-analysis handoff
        mirror_cache_files_to_out_dir(cfg, stn_decade_path, label="station_decadal_stats")

    # 3) Akifer join + aquifer decadal metrikler
    print("[4/6] Spatial join with aquifer polygons from in_shapefile...")
    aq = load_aquifers(cfg)
    stn_decade_aq = spatial_join_aquifers(stn_decade, aq, cfg)

    stn_decade_aq_path = os.path.join(cfg.out_dir, "station_decadal_stats_with_aquifer.csv")
    save_table(stn_decade_aq, stn_decade_aq_path, sheet_base="stn_decadal_aq")
    print(f"   -> {len(stn_decade_aq):,} station-decade records with aquifer ID")
    print(f"   Saved: {stn_decade_aq_path} and {os.path.splitext(stn_decade_aq_path)[0] + '.xlsx'}")

    print("[5/6] Computing aquifer-level decadal statistics...")
    aq_decade = compute_aquifer_decadal_stats(stn_decade_aq)
    aq_decade_path = os.path.join(cfg.out_dir, "aquifer_decadal_stats.csv")
    save_table(aq_decade, aq_decade_path, sheet_base="aquifer_decadal")
    print(f"   -> {len(aq_decade):,} aquifer-decade records")
    print(f"   Saved: {aq_decade_path} and {os.path.splitext(aq_decade_path)[0] + '.xlsx'}")

    world = None
    if os.path.isfile(cfg.path_world_shp):
        try:
            world = gpd.read_file(cfg.path_world_shp)
            if world.crs is None:
                world = world.set_crs(cfg.data_crs)
            elif world.crs.to_string() != cfg.data_crs:
                world = world.to_crs(cfg.data_crs)
        except Exception as e:
            print(f"[WARN] Could not load world basemap from {cfg.path_world_shp}: {e}")
            world = None
    else:
        print(
            f"[WARN] World basemap shapefile not found at {cfg.path_world_shp}. "
            "Download a Natural Earth 'admin 0 countries' shapefile and update this path if needed."
        )

    # 4) Tüm onyıllar ve yıllar için haritalar (çıktılar yine out_anomalies içine)
    print("[6/6] Plotting maps for all decades and years...")

    regions_for_maps = ["global", "US", "Europe"]

    # --- Onyıllık haritalar ---
    decadal_excluded = []
    all_decades = sorted(stn_decade_aq["Decade"].dropna().unique())

    for reg in regions_for_maps:
        desc = f"Decadal maps ({reg})"
        with tqdm(all_decades, desc=desc, unit="decade") as pbar:
            for dec in pbar:
                pbar.set_postfix(decade=f"{int(dec)}s")
                plot_decadal_station_map(
                    stn_decade_aq,
                    cfg,
                    decade=int(dec),
                    excluded_collector=decadal_excluded,
                    region=reg,
                    world_gdf=world,
                )

    # --- Yıllık anomaly haritaları + aralık ve excluded kayıtları ---
    annual_limits = []
    annual_excluded = []
    all_years = sorted(anoms["Year"].dropna().unique())

    for reg in regions_for_maps:
        desc = f"Yearly anomaly maps ({reg})"
        with tqdm(all_years, desc=desc, unit="year") as pbar:
            for yr in pbar:
                pbar.set_postfix(year=int(yr))
                plot_anomaly_sign_map(
                    anoms,
                    cfg,
                    year=int(yr),
                    limits_collector=annual_limits,
                    excluded_collector=annual_excluded,
                    region=reg,
                    world_gdf=world,
                )

    # Yıllık min/max ve trimming aralıklarını CSV+XLSX'ye yaz
    annual_limits_path = os.path.join(cfg.out_dir, "annual_max_min_anomalies.csv")
    save_table(pd.DataFrame(annual_limits), annual_limits_path, sheet_base="annual_limits")
    print(f"   Saved annual anomaly ranges to: {annual_limits_path} and {os.path.splitext(annual_limits_path)[0] + '.xlsx'}")

    # Excluded noktaları CSV+XLSX'lere yaz
    if decadal_excluded:
        dec_excl_path = os.path.join(cfg.out_dir, "decadal_excluded_p0_p100_mean_anomalies.csv")
        save_table(pd.DataFrame(decadal_excluded), dec_excl_path, sheet_base="decadal_excluded")
        print(f"   Saved decadal excluded extremes to: {dec_excl_path} and {os.path.splitext(dec_excl_path)[0] + '.xlsx'}")

    if annual_excluded:
        ann_excl_path = os.path.join(cfg.out_dir, "annual_excluded_p0_p100_anomalies.csv")
        save_table(pd.DataFrame(annual_excluded), ann_excl_path, sheet_base="annual_excluded")
        print(f"   Saved annual excluded extremes to: {ann_excl_path} and {os.path.splitext(ann_excl_path)[0] + '.xlsx'}")

    print("Done.")


if __name__ == "__main__":
    main(CFG)
