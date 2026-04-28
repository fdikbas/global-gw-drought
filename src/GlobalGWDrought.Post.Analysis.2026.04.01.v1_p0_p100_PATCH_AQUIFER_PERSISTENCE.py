"""
GlobalGWDrought – Post Analysis (network coverage, trends, persistence, diagnostics)

Purpose
-------
This script performs the “post-analysis” layer of the GlobalGWDrought workflow using the outputs
produced by the upstream anomaly/QC (HybridQC) pipeline. It generates publication-ready figures,
summary tables, and diagnostics that support the Results/Discussion sections (e.g., network coverage,
aquifer-scale trends, persistence, asymmetry, trimming/outlier diagnostics, and optional station-level
analyses).

Single-source-of-truth design (to prevent transition/pipeline mismatches)
------------------------------------------------------------------------
The primary objective of this version is to eliminate “handoff errors” between scripts by defining
a clear and stable data interface.

This post-analysis script is designed to rely on TWO upstream CSV files as the canonical inputs:

(1) station_anomalies_all.csv  (STATION × YEAR anomalies; primary input)
    - Default location:
        ./out_anomalies/station_anomalies_all.csv
      (resolved via cfg.anomalies_dir / cfg.station_anomalies_file)
    - Produced by:
        GlobalGWDrought.Anomaly.Calculation.HybridQC.*.py
    - Required columns in the upstream file:
        StnID, Year, Lat, Lon, Anomaly
    - Internal normalization performed by this script:
        Anomaly → anomaly (float)
        Region  derived from Lon/Lat (US / Europe / Global)
    - Meaning:
        Annual anomaly is computed only for consecutive years:
            Anomaly(t) = Level(t) - Level(t-1)   [m yr^-1]
      where Level is a unified “wetness-like” groundwater level (defined upstream).

(2) station_decadal_stats.csv  (STATION × DECADE summaries; decadal aggregation)
    - Default location:
        ./out_anomalies/station_decadal_stats.csv
      (resolved via cfg.anomalies_dir / cfg.decadal_stats_file)
    - Produced by:
        GlobalGWDrought.Anomaly.Calculation.HybridQC.*.py
    - Minimum expected columns:
        StnID, Decade, Lat, Lon, n_anom, sum_anom
      Optional (if available):
        sum_neg, sum_pos, ratio_neg/ratio_pos, frac_neg/frac_pos, etc.

Optional / legacy input (not required for the main workflow)
-----------------------------------------------------------
(3) annual_max_min_anomalies.csv  (annual min–max and trimming stats; legacy artifact)
    - Typical legacy location:
        ./out_anomalies/annual_max_min_anomalies.csv (cfg.annual_stats_file)
    - In this version, annual statistics are preferably DERIVED IN-MEMORY from station_anomalies_all.csv
      to ensure full consistency with the QC-filtered anomaly set.
    - Output write policy is controlled via:
        cfg.annual_stats_write_mode = "never" | "if_missing" | "always"
      Recommended default: "never"
      Explanation:
        * "never"      → do not write/overwrite annual_max_min_anomalies each run.
        * "if_missing" → write once if the file does not exist; otherwise keep it unchanged.
        * "always"     → regenerate and overwrite on every run.
      This prevents repeated regeneration when not needed, while still allowing reproducibility if desired.

Region assignment (US / Europe / Global)
----------------------------------------
- Region tags are derived from Lon/Lat using a simple bounding-box logic.
- Outputs typically include:
    * Global: all records (worldwide)
    * US: subset within the US bounding box
    * Europe: subset within the Europe bounding box
- Some summaries also report “Other world” as a complement group (depending on the section).

Aquifer ID attachment (AQUIFER_ID)
----------------------------------
- station_decadal_stats.csv may not contain AQUIFER_ID.
- If missing, this script attempts to attach aquifer IDs by spatially joining station points to an aquifer
  polygon shapefile (point-in-polygon).
- Controlled via:
    cfg.aquifer_shapefile
- If the shapefile is missing or cannot be read, aquifer-level analyses are safely skipped and the script
  continues (with warnings), rather than failing silently or producing inconsistent results.

Core outputs (where files go)
-----------------------------
- Numerical outputs are written to:
    cfg.anomalies_dir  (default: ./out_anomalies)
  Examples (names vary by section):
    - aquifer_trend_summary*.csv
    - aquifer_negative_hotspots*.csv
    - aquifer_asymmetry_summary*.csv
    - station_regime_clusters.csv
    - station_drought_metrics_from_annual_anomaly.csv
    - station_trends_from_annual_anomaly_MK*.csv
    - trimming/outlier diagnostics and station repeated-outlier summaries

- Figures (publication-ready) are written to:
    cfg.out_fig_dir  (default: ./out_figs_network)
  The figure resolution/format is controlled by Config parameters (e.g., cfg.fig_dpi, raster/vector options).

Summary log for manuscript/QA
-----------------------------
- Key console summaries are additionally saved to:
    ./out_anomalies/post_analysis_summary.txt (cfg.summary_log_file)
  This provides a persistent record of counts, parameter settings, and headline results for manuscript writing
  and reproducibility.

Basemap dependencies and graceful degradation
---------------------------------------------
- cartopy is optional; if unavailable, map plotting is reduced accordingly.
- A Natural Earth world boundary shapefile may be used if provided:
    cfg.path_world_shp (e.g., ne_110m_admin_0_countries.shp)
  If missing, the script issues warnings but continues.

Execution notes
---------------
- Typical usage: run the script from Spyder (runfile) or the command line (python script.py).
- The most critical settings are:
    * cfg.anomalies_dir (must contain station_anomalies_all.csv and station_decadal_stats.csv)
    * cfg.out_fig_dir
    * cfg.aquifer_shapefile (only required for aquifer-level spatial steps)
- If upstream column names differ from the expected schema, the script will raise explicit errors to avoid
  producing partially incorrect results.

----------------------------------------------------------------------------------------------------------

[TURKISH]
Amaç
-----
Bu betik, anomali/QC (HybridQC) aşamasının çıktıları üzerinden “post-analysis” üretir:
- Ağ kapsaması (zaman içinde istasyon ve akifer kapsamı),
- Yıllık anomali aralığı evrimi,
- Akifer ölçeğinde (decadal) trend/dağılım haritaları ve özet tablolar,
- Çok dönemli trend analizi (ör. 1900–1959, 1960–1989, 1990–2020),
- Asimetri (pozitif/negatif ağırlık) ve persistens (negatif run uzunlukları) metrikleri,
- Kırpma (trimming) ve outlier tanılama özetleri,
- (Opsiyonel) istasyon rejim sınıflandırması ve istasyon trendleri gibi ek çıktılar.

Bu post-analysis betiğinin “tek doğru giriş seti” yaklaşımı
-----------------------------------------------------------
Bu sürüm, ana akışı iki temel upstream CSV çıktısına dayandıracak şekilde tasarlanmıştır:

(1) station_anomalies_all.csv  (STATION–YEAR ANOMALIES; birincil kaynak)
    - Konum:  ./out_anomalies/station_anomalies_all.csv  (cfg.anomalies_dir / cfg.station_anomalies_file)
    - Üreten betik: anomali/QC pipeline (HybridQC)
    - Gerekli kolonlar (ham dosyada):
        StnID, Year, Lat, Lon, Anomaly
    - Bu betik içinde standartlaştırılan kolonlar:
        anomaly (float), Region ('US' | 'Europe' | 'Global'), vb.
    - Anlam:
        "Anomaly" = sadece ardışık yıllar için (Level_t - Level_{t-1}) yıllık değişim (m yr^-1).

(2) station_decadal_stats.csv  (STATION–DECADE STATS; decadal agregasyon)
    - Konum: ./out_anomalies/station_decadal_stats.csv  (cfg.anomalies_dir / cfg.decadal_stats_file)
    - Üreten betik: anomali/QC pipeline (HybridQC)
    - Beklenen içerik (minimum):
        StnID, Decade, Lat, Lon, n_anom, sum_anom
      (varsa) sum_neg, sum_pos, ratio_neg/pos, frac_neg/pos, vb.

Opsiyonel/legacy girdi:
-----------------------
(3) annual_max_min_anomalies.csv  (Yıllık min–max ve trimming istatistikleri; legacy)
    - Konum: ./out_anomalies/annual_max_min_anomalies.csv (cfg.annual_stats_file)
    - Bu betik, varsayılan olarak bu dosyayı DOĞRUDAN okumak yerine,
      station_anomalies_all.csv’den yıllık istatistikleri bellekte TÜRETİR.
      Bunun amacı, upstream QC/filtreleme ile tam tutarlılık sağlamaktır.

    - Yazma davranışı kontrolü (kritik):
        cfg.annual_stats_write_mode = "never" | "if_missing" | "always"
      Varsayılan: "never"
      Açıklama:
        * "never"      → annual_max_min_anomalies.csv her çalıştırmada üretilmez/üzerine yazılmaz.
        * "if_missing" → dosya yoksa üretilir (varsa dokunulmaz).
        * "always"     → her çalıştırmada yeniden yazılır.
      Not: Bu parametre, “her çalıştırmada annual dosyası oluşmasın” tercihine hizmet eder.

Bölge etiketleme (Region)
-------------------------
- Region, Lon/Lat bounding-box ile atanır:
    US / Europe / Other world (bazı özetlerde)
  station_anomalies_all.csv okunurken istasyonlara 'US'|'Europe'|'Global' etiketi verilir.
- Yıllık özetlerde:
    'global' = TÜM satırlar (bölge etiketinden bağımsız) üzerinden agregasyon,
    'US' ve 'Europe' = ilgili region alt-kümeleri üzerinden agregasyon.

Akifer ID (AQUIFER_ID) bağlama
------------------------------
- station_decadal_stats.csv içinde AQUIFER_ID yoksa, betik nokta-içinde-poligon (point-in-polygon)
  mekânsal join ile AQUIFER_ID eklemeye çalışır.
- cfg.aquifer_shapefile ayarı ile shapefile yolu verilir.
- Uyum amacı:
  AQUIFER_ID, shapefile özellik indeksine (0..n-1) eşit olacak şekilde ele alınır (bazı harita üretim
  fonksiyonları poligonları bu indeks üzerinden okuyabilir).
- Shapefile yoksa/boşsa: AQUIFER_ID NaN kalır; akifer-düzeyi analizler güvenli şekilde atlanır.

Başlıca çıktı klasörleri
------------------------
- cfg.anomalies_dir = "./out_anomalies"
    Sayısal çıktıların (CSV; bazıları opsiyonel XLSX yan-dosyalar) ve özet logun tutulduğu yer.
- cfg.out_fig_dir = "./out_figs_network"
    Makale kalitesinde şekillerin kaydedildiği yer (cfg.fig_dpi, cfg.raster_format, cfg.save_vector).

Özet log
---------
- Konsoldaki önemli özet blokları ayrıca şu dosyaya eklenir:
    ./out_anomalies/post_analysis_summary.txt (cfg.summary_log_file)
  Bu dosya, makale yazımında doğrudan “sayısal bulguların metin kanıtı” gibi kullanılabilir.

Harita altlığı (basemap) ve bağımlılıklar
-----------------------------------------
- cartopy opsiyoneldir; kurulu değilse betik HAS_CARTOPY=False ile bazı haritaları sınırlı biçimde üretir.
- World basemap shapefile yolu (Natural Earth admin_0 countries) cfg.path_world_shp ile yönetilir.
  Dosya bulunamazsa betik uyarı verip çalışmaya devam edecek şekilde kurgulanmıştır.

Çalıştırma notları
------------------
- Tipik kullanım: Spyder içinde runfile(...), veya doğrudan python ile çalıştırma.
- En kritik ayarlar: Config içindeki anomalies_dir, out_fig_dir ve dosya adlarıdır.
- Eğer input dosyaların kolon isimleri farklı ise:
  station_anomalies_all.csv için "StnID, Year, Lat, Lon, Anomaly";
  station_decadal_stats.csv için en az "StnID, Decade, Lat, Lon, n_anom, sum_anom"
  sağlanmalıdır (aksi halde betik bilinçli olarak hata vererek durur).


Created on Tue Nov 18 13:51:25 2025

@author: Fatih DİKBAŞ
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from math import sqrt, erfc
from typing import Any

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib as mpl
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd
import geopandas as gpd
import shapefile  # for reading aquifer polygons by index (F3 map)

try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    HAS_CARTOPY = True
except ImportError:
    HAS_CARTOPY = False


# Colour palette tuned for journal-quality, colour-blind-friendly figures

# Global network coverage (F1) – lines + darker markers
COLOR_GLOBAL_STATION = "#1b9e77"               # teal (line)
COLOR_GLOBAL_STATION_MARKER = "#0f5c46"        # darker teal (markers)

COLOR_GLOBAL_DECADAL_STATION = "#7570b3"       # purple (line)
COLOR_GLOBAL_DECADAL_STATION_MARKER = "#4b3c7c"  # darker purple (markers)

COLOR_GLOBAL_AQUIFER = "#d95f02"               # orange (line)
COLOR_GLOBAL_AQUIFER_MARKER = "#994008"        # darker orange (markers)

# Regional annual coverage figure (if used)
COLOR_REGION_GLOBAL = "#1b9e77"           # teal
COLOR_REGION_US = "#7570b3"               # purple
COLOR_REGION_EUROPE = "#d95f02"           # orange

# Colour palette for anomaly range figure (journal-friendly, colour-blind aware)
COLOR_ANOM_FULL_RANGE = "#b3cde3"      # light blue – full (min–max) range
COLOR_ANOM_TRIM_RANGE = "#2b8cbe"      # medium blue – trimmed (q05–q95) range
COLOR_ANOM_MEDIAN_LINE = "#011f4b"     # dark blue – median anomaly line (optional)

# Aquifer trend palette (F3–F4)
COLOR_TREND_NEG = "#d73027"        # strong red – significant negative trend
COLOR_TREND_POS = "#4575b4"        # strong blue – significant positive trend
COLOR_TREND_NS = "#bdbdbd"         # grey – no significant trend
COLOR_TREND_OUTLINE = "#404040"    # dark outline for markers

# Station regime cluster colors (max 8 cluster için yeterli)
COLOR_REGIME_CLUSTERS = [
    "#d73027",  # Cluster 1
    "#4575b4",  # Cluster 2
    "#1a9850",  # Cluster 3
    "#984ea3",  # Cluster 4
    "#fdae61",  # Cluster 5
    "#ffff33",  # Cluster 6
    "#a6cee3",  # Cluster 7
    "#fb9a99",  # Cluster 8
]

REGION_BOUNDS = {
    "global": {
        "lon_min": -180.0,
        "lon_max": 180.0,
        "lat_min": -60.0,
        "lat_max": 80.0,
    },
    "US": {
        "lon_min": -130.0,
        "lon_max": -60.0,
        "lat_min": 20.0,
        "lat_max": 55.0,
    },
    "Europe": {
        "lon_min": -15.0,
        "lon_max": 35.0,
        "lat_min": 30.0,
        "lat_max": 72.0,
    },
}

# Çok dönemli trend analizi için varsayılan dönemler (decadal merkez yıllarına göre)
# Siz isterseniz bu sınırları rahatça değişebilirsiniz.
MULTI_TREND_PERIODS = [
    ("1900–1959", 1900, 1959),
    ("1960–1989", 1960, 1989),
    ("1990–2020", 1990, 2020),
]

def _region_from_lonlat(lon: float, lat: float) -> str:
    """
    Basit Lon/Lat sınırlarına göre istasyonu/ölçümü bölgelere atar.
    'US', 'Europe' veya 'Other world' döndürür.
    """
    try:
        lon = float(lon)
        lat = float(lat)
    except (TypeError, ValueError):
        return "Other world"

    if not np.isfinite(lon) or not np.isfinite(lat):
        return "Other world"

    us = REGION_BOUNDS["US"]
    eu = REGION_BOUNDS["Europe"]

    if (
        us["lon_min"] <= lon <= us["lon_max"]
        and us["lat_min"] <= lat <= us["lat_max"]
    ):
        return "US"

    if (
        eu["lon_min"] <= lon <= eu["lon_max"]
        and eu["lat_min"] <= lat <= eu["lat_max"]
    ):
        return "Europe"

    return "Other world"


@dataclass
class Config:
    """
    Configuration for the global groundwater network coverage / post-analysis.
    """

    # --- Genel klasörler ---
    # Ana pipeline'ın anomaly CSV çıktılarının bulunduğu klasör
    anomalies_dir: str = r"./out_anomalies"

    # Bu post-analysis scriptinin figür çıktılarının kaydedileceği klasör
    out_fig_dir: str = r"./out_figs_network"

    # Konsol özetlerinin yazıldığı log dosyası
    summary_log_file: str = "post_analysis_summary.txt"

    # --- Ana pipeline çıktıları (CSV) ---
    # Yıllık anomaly aralıkları ve istasyon sayıları
    annual_stats_file: str = "annual_max_min_anomalies.csv"
    # Station-year anomalies (recommended primary input for post-analysis)
    station_anomalies_file: str = "station_anomalies_all.csv"

    # Prefer deriving annual stats from station_anomalies_all.csv (ensures consistency with QC)
    prefer_station_anomalies_for_annual_stats: bool = True

    # Write derived annual stats file (annual_max_min_anomalies.csv): "never" | "if_missing" | "always"
    annual_stats_write_mode: str = "never"
    # İstasyon–onyıl istatistikleri (akifer ID dahil)
    decadal_stats_file: str = "station_decadal_stats.csv"
    # Akifer–onyıl istatistikleri (trend/asimetri/persistens için)
    aquifer_decadal_stats_file: str = "aquifer_decadal_stats.csv"

    # --- Trimming / outlier dosyaları ---
    # 5–95% trimming ile dışlanan yıllık noktalar
    annual_excluded_file: str = "annual_excluded_p5_p95_anomalies.csv"
    # 5–95% trimming ile dışlanan onyıllık ortalama anomaly değerleri
    decadal_excluded_file: str = "decadal_excluded_p5_p95_mean_anomalies.csv"

    # Global outlier kayıtları (ana pipeline tarafından üretilen)
    global_outlier_anomalies_file: str = "global_outlier_anomalies.csv"

    # İstasyon bazlı tekrar eden outlier özeti (5.2)
    repeated_outlier_summary_file: str = "station_repeated_outliers_summary.csv"

    # Akifer trend özet dosyası (genel)
    aquifer_trend_summary_file: str = "aquifer_trend_summary.csv"

    # --- Şekil altlıkları (shapefile’lar) ---
    # Dünya altlık haritası (Natural Earth admin_0 countries, EPSG:4326)
    path_world_shp: str = r"./in_shapefile/ne_110m_admin_0_countries.shp"

    # Jasechko et al. (2024) akifer shapefile’ı; AQUIFER_ID ile eşleşiyor
    aquifer_shapefile: str | None = r"./in_shapefile/jasechko_et_al_2024_aquifers.shp"

    # --- Şekil çıktı ayarları ---
    fig_dpi: int = 600               # yüksek çözünürlük (makale için)
    raster_format: str = "tif"       # raster format (TIFF)
    save_vector: bool = True         # ek olarak vektör çıktı kaydedilsin mi?
    vector_format: str = "pdf"       # vektör formatı (PDF)

    # --- Trend analizi ayarları ---
    trend_min_stations_per_decade: int = 3    # bir onyıla girecek min istasyon sayısı
    trend_min_decades_per_aquifer: int = 3    # bir akifer için min onyıl sayısı
    trend_alpha: float = 0.05                 # Mann–Kendall anlamlılık düzeyi

    # --- Negatif eğim hotspot (küme) analizi ---
    hotspot_grid_size_deg: float = 5.0        # grid hücre boyutu (derece)
    hotspot_min_count: int = 5                # hücrede min. akifer sayısı
    hotspot_abs_slope_quantile: float = 0.75  # |slope| için quantile eşiği

    # --- İstasyon rejim sınıflandırması (2.2, F5) ---
    station_regime_min_decades: int = 3       # bir istasyon için min onyıl sayısı
    station_regime_n_clusters: int = 4        # K-means küme sayısı
    station_regime_random_state: int = 42     # tekrarlanabilirlik için seed
    station_regime_max_iter: int = 100        # K-means max iterasyon

    # 2.2 Station regime classification (F5)
    station_regime_min_decades: int = 6
    station_regime_n_clusters: int = 4


def ensure_dir(path: str) -> None:
    """Create directory if it does not exist."""
    os.makedirs(path, exist_ok=True)


def append_to_summary_log(cfg: Config, text: str) -> None:
    """
    Konsola basılan önemli özet bloklarını ayrıca bir metin dosyasına da
    ekler. Makale yazarken bu dosya doğrudan referans olarak kullanılabilir.

    Dosya yolu: os.path.join(cfg.anomalies_dir, cfg.summary_log_file)
    """
    ensure_dir(cfg.anomalies_dir)
    path = os.path.join(cfg.anomalies_dir, cfg.summary_log_file)

    # Tek noktadan yönetebilmek için, her çağrıda bir boş satır da ekleyelim
    with open(path, "a", encoding="utf-8") as f:
        f.write(text)
        if not text.endswith("\n"):
            f.write("\n")
        f.write("\n")


def load_station_anomalies_all(cfg: Config) -> pd.DataFrame:
    """
    Load station–year anomalies produced by the anomaly-calculation pipeline.

    Expected input file (CSV):
        cfg.anomalies_dir / cfg.station_anomalies_file
        Default: ./out_anomalies/station_anomalies_all.csv

    Required columns:
        - StnID
        - Year
        - Lat
        - Lon
        - Anomaly   (year-to-year Level change, m yr^-1; computed only for consecutive years)

    Output columns (standardized):
        - StnID (string)
        - Year (int)
        - Lat, Lon (float)
        - anomaly (float)
        - Region (categorical station label: 'US' | 'Europe' | 'Global')
    """
    path = os.path.join(cfg.anomalies_dir, cfg.station_anomalies_file)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Station anomalies file not found: {path}")

    df = pd.read_csv(path)

    required = {"StnID", "Year", "Lat", "Lon", "Anomaly"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{cfg.station_anomalies_file} is missing required columns: {sorted(missing)}"
        )

    df = df[["StnID", "Year", "Lat", "Lon", "Anomaly"]].copy()

    # Types and NaN handling
    df["StnID"] = df["StnID"].astype(str)
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    df["Lat"] = pd.to_numeric(df["Lat"], errors="coerce")
    df["Lon"] = pd.to_numeric(df["Lon"], errors="coerce")
    df["anomaly"] = pd.to_numeric(df["Anomaly"], errors="coerce")
    df = df.dropna(subset=["Year", "Lat", "Lon", "anomaly"]).copy()
    df["Year"] = df["Year"].astype(int)

    # Station region label (used for regional summaries; global aggregates use all rows)
    reg = np.full(len(df), "Global", dtype=object)
    b_us = REGION_BOUNDS["US"]
    b_eu = REGION_BOUNDS["Europe"]

    lon = df["Lon"].to_numpy(dtype=float)
    lat = df["Lat"].to_numpy(dtype=float)

    m_us = (
        (lon >= b_us["lon_min"])
        & (lon <= b_us["lon_max"])
        & (lat >= b_us["lat_min"])
        & (lat <= b_us["lat_max"])
    )
    m_eu = (
        (lon >= b_eu["lon_min"])
        & (lon <= b_eu["lon_max"])
        & (lat >= b_eu["lat_min"])
        & (lat <= b_eu["lat_max"])
    )

    reg[m_us] = "US"
    reg[~m_us & m_eu] = "Europe"
    df["Region"] = reg

    df = df.sort_values(["StnID", "Year"]).reset_index(drop=True)
    return df


def compute_annual_stats_from_station_anomalies(
    station_anoms: pd.DataFrame,
    # trim_low: float = 0.05,
    # trim_high: float = 0.95,
    trim_low: float = 0.0,
    trim_high: float = 1.0,
    min_points_for_trim: int = 20,
) -> pd.DataFrame:
    """
    Derive annual anomaly-range statistics from station-year anomalies.

    Produces the same core fields expected by the post-analysis figures:
        - Year
        - data_min, data_max (raw min/max anomaly)
        - q_min, q_max       (trim_low–trim_high quantiles; falls back to raw if sample is small)
        - median             (annual median anomaly)
        - n_total            (annual sample size)
        - Region             ('global', 'US', 'Europe')

    Notes
    -----
    * 'global' is an aggregation over ALL station anomalies (irrespective of
      the station's Region label).
    * 'US' and 'Europe' are aggregations over stations within REGION_BOUNDS.
    """
    if station_anoms is None or station_anoms.empty:
        return pd.DataFrame(
            columns=[
                "Year",
                "data_min",
                "data_max",
                "q_min",
                "q_max",
                "median",
                "n_total",
                "Region",
            ]
        )

    df = station_anoms.copy()
    df = df[np.isfinite(df["anomaly"].to_numpy(dtype=float))].copy()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Year",
                "data_min",
                "data_max",
                "q_min",
                "q_max",
                "median",
                "n_total",
                "Region",
            ]
        )

    def _annual_agg(x: pd.Series) -> dict:
        vals = x.to_numpy(dtype=float)
        vals = vals[np.isfinite(vals)]
        n = int(vals.size)
        if n == 0:
            return {
                "data_min": np.nan,
                "data_max": np.nan,
                "q_min": np.nan,
                "q_max": np.nan,
                "median": np.nan,
                "n_total": 0,
            }

        data_min = float(np.nanmin(vals))
        data_max = float(np.nanmax(vals))
        median = float(np.nanmedian(vals))

        if n >= int(min_points_for_trim):
            qmin, qmax = np.quantile(vals, [trim_low, trim_high])
            qmin = float(qmin)
            qmax = float(qmax)
        else:
            qmin, qmax = data_min, data_max

        return {
            "data_min": data_min,
            "data_max": data_max,
            "q_min": qmin,
            "q_max": qmax,
            "median": median,
            "n_total": n,
        }

    out_frames = []

    # --- Global (all rows) ---
    g = df.groupby("Year")["anomaly"].apply(_annual_agg)
    global_df = pd.DataFrame(list(g.values), index=g.index).reset_index()
    global_df["Region"] = "global"
    out_frames.append(global_df)

    # --- US / Europe (subsets by station label) ---
    for reg in ["US", "Europe"]:
        sub = df[df["Region"] == reg].copy()
        if sub.empty:
            continue
        g = sub.groupby("Year")["anomaly"].apply(_annual_agg)
        reg_df = pd.DataFrame(list(g.values), index=g.index).reset_index()
        reg_df["Region"] = reg
        out_frames.append(reg_df)

    out = pd.concat(out_frames, ignore_index=True)
    out = out.sort_values(["Year", "Region"]).reset_index(drop=True)
    return out



def normalize_annual_stats_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize annual anomaly-statistics tables to the canonical *wide* schema:

        Year, Region, data_min, data_max, q_min, q_max, median, n_total

    This solver exists for backward-compatibility with legacy/intermediate outputs
    that were saved in a *long* (stacked) format such as:

        Year, Region, level_1, 0
        1901, global, data_min, -12.3
        1901, global, data_max,  14.7
        ...

    It is intentionally permissive and focuses on producing a usable table for
    downstream plotting and network-coverage counts.
    """
    if df is None or df.empty:
        return pd.DataFrame(
            columns=["Year", "Region", "data_min", "data_max", "q_min", "q_max", "median", "n_total"]
        )

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    # Ensure Region exists
    if "Region" not in out.columns:
        out["Region"] = "global"

    # --- Long formats: (Year, Region, level_1, value) -> pivot to wide ---
    long_metric_cols = {"level_1", "metric", "stat", "variable"}
    value_cols = ["0", "value", "Value", "val", "anomaly"]

    metric_col = next((c for c in value_cols), None)
    # metric name column
    mcol = next((c for c in long_metric_cols if c in out.columns), None)
    vcol = next((c for c in value_cols if c in out.columns), None)

    if mcol is not None and vcol is not None and "Year" in out.columns:
        piv = out.pivot_table(
            index=["Year", "Region"],
            columns=mcol,
            values=vcol,
            aggfunc="first",
        ).reset_index()
        piv.columns.name = None
        out = piv
        out.columns = [str(c).strip() for c in out.columns]

    # --- Common rename harmonization ---
    rename_map = {
        "min": "data_min",
        "max": "data_max",
        "p05": "q_min",
        "p95": "q_max",
        "q05": "q_min",
        "q95": "q_max",
        "q_05": "q_min",
        "q_95": "q_max",
        "n": "n_total",
        "count": "n_total",
        "n_station": "n_total",
        "n_stations": "n_total",
        "n_total_station": "n_total",
    }
    for k, v in list(rename_map.items()):
        if k in out.columns and v not in out.columns:
            out = out.rename(columns={k: v})

    # Ensure required columns exist (median can be optional, but we prefer present)
    required = {"Year", "data_min", "data_max", "q_min", "q_max", "n_total"}
    missing = required - set(out.columns)
    if missing:
        raise ValueError(
            "Annual stats table could not be normalized to the required schema. "
            f"Missing columns after normalization: {sorted(missing)}. "
            f"Available columns: {list(out.columns)}"
        )

    if "median" not in out.columns:
        out["median"] = np.nan

    # Type enforcement
    out["Year"] = pd.to_numeric(out["Year"], errors="coerce").astype("Int64")
    out = out.dropna(subset=["Year"]).copy()
    out["Year"] = out["Year"].astype(int)

    for c in ["data_min", "data_max", "q_min", "q_max", "median", "n_total"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    # Normalize Region labels
    out["Region"] = out["Region"].astype(str)
    out.loc[out["Region"].str.lower().isin(["global", "all"]), "Region"] = "global"

    out = out.sort_values(["Year", "Region"]).reset_index(drop=True)
    return out



def load_annual_stats(cfg: Config) -> pd.DataFrame:
    """
    Load or derive annual anomaly-range and count statistics used by the post-analysis.

    Preferred (default) route:
        station_anomalies_all.csv  ->  derive annual stats in memory

    Backward-compatible route:
        annual_max_min_anomalies.csv -> read directly (legacy pipeline output)

    Writing behaviour for derived annual stats is controlled by:
        cfg.annual_stats_write_mode = "never" | "if_missing" | "always"
    """
    station_path = os.path.join(cfg.anomalies_dir, cfg.station_anomalies_file)
    annual_path = os.path.join(cfg.anomalies_dir, cfg.annual_stats_file)

    use_station = bool(cfg.prefer_station_anomalies_for_annual_stats) and os.path.exists(
        station_path
    )

    if use_station:
        print(f"   -> Using station anomalies as primary input for annual stats: {station_path}")
    else:
        print(f"   -> Station anomalies not found or disabled; falling back to legacy annual stats (if present): {annual_path}")

    if use_station:
        stn = load_station_anomalies_all(cfg)
        annual_df = compute_annual_stats_from_station_anomalies(stn)

        # Normalize/validate (robust against legacy/stacked formats)
        annual_df = normalize_annual_stats_table(annual_df)

        mode = str(getattr(cfg, "annual_stats_write_mode", "if_missing")).strip().lower()
        if mode not in {"never", "if_missing", "always"}:
            print(
                f"[WARN] Unknown annual_stats_write_mode='{mode}'. "
                "Falling back to 'if_missing'."
            )
            mode = "if_missing"

        should_write = (mode == "always") or (mode == "if_missing" and not os.path.exists(annual_path))
        if should_write:
            try:
                annual_df.to_csv(annual_path, index=False)
                # Optional Excel sidecar (best-effort, do not fail run)
                xlsx_path = os.path.splitext(annual_path)[0] + ".xlsx"
                try:
                    annual_df.to_excel(xlsx_path, index=False)
                except Exception as e:
                    print(f"[WARN] Could not write Excel annual stats: {e}")
            except Exception as e:
                print(f"[WARN] Could not write derived annual stats CSV: {e}")

        return annual_df

    # --- Legacy: read annual_max_min_anomalies.csv directly ---
    if not os.path.exists(annual_path):
        raise FileNotFoundError(
            "Neither station anomalies nor annual stats file could be found. "
            f"Missing: {station_path} and {annual_path}"
        )

    df = pd.read_csv(annual_path)

    # Normalize legacy/stacked annual stats tables to the canonical schema
    df = normalize_annual_stats_table(df)
    return df


def _attach_aquifer_ids_to_station_decadal(
    station_decadal_df: pd.DataFrame, cfg: Config
) -> pd.DataFrame:
    """
    Attach AQUIFER_ID to station_decadal_stats using a point-in-polygon join.

    Notes
    -----
    - This post-analysis script treats AQUIFER_ID as the *feature index* of the
      aquifer shapefile (0..n-1) to stay compatible with downstream functions
      that read polygons via `shapefile.Reader` using this index.
    - To avoid a very expensive join on station×decade rows, the join is done on
      unique stations (StnID, Lon, Lat) and then merged back.
    """
    df = station_decadal_df.copy()

    # Always ensure the column exists
    if "AQUIFER_ID" not in df.columns:
        df["AQUIFER_ID"] = np.nan

    shp_path = getattr(cfg, "aquifer_shapefile", None)
    if (shp_path is None) or (not os.path.exists(shp_path)):
        print(
            "   ! Aquifer shapefile is not available; AQUIFER_ID will remain NaN "
            "and aquifer-level analyses will be skipped."
        )
        return df

    # Unique station coordinates (deduplicate by StnID)
    cols = [c for c in ["StnID", "Lon", "Lat"] if c in df.columns]
    if set(cols) != {"StnID", "Lon", "Lat"}:
        print(
            "   ! station_decadal_stats is missing Lon/Lat columns; cannot attach "
            "AQUIFER_ID. Aquifer-level analyses will be skipped."
        )
        return df

    stn = (
        df[["StnID", "Lon", "Lat"]]
        .dropna(subset=["Lon", "Lat"])
        .drop_duplicates(subset=["StnID"])
        .copy()
    )
    if stn.empty:
        print(
            "   ! No valid station coordinates found; cannot attach AQUIFER_ID. "
            "Aquifer-level analyses will be skipped."
        )
        return df

    try:
        # Points
        pts = gpd.GeoDataFrame(
            stn,
            geometry=gpd.points_from_xy(stn["Lon"].astype(float), stn["Lat"].astype(float)),
            crs="EPSG:4326",
        )

        # Aquifers
        aq = gpd.read_file(shp_path)
        if aq.empty:
            print(
                "   ! Aquifer shapefile is empty; cannot attach AQUIFER_ID. "
                "Aquifer-level analyses will be skipped."
            )
            return df

        if aq.crs is None:
            # Assume EPSG:4326 if missing (best-effort)
            aq = aq.set_crs("EPSG:4326")
        else:
            aq = aq.to_crs("EPSG:4326")

        # Reset index so that AQUIFER_ID matches feature order (0..n-1)
        aq = aq.reset_index().rename(columns={"index": "AQUIFER_ID"})
        aq = aq[["AQUIFER_ID", "geometry"]]

        # Spatial join (intersects is more tolerant for boundary points)
        try:
            joined = gpd.sjoin(pts, aq, how="left", predicate="intersects")
        except TypeError:
            # Older geopandas API
            joined = gpd.sjoin(pts, aq, how="left", op="intersects")

        if joined.empty or ("AQUIFER_ID" not in joined.columns):
            print(
                "   ! Spatial join returned no matches; AQUIFER_ID will remain NaN "
                "and aquifer-level analyses will be skipped."
            )
            return df

        # If a point touches multiple polygons (rare), choose the smallest index
        m = (
            joined[["StnID", "AQUIFER_ID"]]
            .dropna(subset=["AQUIFER_ID"])
            .sort_values(["StnID", "AQUIFER_ID"])
            .drop_duplicates(subset=["StnID"], keep="first")
        )

        if m.empty:
            print(
                "   ! No station could be mapped to an aquifer polygon; AQUIFER_ID will "
                "remain NaN and aquifer-level analyses will be skipped."
            )
            return df

        df = df.merge(m, on="StnID", how="left", suffixes=("", "_join"))

        # Prefer existing AQUIFER_ID if present
        if "AQUIFER_ID_join" in df.columns:
            df["AQUIFER_ID"] = df["AQUIFER_ID"].where(
                np.isfinite(df["AQUIFER_ID"]), df["AQUIFER_ID_join"]
            )
            df = df.drop(columns=["AQUIFER_ID_join"])

        # Final type normalization
        df["AQUIFER_ID"] = pd.to_numeric(df["AQUIFER_ID"], errors="coerce")

    except Exception as exc:
        print(
            f"   ! Failed to attach AQUIFER_ID via spatial join: {exc}. "
            "Aquifer-level analyses will be skipped."
        )
        return df

    return df


def load_decadal_stats(cfg: Config) -> pd.DataFrame:
    """
    Load station–decade statistics produced by the anomaly calculation pipeline.

    Expected input (default): station_decadal_stats.csv
        - StnID, Decade
        - n_anom, sum_anom, sum_neg, sum_pos, n_neg, n_pos
        - Lat, Lon
        - ratio_neg, ratio_pos, frac_neg, frac_pos

    If AQUIFER_ID is not present in the input file, it will be attached using
    cfg.aquifer_shapefile. This keeps the post-analysis pipeline consistent with
    the start-to-end workflow that is based on:
        1) station_anomalies_all.csv
        2) station_decadal_stats.csv
    """
    path = os.path.join(cfg.anomalies_dir, cfg.decadal_stats_file)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Decadal stats file not found: {path}")

    df = pd.read_csv(path)

    # Minimal required columns for station-decadal processing
    required_cols = {"Decade", "StnID", "Lat", "Lon", "n_anom", "sum_anom"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            "station_decadal_stats.csv is missing required columns: "
            f"{sorted(missing)}"
        )

    # Type normalization
    df["StnID"] = df["StnID"].astype(str)
    df["Decade"] = pd.to_numeric(df["Decade"], errors="coerce").astype("Int64")

    num_cols = [
        "n_anom",
        "sum_anom",
        "sum_neg",
        "sum_pos",
        "n_neg",
        "n_pos",
        "Lat",
        "Lon",
        "ratio_neg",
        "ratio_pos",
        "frac_neg",
        "frac_pos",
        "AQUIFER_ID",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Attach aquifer IDs (if missing)
    if ("AQUIFER_ID" not in df.columns) or (df["AQUIFER_ID"].isna().all()):
        df = _attach_aquifer_ids_to_station_decadal(df, cfg)

    # Drop rows with invalid Decade (cannot be used)
    df = df.dropna(subset=["Decade"]).copy()
    df["Decade"] = df["Decade"].astype(int)

    df = df.sort_values(["Decade", "StnID"]).reset_index(drop=True)
    return df


def load_aquifer_decadal_stats(
    cfg: Config, station_decadal_df: pd.DataFrame | None = None
) -> pd.DataFrame:
    """
    Produce an aquifer-level decadal table for trend and persistence analysis.

    Compatibility objective
    -----------------------
    This post-analysis pipeline is designed to be driven by the *two* upstream
    outputs of the anomaly/QC script:

        1) station_anomalies_all.csv
        2) station_decadal_stats.csv

    Therefore, when `station_decadal_df` is provided (as in `main()`), aquifer-
    level decadal statistics are **derived** from that dataframe, after aquifer
    IDs have been attached via the aquifer shapefile.

    Backward compatibility
    ----------------------
    If `station_decadal_df` is not provided, the function falls back to loading
    cfg.aquifer_decadal_stats_file (if available).

    Minimum columns guaranteed in the returned dataframe:
        - AQUIFER_ID
        - Decade
        - n_stn_decade
        - mean_sum_anom  (computed as sum(sum_anom)/sum(n_anom) per aquifer×decade)

    If available in station_decadal_df, the following are included as weighted
    means (weights=n_anom):
        - mean_ratio_neg, mean_ratio_pos
        - mean_frac_neg, mean_frac_pos
    """

    # --- Preferred path: derive from station_decadal_df ---
    if station_decadal_df is not None:
        s = station_decadal_df.copy()

        required2 = {"AQUIFER_ID", "Decade", "StnID", "n_anom", "sum_anom"}
        missing2 = required2 - set(s.columns)
        if missing2:
            raise ValueError(
                "station_decadal_df is missing required columns to derive aquifer stats: "
                f"{sorted(missing2)}"
            )

        # Keep only valid, finite aquifer+decade rows
        s["AQUIFER_ID"] = pd.to_numeric(s["AQUIFER_ID"], errors="coerce")
        s["Decade"] = pd.to_numeric(s["Decade"], errors="coerce")
        s["n_anom"] = pd.to_numeric(s["n_anom"], errors="coerce")
        s["sum_anom"] = pd.to_numeric(s["sum_anom"], errors="coerce")

        s = s.dropna(subset=["AQUIFER_ID", "Decade"]).copy()
        s = s[(s["n_anom"] > 0) & np.isfinite(s["sum_anom"].values)].copy()
        if s.empty:
            print(
                "   ! Could not derive aquifer_decadal_stats (no valid AQUIFER_ID/Decade rows). "
                "Aquifer-level analyses will be skipped."
            )
            return pd.DataFrame(
                columns=["AQUIFER_ID", "Decade", "n_stn_decade", "mean_sum_anom"]
            )

        # Weighted mean anomaly per aquifer×decade: sum(sum_anom)/sum(n_anom)
        grp = s.groupby(["AQUIFER_ID", "Decade"], as_index=False)

        agg = grp.agg(
            n_stn_decade=("StnID", "nunique"),
            n_anom_total=("n_anom", "sum"),
            sum_anom_total=("sum_anom", "sum"),
            sum_neg_total=("sum_neg", "sum") if "sum_neg" in s.columns else ("sum_anom", "sum"),
            sum_pos_total=("sum_pos", "sum") if "sum_pos" in s.columns else ("sum_anom", "sum"),
        )

        agg["mean_sum_anom"] = agg["sum_anom_total"] / agg["n_anom_total"]

        # Optional: ratios / fractions as weighted means
        def _weighted_mean(col: str, weight: str = "n_anom") -> pd.Series:
            if col not in s.columns:
                return pd.Series(dtype=float)

            x = pd.to_numeric(s[col], errors="coerce")
            w = pd.to_numeric(s[weight], errors="coerce")
            m = np.isfinite(x.values) & np.isfinite(w.values) & (w.values > 0)
            if not np.any(m):
                return pd.Series(dtype=float)

            tmp = s.loc[m, ["AQUIFER_ID", "Decade"]].copy()
            tmp["_w"] = w.loc[m].astype(float).values
            tmp["_wx"] = (x.loc[m].astype(float).values) * tmp["_w"].values

            g = tmp.groupby(["AQUIFER_ID", "Decade"], as_index=False).agg(
                _w_sum=("_w", "sum"),
                _wx_sum=("_wx", "sum"),
            )
            out = g["_wx_sum"] / g["_w_sum"]
            out.index = pd.MultiIndex.from_frame(g[["AQUIFER_ID", "Decade"]])
            return out

        # Build MultiIndex for fast alignment
        idx = pd.MultiIndex.from_frame(agg[["AQUIFER_ID", "Decade"]])
        agg = agg.set_index(idx)

        for col, out_name in [
            ("ratio_neg", "mean_ratio_neg"),
            ("ratio_pos", "mean_ratio_pos"),
            ("frac_neg", "mean_frac_neg"),
            ("frac_pos", "mean_frac_pos"),
        ]:
            wm = _weighted_mean(col)
            if not wm.empty:
                agg[out_name] = wm.reindex(agg.index)

        agg = agg.reset_index(drop=True)

        # Final types / sorting
        agg["AQUIFER_ID"] = pd.to_numeric(agg["AQUIFER_ID"], errors="coerce").astype(int)
        agg["Decade"] = pd.to_numeric(agg["Decade"], errors="coerce").astype(int)
        agg = agg.sort_values(["Decade", "AQUIFER_ID"]).reset_index(drop=True)
        return agg

    # --- Fallback path: load existing file (older workflow) ---
    path = os.path.join(cfg.anomalies_dir, cfg.aquifer_decadal_stats_file)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Aquifer decadal stats file not found: {path}")

    df = pd.read_csv(path)

    required = {"AQUIFER_ID", "Decade", "n_stn_decade"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            "aquifer_decadal_stats.csv is missing required columns: "
            f"{sorted(missing)}"
        )

    has_mean_sum = "mean_sum_anom" in df.columns
    has_ratios = {"mean_ratio_neg", "mean_ratio_pos"}.issubset(df.columns)
    if not (has_mean_sum or has_ratios):
        raise ValueError(
            "aquifer_decadal_stats.csv must contain either 'mean_sum_anom' "
            "or both 'mean_ratio_neg' and 'mean_ratio_pos'."
        )

    df["AQUIFER_ID"] = pd.to_numeric(df["AQUIFER_ID"], errors="coerce")
    df["Decade"] = pd.to_numeric(df["Decade"], errors="coerce")
    df["n_stn_decade"] = pd.to_numeric(df["n_stn_decade"], errors="coerce")
    df = df.dropna(subset=["AQUIFER_ID", "Decade"]).copy()
    df["AQUIFER_ID"] = df["AQUIFER_ID"].astype(int)
    df["Decade"] = df["Decade"].astype(int)
    df = df.sort_values(["Decade", "AQUIFER_ID"]).reset_index(drop=True)
    return df


def load_annual_trimmed_anomalies(cfg: Config) -> pd.DataFrame:
    """
    annual_excluded_p5_p95_anomalies.csv dosyasını okur.

    Beklenen temel kolonlar:
        - Year
        - side  ('low' / 'high')
        - Lon, Lat (bölge tayini için; Region yoksa kullanılır)
        - Region (varsa doğrudan kullanılır)
    """
    path = os.path.join(cfg.anomalies_dir, cfg.annual_excluded_file)
    if not os.path.exists(path):
        print(f"[F9/F10] Annual trimming file not found: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path)

    # Year kolonunu standardize et
    if "Year" not in df.columns and "year" in df.columns:
        df = df.rename(columns={"year": "Year"})
    if "Year" not in df.columns:
        raise ValueError(
            f"{cfg.annual_excluded_file} must contain a 'Year' (or 'year') column."
        )
    df = df.dropna(subset=["Year"]).copy()
    df["Year"] = df["Year"].astype(int)

    # side kolonunu standardize et
    if "side" in df.columns:
        df["side"] = (
            df["side"]
            .astype(str)
            .str.strip()
            .str.lower()
        )
    else:
        # Çok istisnai; normalde pipeline side kolonunu yazar.
        df["side"] = "unknown"

    # Region yoksa Lon/Lat'ten türet
    if "Region" not in df.columns:
        if {"Lon", "Lat"}.issubset(df.columns):
            df["Region"] = [
                _region_from_lonlat(lon, lat)
                for lon, lat in zip(df["Lon"].values, df["Lat"].values)
            ]
        else:
            df["Region"] = "global"

    return df


def load_decadal_trimmed_anomalies(cfg: Config) -> pd.DataFrame:
    """
    decadal_excluded_p5_p95_mean_anomalies.csv dosyasını okur.

    Beklenen temel kolonlar:
        - Decade
        - side  ('low' / 'high')
        - Lon, Lat (bölge tayini için; Region yoksa kullanılır)
        - Region (varsa doğrudan kullanılır)
    """
    path = os.path.join(cfg.anomalies_dir, cfg.decadal_excluded_file)
    if not os.path.exists(path):
        print(f"[F9/F10] Decadal trimming file not found: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path)

    if "Decade" not in df.columns and "decade" in df.columns:
        df = df.rename(columns={"decade": "Decade"})
    if "Decade" not in df.columns:
        raise ValueError(
            f"{cfg.decadal_excluded_file} must contain a 'Decade' (or 'decade') column."
        )
    df = df.dropna(subset=["Decade"]).copy()
    df["Decade"] = df["Decade"].astype(int)

    if "side" in df.columns:
        df["side"] = (
            df["side"]
            .astype(str)
            .str.strip()
            .str.lower()
        )
    else:
        df["side"] = "unknown"

    if "Region" not in df.columns:
        if {"Lon", "Lat"}.issubset(df.columns):
            df["Region"] = [
                _region_from_lonlat(lon, lat)
                for lon, lat in zip(df["Lon"].values, df["Lat"].values)
            ]
        else:
            df["Region"] = "global"

    return df


def summarize_trimming_by_region(
    annual_excl: pd.DataFrame,
    decadal_excl: pd.DataFrame,
    cfg: Config,
) -> None:
    """
    5–95% trimming’in yıllık ve onyıllık bazda ne kadar veri dışladığını
    ve bölgelere göre dağılımını özetler. Konsola ve summary_log dosyasına yazar.
    """
    lines: list[str] = []
    lines.append("=== 5–95% trimming diagnostics (annual / decadal) ===")

    if not annual_excl.empty:
        lines.append("[Annual] Total excluded values by tail (global):")
        tbl = (
            annual_excl.groupby("side")
            .size()
            .rename("n")
            .reset_index()
        )
        lines.append(tbl.to_string(index=False))

        if "Region" in annual_excl.columns:
            lines.append("")
            lines.append("[Annual] Excluded values by Region x tail:")
            tbl_reg = (
                annual_excl.groupby(["Region", "side"])
                .size()
                .rename("n")
                .reset_index()
                .pivot(index="Region", columns="side", values="n")
                .fillna(0.0)
            )
            lines.append(tbl_reg.astype(int).to_string())
    else:
        lines.append("[Annual] No annual trimming data found.")

    lines.append("")

    if not decadal_excl.empty:
        lines.append("[Decadal] Total excluded values by tail (global):")
        tbl2 = (
            decadal_excl.groupby("side")
            .size()
            .rename("n")
            .reset_index()
        )
        lines.append(tbl2.to_string(index=False))

        if "Region" in decadal_excl.columns:
            lines.append("")
            lines.append("[Decadal] Excluded values by Region x tail:")
            tbl2_reg = (
                decadal_excl.groupby(["Region", "side"])
                .size()
                .rename("n")
                .reset_index()
                .pivot(index="Region", columns="side", values="n")
                .fillna(0.0)
            )
            lines.append(tbl2_reg.astype(int).to_string())
    else:
        lines.append("[Decadal] No decadal trimming data found.")

    lines.append("====================================================")

    block = "\n".join(lines)
    print("\n" + block + "\n")
    append_to_summary_log(cfg, block)


def load_global_outlier_anomalies(cfg: Config) -> pd.DataFrame:
    """
    GlobalGWDrought ana pipeline'ının ürettiği global_outlier_anomalies.csv
    dosyasını okur.

    Beklenen temel kolonlar:
        - StnID
        - Lon, Lat
        - Region (varsa; yoksa 'global' atanır)
    """
    path = os.path.join(cfg.anomalies_dir, cfg.global_outlier_anomalies_file)
    if not os.path.exists(path):
        print(f"   ! Global outlier file not found: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path)
    if df.empty:
        print("   ! Global outlier file is empty.")
        return df

    # Kolon isimlerini minimal düzeyde kontrol edelim
    required_cols = {"StnID", "Lon", "Lat"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"global_outlier_anomalies.csv is missing required columns: {missing}"
        )

    # Region yoksa 'global' tanımla
    if "Region" not in df.columns:
        df["Region"] = "global"

    # Region değerlerini biraz normalize edelim
    df["Region"] = (
        df["Region"]
        .astype(str)
        .str.strip()
        .replace({"Other world": "global"})
    )

    return df


def summarize_repeated_outlier_stations(
    outlier_df: pd.DataFrame,
    cfg: Config,
    thresholds: tuple[int, ...] = (3, 5, 10),
) -> pd.DataFrame:
    """
    Her istasyon için global outlier sayısını hesaplar ve:
      - thresholds (örn. 3+, 5+, 10+) için istasyon sayılarını log'a yazar,
      - en çok outlier üreten istasyonların kısa listesini log'a ekler,
      - tam istasyon özetini CSV olarak kaydeder.

    Dönüş:
        station_summary_df: kolonlar
            StnID, n_outliers, Lon, Lat, Region
    """
    if outlier_df.empty:
        print("   ! No global outliers available; skipping repeated-station summary.")
        return pd.DataFrame()

    grp = (
        outlier_df.groupby("StnID")
        .agg(
            n_outliers=("StnID", "size"),
            Lon=("Lon", "median"),
            Lat=("Lat", "median"),
            Region=("Region", "first"),
        )
        .reset_index()
    )

    if grp.empty:
        print("   ! Grouped outlier summary is empty; skipping.")
        return grp

    grp = grp.sort_values("n_outliers", ascending=False)

    # Konsol + özet dosyası için metin bloğu
    lines: list[str] = []
    lines.append("=== Repeated-global-outlier station summary ===")
    lines.append(f"Total stations with any global outlier: {len(grp)}")

    for thr in thresholds:
        n_thr = int((grp["n_outliers"] >= thr).sum())
        lines.append(f"Stations with ≥{thr} outliers: {n_thr}")

    lines.append("")
    lines.append("Top 10 stations by number of outliers:")
    top10 = grp.head(10)
    for _, row in top10.iterrows():
        lines.append(
            f" - {row['StnID']}: n_outliers={int(row['n_outliers'])}, "
            f"Region={row.get('Region', 'N/A')}, "
            f"lon={row['Lon']:.2f}, lat={row['Lat']:.2f}"
        )

    block = "\n".join(lines)
    print("\n" + block + "\n")
    append_to_summary_log(cfg, block)

    # Tam tabloyu CSV olarak kaydet
    out_path = os.path.join(cfg.anomalies_dir, cfg.repeated_outlier_summary_file)
    ensure_dir(cfg.anomalies_dir)
    grp.to_csv(out_path, index=False)
    print(f"   -> Station-level repeated-outlier summary written to: {out_path}")

    return grp


def compute_network_counts(
    annual_df: pd.DataFrame,
    decadal_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute basic coverage metrics of the groundwater observation network.

    Annual (global): station counts by Year (from n_total / n_station / StnID-derived)
    Decadal (global): n_station (unique stations), n_aquifer (unique aquifers)
    """

    if annual_df is None or annual_df.empty:
        raise ValueError("annual_df is empty; cannot compute network counts.")

    # Defensive: strip any accidental whitespace in column names
    annual = annual_df.copy()
    annual.columns = [str(c).strip() for c in annual.columns]

    # --- Annual counts: prefer rows explicitly marked as global; otherwise fallback to all rows ---
    if "Region" in annual.columns:
        reg = annual["Region"].astype(str).str.strip().str.lower()
        annual_global = annual.loc[reg == "global"].copy()
        if annual_global.empty:
            # Do not hard-fail; some pipelines may not write an explicit 'global' aggregate
            annual_global = annual.copy()
    else:
        annual_global = annual.copy()

    if "Year" not in annual_global.columns:
        raise ValueError(f"annual_df must contain 'Year'. Available columns: {list(annual_global.columns)}")

    annual_global["Year"] = pd.to_numeric(annual_global["Year"], errors="coerce").astype("Int64")
    annual_global = annual_global.dropna(subset=["Year"]).copy()
    annual_global["Year"] = annual_global["Year"].astype(int)

    # --- Determine which column encodes annual counts ---
    count_candidates = ["n_total", "n_station", "n_stations", "count", "n"]
    count_col = next((c for c in count_candidates if c in annual_global.columns), None)

    if count_col is None:
        # Fallback: derive from per-station annual records if present
        if "StnID" in annual_global.columns:
            annual_counts = (
                annual_global.groupby("Year")["StnID"]
                .nunique()
                .rename("n_station")
                .reset_index()
                .sort_values("Year")
                .reset_index(drop=True)
            )
        else:
            raise ValueError(
                "annual_df does not contain any of the expected count columns "
                f"{count_candidates} and does not contain 'StnID' for derivation. "
                f"Available columns: {list(annual_global.columns)}"
            )
    else:
        tmp = annual_global[["Year", count_col]].copy()
        tmp[count_col] = pd.to_numeric(tmp[count_col], errors="coerce")

        # If multiple rows per Year exist, aggregate to a single annual count.
        # If AQUIFER_ID exists, rows are likely disjoint by aquifer; sum is sensible.
        if tmp.duplicated("Year").any():
            if "AQUIFER_ID" in annual_global.columns:
                tmp = tmp.groupby("Year", as_index=False)[count_col].sum()
            else:
                # Conservative fallback: avoid accidental over-counting
                tmp = tmp.groupby("Year", as_index=False)[count_col].max()

        annual_counts = (
            tmp.rename(columns={count_col: "n_station"})
               .sort_values("Year")
               .reset_index(drop=True)
        )

    # --- Decadal counts: group by Decade ---
    if decadal_df is None or decadal_df.empty:
        raise ValueError("decadal_df is empty; cannot compute decadal network counts.")

    grp = decadal_df.groupby("Decade", dropna=False)
    decadal_counts = grp.agg(
        n_station=("StnID", "nunique"),
        n_aquifer=("AQUIFER_ID", lambda s: s.dropna().nunique()),
    ).reset_index()

    decadal_counts = decadal_counts.sort_values("Decade").reset_index(drop=True)
    return annual_counts, decadal_counts


def save_figure(fig: plt.Figure, cfg: Config, basename: str, out_dir: str | None = None) -> None:
    """
    Save a figure in raster (TIFF) and optionally in vector (PDF/SVG/EPS) format
    using configuration in cfg.

    Parameters
    ----------
    fig
        Matplotlib Figure.
    cfg
        Global configuration (dpi, formats, etc.).
    basename
        Output file basename (without extension).
    out_dir
        Optional override for output folder. If None, cfg.out_fig_dir is used.
    """
    out_dir_use = cfg.out_fig_dir if out_dir is None else out_dir
    ensure_dir(out_dir_use)
    root = os.path.join(out_dir_use, basename)

    # Raster (TIFF) version – white background, high resolution
    raster_path = f"{root}.{cfg.raster_format}"
    print(f"   -> Saving raster figure: {raster_path}", flush=True)

    t0 = time.perf_counter()
    try:
        raster_kwargs: dict[str, Any] = dict(
            dpi=cfg.fig_dpi,
            format=cfg.raster_format,
            bbox_inches="tight",
            facecolor="white",
        )

        # Best-effort TIFF compression to reduce disk usage (no behavior change if unsupported).
        if str(cfg.raster_format).lower() in ("tif", "tiff"):
            raster_kwargs["pil_kwargs"] = {"compression": "tiff_lzw"}

        fig.savefig(raster_path, **raster_kwargs)
        dt = time.perf_counter() - t0
        print(f"      Raster saved in {dt:,.1f}s", flush=True)

    except OSError as e:
        # ENOSPC (disk full) => keep pipeline alive and provide a clear console message
        if getattr(e, "errno", None) == 28 or "No space left on device" in str(e):
            print(f"   ! Disk full while saving raster figure: {raster_path}", flush=True)
            print(f"     {e}", flush=True)
            return
        raise

    # Optional vector version (e.g., PDF) – white background
    if cfg.save_vector:
        vector_path = f"{root}.{cfg.vector_format}"
        print(f"   -> Saving vector figure: {vector_path}", flush=True)

        t1 = time.perf_counter()
        try:
            fig.savefig(
                vector_path,
                format=cfg.vector_format,
                bbox_inches="tight",
                facecolor="white",
            )
            dt = time.perf_counter() - t1
            print(f"      Vector saved in {dt:,.1f}s", flush=True)

        except OSError as e:
            if getattr(e, "errno", None) == 28 or "No space left on device" in str(e):
                print(f"   ! Disk full while saving vector figure: {vector_path}", flush=True)
                print(f"     {e}", flush=True)
                return
            raise


# =========================
# FIGURE OUTPUT SUBFOLDERS
# =========================
def get_p5_p95_fig_dir(cfg: Config) -> str:
    """Return output directory for p5–p95 (trimmed) figure variants."""
    return os.path.join(cfg.out_fig_dir, "p5_p95_trimmed")


def get_p0_p100_fig_dir(cfg: Config) -> str:
    """Return output directory for p0–p100 (full-range) figure variants."""
    return os.path.join(cfg.out_fig_dir, "p0_p100_full")


def ensure_variant_fig_dirs(cfg: Config) -> tuple[str, str]:
    """
    Ensure that variant figure directories exist; returns (p5_p95_dir, p0_p100_dir).
    """
    p5p95_dir = get_p5_p95_fig_dir(cfg)
    p0p100_dir = get_p0_p100_fig_dir(cfg)
    ensure_dir(p5p95_dir)
    ensure_dir(p0p100_dir)
    return p5p95_dir, p0p100_dir



def lighten_color(color: str, factor: float = 0.3):
    """
    Lighten a matplotlib color by mixing it with white.

    factor in [0,1]:
        0 -> white
        1 -> original color
    """
    # Güvenlik: factor'ü 0–1 aralığına zorla
    factor = float(max(0.0, min(1.0, factor)))

    rgb = np.array(mcolors.to_rgb(color))
    # factor=1 → orijinal renk, factor=0 → beyaz
    return tuple(1.0 - factor * (1.0 - rgb))


# ==========================================================
# Güvenli renk ölçeği seçici (tek-kutuplu ↔ iki-kutuplu otomatik)
# ==========================================================
def _safe_diverging_norm_and_cmap(data_min, data_max, p2=None, p98=None):
    """
    Veri aralığına göre uygun norm/cmap döndürür.
      - Tümü >= 0 ise: Normalize(0..vmax), 'YlOrRd'
      - Tümü <= 0 ise: Normalize(vmin..0), 'YlGnBu'
      - Karışık işaretli ise: TwoSlopeNorm(vmin<0<vmax), 'RdBu'
    Döndürür: norm, cmap (mpl colormap objesi), vmin, vmax
    """
    # Robust sınırları uygula (opsiyonel)
    vmin = float(data_min) if p2 is None or not np.isfinite(p2) else float(min(p2, data_min))
    vmax = float(data_max) if p98 is None or not np.isfinite(p98) else float(max(p98, data_max))

    # Tek-kutuplu (tamamı pozitif veya sıfır)
    if vmin >= 0:
        if not np.isfinite(vmax) or vmax <= 0:
            vmax = 1.0
        norm = mcolors.Normalize(vmin=0.0, vmax=vmax)
        cmap = mpl.colormaps.get("YlOrRd")
        return norm, cmap, 0.0, vmax

    # Tek-kutuplu (tamamı negatif veya sıfır)
    if vmax <= 0:
        if not np.isfinite(vmin) or vmin >= 0:
            vmin = -1.0
        norm = mcolors.Normalize(vmin=vmin, vmax=0.0)
        cmap = mpl.colormaps.get("YlGnBu")
        return norm, cmap, vmin, 0.0

    # Karışık işaretli → diverging, 0 merkezli
    eps = 1e-9
    if vmin >= 0:
        vmin = -eps
    if vmax <= 0:
        vmax = +eps
    if not (np.isfinite(vmin) and np.isfinite(vmax)) or (vmax - vmin) <= 1e-12:
        vmin, vmax = -1.0, 1.0

    norm = mcolors.TwoSlopeNorm(vmin=vmin, vcenter=0.0, vmax=vmax)
    cmap = mpl.colormaps.get("RdBu")
    return norm, cmap, vmin, vmax


def get_marker_size(n_points: int) -> float:
    """
    İstasyon sayısına göre nokta boyutunu ölçekler.
    Çok istasyon olduğunda marker'lar daha küçük çizilir.

    Parametre
    ---------
    n_points : int
        Haritadaki istasyon sayısı.

    Dönüş
    -----
    size : float
        Matplotlib 's' parametresi için marker boyutu.
    """
    if n_points <= 100:
        return 25.0
    elif n_points <= 500:
        return 15.0
    elif n_points <= 2000:
        return 8.0
    else:
        return 4.0


def style_ts_axis(ax: plt.Axes) -> None:
    """
    Apply a consistent, journal-style formatting to time-series axes.
    """
    ax.grid(True, which="major", linestyle="-", linewidth=0.4, alpha=0.4)
    ax.set_axisbelow(True)

    # Remove top and right spines for a cleaner look
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

    # Make left and bottom spines slightly bolder
    for spine in ["left", "bottom"]:
        ax.spines[spine].set_linewidth(0.8)

    # Tick parameters (journal-friendly)
    ax.tick_params(axis="both", which="major", labelsize=8, width=0.6, length=3)


def theil_sen_slope(x: np.ndarray, y: np.ndarray) -> float:
    """
    Compute the Theil–Sen (Sen’s) slope estimator for a univariate time series.

    Parameters
    ----------
    x : array-like
        Independent variable (e.g. Decade).
    y : array-like
        Dependent variable (e.g. decadal anomaly).

    Returns
    -------
    float
        Median slope over all pairs (i, j), i < j.
    """
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    n = len(x)
    if n < 2:
        return np.nan

    slopes = []
    for i in range(n - 1):
        for j in range(i + 1, n):
            dx = x[j] - x[i]
            if dx == 0:
                continue
            slopes.append((y[j] - y[i]) / dx)

    if not slopes:
        return np.nan
    return float(np.median(slopes))


def fdr_bh(pvals: np.ndarray, alpha: float = 0.1) -> tuple[np.ndarray, np.ndarray]:
    """
    Benjamini–Hochberg FDR (False Discovery Rate) düzeltmesi.

    Parameters
    ----------
    pvals : array-like
        Testlerden gelen ham p-değerleri.
    alpha : float, optional
        Hedef FDR seviyesi (tipik olarak 0.05 veya 0.10).

    Returns
    -------
    reject : np.ndarray of bool
        Her test için H0 reddediliyor mu (FDR ile düzeltilmiş kritere göre).
    qvals : np.ndarray of float
        Benjamini–Hochberg ayarlı p-değerleri (q-değerleri).
    """
    pvals = np.asarray(pvals, dtype=float)
    n = pvals.size
    qvals = np.full(n, np.nan)
    reject = np.zeros(n, dtype=bool)

    finite_mask = np.isfinite(pvals)
    if finite_mask.sum() == 0:
        return reject, qvals

    p = pvals[finite_mask]
    m = p.size

    # Küçükten büyüğe sırala
    order = np.argsort(p)
    ranked = p[order]

    # BH faktörleri (i/m)
    # adjusted p: p_i * m / i, sonra monotoniteyi sağlamak için geriye doğru minimum
    adj = ranked * m / np.arange(1, m + 1, dtype=float)
    adj_rev = np.minimum.accumulate(adj[::-1])[::-1]

    # Kritik eşik ile anlamlılık (step-up prosedürü)
    bh_thresholds = alpha * (np.arange(1, m + 1, dtype=float) / m)
    sig_sorted = ranked <= bh_thresholds

    if sig_sorted.any():
        k_max = np.where(sig_sorted)[0].max()
        crit = ranked[k_max]
        sig = p <= crit
    else:
        sig = np.zeros_like(p, dtype=bool)

    # Ayarlı p'leri orijinal sıraya döndür
    inv_order = np.argsort(order)
    qvals[finite_mask] = adj_rev[inv_order]
    reject[finite_mask] = sig

    return reject, qvals


def compute_station_regime_features(
    decadal_df: pd.DataFrame,
    cfg: Config,
) -> pd.DataFrame:
    """
    İstasyon-onyıl istatistiklerinden istasyon bazında rejim öznitelikleri üretir.

    Girdi: station_decadal_stats_with_aquifer.csv (decadal_df)
        Kolonlar (en az):
            - StnID, Decade, n_anom, sum_anom
            - frac_neg, frac_pos
            - Lat, Lon
            - Region (varsa; yoksa NaN olabilir)

    Çıktı (satır = istasyon):
        - StnID
        - n_decades, n_years_total
        - mean_anom_station      (tüm yıllar üzerinden ağırlıklı ortalama anomaly)
        - neg_dec_fraction       (negatif onyıl sayısı / toplam onyıl sayısı)
        - trend_slope            (decadal mean anomaly trendi; Sen’s slope)
        - mean_frac_neg, mean_frac_pos
        - Lat, Lon, Region
        - simple_regime          ("mostly_negative", "mostly_positive", "mixed")
    """
    required = {
        "StnID", "Decade", "n_anom", "sum_anom",
        "frac_neg", "frac_pos", "Lat", "Lon",
    }
    missing = required - set(decadal_df.columns)
    if missing:
        raise ValueError(f"Decadal station stats missing columns: {sorted(missing)}")

    df = decadal_df.dropna(subset=["StnID"]).copy()

    # En az station_regime_min_decades onyılı olan istasyonları al
    dec_counts = df.groupby("StnID")["Decade"].nunique()
    valid_ids = dec_counts[dec_counts >= cfg.station_regime_min_decades].index
    df = df[df["StnID"].isin(valid_ids)].copy()

    records: list[dict] = []

    for stn_id, g in df.groupby("StnID"):
        g = g.sort_values("Decade")
        n_dec = len(g)
        total_n_anom = g["n_anom"].sum()
        total_sum_anom = g["sum_anom"].sum()

        if total_n_anom > 0:
            mean_anom_station = total_sum_anom / total_n_anom
        else:
            mean_anom_station = np.nan

        # Onyıl bazlı ortalama anomaly (sum_anom / n_anom)
        mean_anom_dec = g["sum_anom"] / g["n_anom"].replace(0, np.nan)

        # Negatif onyıl oranı
        n_neg_dec = (mean_anom_dec < 0).sum()
        neg_dec_fraction = n_neg_dec / n_dec

        # Trend(A): Decade vs decadal mean anomaly
        slope = theil_sen_slope(g["Decade"].values, mean_anom_dec.values)

        mean_frac_neg = g["frac_neg"].mean()
        mean_frac_pos = g["frac_pos"].mean()

        lat = g["Lat"].iloc[0]
        lon = g["Lon"].iloc[0]

        if "Region" in g.columns:
            region_vals = g["Region"].dropna()
            region = region_vals.iloc[0] if not region_vals.empty else np.nan
        else:
            region = np.nan

        # Eğer Region sütunu yoksa veya tümü NaN ise, Lon/Lat'ten türet
        if (not isinstance(region, str) or region.strip() == "") and np.isfinite(lon) and np.isfinite(lat):
            region = _region_from_lonlat(lon, lat)

        # Basit tipoloji
        if neg_dec_fraction >= 0.70:
            simple_regime = "mostly_negative"
        elif neg_dec_fraction <= 0.30:
            simple_regime = "mostly_positive"
        else:
            simple_regime = "mixed"

        records.append(
            {
                "StnID": stn_id,
                "n_decades": int(n_dec),
                "n_years_total": int(total_n_anom),
                "mean_anom_station": float(mean_anom_station),
                "neg_dec_fraction": float(neg_dec_fraction),
                "trend_slope": float(slope),
                "mean_frac_neg": float(mean_frac_neg),
                "mean_frac_pos": float(mean_frac_pos),
                "Lat": float(lat),
                "Lon": float(lon),
                "Region": region,
                "simple_regime": simple_regime,
            }
        )

    station_features = pd.DataFrame.from_records(records)
    return station_features


def mann_kendall_test(y: np.ndarray) -> tuple[float, float, float]:
    """
    Perform a (two-sided) Mann–Kendall trend test on a time series.

    Parameters
    ----------
    y : array-like
        Time series (e.g. decadal anomaly values).

    Returns
    -------
    tau : float
        Kendall's tau.
    S : float
        Mann–Kendall S statistic.
    p : float
        Two-sided p-value based on the normal approximation (no tie correction).
    """
    y = np.asarray(y, dtype=float)
    y = y[np.isfinite(y)]
    n = len(y)
    if n < 2:
        return np.nan, np.nan, np.nan

    # Mann–Kendall S statistic
    S = 0
    for k in range(n - 1):
        diff = y[k + 1:] - y[k]
        S += np.sign(diff).sum()

    # Variance of S (no tie correction; suitable for continuous anomalies)
    var_S = n * (n - 1) * (2 * n + 5) / 18.0

    if S > 0:
        z = (S - 1) / sqrt(var_S)
    elif S < 0:
        z = (S + 1) / sqrt(var_S)
    else:
        z = 0.0

    # Two-sided p-value from standard normal
    p = erfc(abs(z) / sqrt(2.0))

    # Kendall's tau
    tau = S / (0.5 * n * (n - 1))
    return float(tau), float(S), float(p)


def compute_aquifer_trends(aq_df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """
    Compute decadal trends for each aquifer using:

        A_q(d)  ~  decadal anomaly metric

    where the anomaly metric is taken as:
        - 'mean_sum_anom' if available (preferred; physical units)
        - otherwise: mean_ratio_neg + mean_ratio_pos.

    Only decades with n_stn_decade >= cfg.trend_min_stations_per_decade
    are used, and an aquifer is included only if it has at least
    cfg.trend_min_decades_per_aquifer valid decades.

    For each aquifer, the function returns:
        - AQUIFER_ID
        - n_decades_used
        - sen_slope
        - mk_tau, mk_S, mk_p
        - trend_class  (significant_negative / significant_positive / no_significant_trend)
        - trend_sign   (negative / positive / zero)  <-- yeni
        - mean_n_stn_decade
        - Region (optional; if present in input, mode value is propagated)
    """
    required = {"AQUIFER_ID", "Decade", "n_stn_decade"}
    missing = required - set(aq_df.columns)
    if missing:
        raise ValueError(
            "Aquifer decadal stats are missing required columns: "
            f"{sorted(missing)}"
        )

    use_mean_sum = "mean_sum_anom" in aq_df.columns
    use_ratios = {"mean_ratio_neg", "mean_ratio_pos"}.issubset(aq_df.columns)
    if not (use_mean_sum or use_ratios):
        raise ValueError(
            "Aquifer decadal stats must contain either 'mean_sum_anom' or "
            "both 'mean_ratio_neg' and 'mean_ratio_pos'."
        )

    records: list[dict] = []

    for aquifer_id, grp in aq_df.groupby("AQUIFER_ID"):
        # Filter by minimum station count per decade
        g = grp[grp["n_stn_decade"] >= cfg.trend_min_stations_per_decade].copy()
        g = g.sort_values("Decade")
        if len(g) < cfg.trend_min_decades_per_aquifer:
            continue

        x = g["Decade"].values.astype(float)
        if use_mean_sum:
            y = g["mean_sum_anom"].values.astype(float)
        else:
            y = (g["mean_ratio_neg"] + g["mean_ratio_pos"]).values.astype(float)

        mask = np.isfinite(x) & np.isfinite(y)
        x = x[mask]
        y = y[mask]
        if len(x) < cfg.trend_min_decades_per_aquifer:
            continue

        slope = theil_sen_slope(x, y)
        tau, S, p = mann_kendall_test(y)
        if not np.isfinite(slope) or not np.isfinite(p):
            continue

        # MK anlamlılığa göre sınıf
        if (p < cfg.trend_alpha) and (slope < 0):
            trend_class = "significant_negative"
        elif (p < cfg.trend_alpha) and (slope > 0):
            trend_class = "significant_positive"
        else:
            trend_class = "no_significant_trend"

        # Yeni: sadece eğim yönüne göre sınıf
        if slope < 0:
            trend_sign = "negative"
        elif slope > 0:
            trend_sign = "positive"
        else:
            trend_sign = "zero"

        record = {
            "AQUIFER_ID": aquifer_id,
            "n_decades_used": int(len(x)),
            "sen_slope": float(slope),
            "mk_tau": float(tau),
            "mk_S": float(S),
            "mk_p": float(p),
            "trend_class": trend_class,
            "trend_sign": trend_sign,        # <-- yeni kolon
            "mean_n_stn_decade": float(g["n_stn_decade"].mean()),
        }

        # Optional: propagate region information if available
        if "Region" in g.columns:
            region_mode = g["Region"].mode()
            if not region_mode.empty:
                record["Region"] = region_mode.iloc[0]

        records.append(record)

    trend_df = pd.DataFrame.from_records(records)
    return trend_df


def compute_aquifer_trends_for_periods(
    aq_df: pd.DataFrame,
    cfg: Config,
    periods: list[tuple[str, int, int]] = MULTI_TREND_PERIODS,
) -> dict[str, pd.DataFrame]:
    """
    Çok dönemli akifer trend analizi.

    Her (label, start_decade, end_decade) üçlüsü için:
        - Decade ∈ [start_decade, end_decade] olan satırlar seçilir
        - compute_aquifer_trends(...) ile trend hesaplanır
        - Period_label, Period_start, Period_end kolonları eklenir

    Geriye {label: trend_df} sözlüğü döner.
    """
    results: dict[str, pd.DataFrame] = {}

    for label, d_start, d_end in periods:
        sub = aq_df[
            (aq_df["Decade"] >= d_start) & (aq_df["Decade"] <= d_end)
        ].copy()
        if sub.empty:
            print(f"   ! No decadal data for period {label} ({d_start}–{d_end}).")
            continue

        trend_df = compute_aquifer_trends(sub, cfg)
        if trend_df.empty:
            print(f"   ! No aquifer trends for period {label} after filtering.")
            continue

        trend_df["Period_label"] = label
        trend_df["Period_start"] = d_start
        trend_df["Period_end"] = d_end

        results[label] = trend_df

    return results


def compute_decadal_frac_series(
    aq_df: pd.DataFrame,
    cfg: Config,
) -> pd.DataFrame:
    """
    Compute decadal, region-aggregated fractions of negative and positive
    anomaly years for F7, using aquifer_decadal_stats.csv.

    For each aquifer–decade record we expect:
        - mean_frac_neg   : fraction of years in the decade with negative anomaly
        - mean_frac_pos   : fraction of years in the decade with positive anomaly
        - n_stn_decade    : number of stations contributing in that aquifer–decade

    We compute weighted means (weights = n_stn_decade) by:
        - Region_group (US / Europe / Other world)
        - Global (all aquifers)
    """
    required = {
        "AQUIFER_ID",
        "Decade",
        "n_stn_decade",
        "mean_frac_neg",
        "mean_frac_pos",
    }
    missing = required - set(aq_df.columns)
    if missing:
        raise ValueError(
            "aquifer_decadal_stats.csv is missing required columns for "
            f"fraction analysis: {sorted(missing)}"
        )

    df = aq_df.copy()

    # Attach centroids so we can classify US / Europe / Other world
    try:
        df = attach_aquifer_centroids(df, cfg)
    except Exception as e:
        print(
            "[WARN] Could not attach centroids for frac_neg/frac_pos analysis; "
            f"will compute only global series. Details: {e}"
        )
        df["Region_group"] = "Global"
        has_regional = False
    else:
        def _region_from_xy(lon: float, lat: float) -> str:
            b_us = REGION_BOUNDS["US"]
            b_eu = REGION_BOUNDS["Europe"]

            if (
                (lon >= b_us["lon_min"])
                and (lon <= b_us["lon_max"])
                and (lat >= b_us["lat_min"])
                and (lat <= b_us["lat_max"])
            ):
                return "US"

            if (
                (lon >= b_eu["lon_min"])
                and (lon <= b_eu["lon_max"])
                and (lat >= b_eu["lat_min"])
                and (lat <= b_eu["lat_max"])
            ):
                return "Europe"

            return "Other world"

        df["Region_group"] = [
            _region_from_xy(lon, lat) for lon, lat in zip(df["lon"].values, df["lat"].values)
        ]
        has_regional = True

    # Keep only rows with finite fractions and positive weights
    df = df[
        (df["n_stn_decade"] > 0)
        & np.isfinite(df["mean_frac_neg"])
        & np.isfinite(df["mean_frac_pos"])
    ].copy()

    if df.empty:
        print("   ! No valid rows to compute decadal negative/positive fractions.")
        return pd.DataFrame()

    records: list[dict] = []

    # --- Regional (US / Europe / Other world) weighted means ---
    if has_regional:
        for (decade, reg), g in df.groupby(["Decade", "Region_group"]):
            w = g["n_stn_decade"].to_numpy(dtype=float)
            fn = g["mean_frac_neg"].to_numpy(dtype=float)
            fp = g["mean_frac_pos"].to_numpy(dtype=float)

            w_sum = np.sum(w)
            if not np.isfinite(w_sum) or w_sum <= 0.0:
                continue

            records.append(
                {
                    "Decade": int(decade),
                    "Region_group": reg,
                    "mean_frac_neg_w": float(np.sum(fn * w) / w_sum),
                    "mean_frac_pos_w": float(np.sum(fp * w) / w_sum),
                    "n_aquifers": int(len(g)),
                    "total_n_stn_decade": float(w_sum),
                }
            )

    # --- Global weighted means per decade (all aquifers) ---
    for decade, g in df.groupby("Decade"):
        w = g["n_stn_decade"].to_numpy(dtype=float)
        fn = g["mean_frac_neg"].to_numpy(dtype=float)
        fp = g["mean_frac_pos"].to_numpy(dtype=float)

        w_sum = np.sum(w)
        if not np.isfinite(w_sum) or w_sum <= 0.0:
            continue

        records.append(
            {
                "Decade": int(decade),
                "Region_group": "Global",
                "mean_frac_neg_w": float(np.sum(fn * w) / w_sum),
                "mean_frac_pos_w": float(np.sum(fp * w) / w_sum),
                "n_aquifers": int(len(g)),
                "total_n_stn_decade": float(w_sum),
            }
        )

    out = pd.DataFrame.from_records(records)
    if out.empty:
        print("   ! No decadal fraction statistics could be computed.")
        return out

    out = out.sort_values(["Decade", "Region_group"]).reset_index(drop=True)
    return out


def plot_decadal_fraction_negative_positive(
    decadal_frac: pd.DataFrame,
    cfg: Config,
) -> None:
    """
    F7: 'Share of negative vs positive anomaly years' – line + marker plot.

    x-axis: Decade
    y-axis: Weighted mean fraction of negative / positive anomaly years

    Two stacked panels:
        (a) mean_frac_neg_w
        (b) mean_frac_pos_w

    Series:
        - Global (teal)
        - US (purple)
        - Europe (orange)

    Stil:
        - Çizgi: ana rengin daha açık tonu
        - Nokta: aynı rengin daha koyu/orijinal tonu
    """
    if decadal_frac is None or decadal_frac.empty:
        print("   ! No decadal fraction data available for F7.")
        return

    # Sadece Global / US / Europe
    mask = decadal_frac["Region_group"].isin(["Global", "US", "Europe"])
    df = decadal_frac[mask].copy()
    if df.empty:
        print("   ! No Global/US/Europe rows for F7; skipping.")
        return

    decades = np.sort(df["Decade"].unique())
    if decades.size == 0:
        print("   ! No decades found for F7.")
        return

    # Legend sırası
    region_order = ["Global", "US", "Europe"]
    region_labels = {
        "Global": "Global",
        "US": "US",
        "Europe": "Europe",
    }
    # Temel (koyu) renkler – marker için
    region_colors = {
        "Global": COLOR_REGION_GLOBAL,
        "US": COLOR_REGION_US,
        "Europe": COLOR_REGION_EUROPE,
    }

    fig, (ax_neg, ax_pos) = plt.subplots(
        2,
        1,
        figsize=(6.5, 5.0),
        sharex=True,
    )

    # Arka planda üç ana dönemi taralı göster (MULTI_TREND_PERIODS)
    for (label, d_start, d_end) in MULTI_TREND_PERIODS:
        ax_neg.axvspan(d_start, d_end, color="0.92", alpha=0.4, zorder=0)
        ax_pos.axvspan(d_start, d_end, color="0.92", alpha=0.4, zorder=0)

    # Çizim sırası: önce US ve Europe, en son Global (böylece global üstte kalır)
    draw_order = ["US", "Europe", "Global"]
    line_handles: dict[str, plt.Line2D] = {}

    for reg in draw_order:
        sub = df[df["Region_group"] == reg].copy()
        if sub.empty:
            continue
        sub = sub.sort_values("Decade")

        x = sub["Decade"].values
        y_neg = sub["mean_frac_neg_w"].values
        y_pos = sub["mean_frac_pos_w"].values

        base_col = region_colors[reg]
        line_col = lighten_color(base_col, factor=0.4)   # daha açık ton
        marker_col = base_col                            # daha koyu/orijinal

        # Negatif pay paneli
        line_neg = ax_neg.plot(
            x,
            y_neg,
            color=line_col,
            linewidth=1.6,
            zorder=1.5,
        )[0]
        ax_neg.scatter(
            x,
            y_neg,
            s=18,
            facecolor=marker_col,
            edgecolor=marker_col,
            linewidth=0.3,
            zorder=2.0,
        )

        # Pozitif pay paneli
        ax_pos.plot(
            x,
            y_pos,
            color=line_col,
            linewidth=1.6,
            zorder=1.5,
        )
        ax_pos.scatter(
            x,
            y_pos,
            s=18,
            facecolor=marker_col,
            edgecolor=marker_col,
            linewidth=0.3,
            zorder=2.0,
        )

        # Legend için sadece üst panelde line handle saklıyoruz
        line_handles[reg] = line_neg

    # Eksen ayarları
    x_min = decades.min() - 5
    x_max = decades.max() + 5
    ax_neg.set_xlim(x_min, x_max)

    for ax in (ax_neg, ax_pos):
        ax.set_ylim(0.0, 1.0)
        ax.grid(True, alpha=0.3, zorder=-1)
        ax.tick_params(labelsize=8)

    ax_neg.set_ylabel(
        "Mean fraction of\nnegative anomaly years",
        fontsize=9,
    )
    ax_pos.set_ylabel(
        "Mean fraction of\npositive anomaly years",
        fontsize=9,
    )
    ax_pos.set_xlabel("Decade", fontsize=9)

    # Legend: Global önce, sonra US ve Europe
    handles = [line_handles[r] for r in region_order if r in line_handles]
    labels = [region_labels[r] for r in region_order if r in line_handles]
    ax_neg.legend(
        handles,
        labels,
        fontsize=8,
        frameon=False,
        loc="upper left",
        ncol=1,
    )

    # Alt panelde dönem etiketleri (1900–1959 vb.)
    for (label, d_start, d_end) in MULTI_TREND_PERIODS:
        x_c = 0.5 * (d_start + d_end)
        ax_pos.text(
            x_c,
            1.02,
            label,
            ha="center",
            va="bottom",
            fontsize=8,
            transform=ax_pos.get_xaxis_transform(),
        )

    fig.suptitle(
        "Share of negative vs positive anomaly years (aquifer-level, decadal)",
        fontsize=10,
    )

    fig.tight_layout()
    save_figure(fig, cfg, basename="F7_decadal_negative_positive_fractions")
    plt.close(fig)


def plot_decadal_station_mean_anomaly_distributions(
    decadal_df: pd.DataFrame,
    cfg: Config,
) -> None:
    """
    F13: 'Decadal distribution of station mean anomalies'

    - Tüm dönem (overall)
    - MULTI_TREND_PERIODS içindeki üç alt dönem

    Her dönem için ayrı bir F13 figürü üretir.
    Her figürde 3 panel (global, US, Europe) ve
    her panelde y-ekseninde o panelin gözlenen aralığı kullanılır.
    """
    if decadal_df is None or decadal_df.empty:
        print("   ! No station-decadal statistics available; skipping F13.")
        return

    required = {"sum_anom", "n_anom", "Decade"}
    if not required.issubset(decadal_df.columns):
        print(
            "   ! Required columns (sum_anom, n_anom, Decade) missing; "
            "skipping F13."
        )
        return

    df = decadal_df.copy()
    # Sıfır yılı olmayan onyıllar
    df = df[df["n_anom"] > 0].copy()
    df["mean_anom"] = df["sum_anom"] / df["n_anom"]
    df = df[np.isfinite(df["mean_anom"])]

    if df.empty:
        print("   ! All mean anomalies are non-finite; skipping F13.")
        return

    # Region yoksa hepsini 'global' kabul et
    if "Region" not in df.columns:
        df["Region"] = "global"

    region_colors = {
        "global": COLOR_REGION_GLOBAL,
        "US": COLOR_REGION_US,
        "Europe": COLOR_REGION_EUROPE,
    }

    # --- Dönem listesi: overall + üç alt dönem ---
    periods: list[tuple[str, int | None, int | None, str]] = []

    # Overall dönem (veriden otomatik)
    overall_min_dec = int(df["Decade"].min())
    overall_max_dec = int(df["Decade"].max())
    periods.append(("overall", None, None, f"{overall_min_dec}–{overall_max_dec}"))

    # MULTI_TREND_PERIODS: [(label, d_start, d_end), ...]
    for label, d_start, d_end in MULTI_TREND_PERIODS:
        periods.append((label, d_start, d_end, label))

    regions = ["global", "US", "Europe"]

    for label, d_start, d_end, period_label in periods:
        df_p = df.copy()
        if d_start is not None and d_end is not None:
            df_p = df_p[(df_p["Decade"] >= d_start) & (df_p["Decade"] <= d_end)]

        if df_p.empty:
            print(f"   ! No data for period '{period_label}' in F13; skipping.")
            continue

        decades = sorted(df_p["Decade"].unique())
        if len(decades) == 0:
            print(f"   ! No decades found for period '{period_label}' in F13; skipping.")
            continue

        fig, axes = plt.subplots(
            len(regions),
            1,
            figsize=(8.0, 7.5),
            sharex=True,
        )

        for ax, reg in zip(axes, regions):
            if reg == "global":
                reg_df = df_p.copy()
            else:
                reg_df = df_p[df_p["Region"] == reg].copy()

            ax.set_title(reg, loc="left", fontsize=9)

            if reg_df.empty:
                ax.text(
                    0.5,
                    0.5,
                    "no data",
                    transform=ax.transAxes,
                    ha="center",
                    va="center",
                    fontsize=8,
                )
                ax.grid(True, axis="y", alpha=0.3, linewidth=0.5)
                continue

            data = [
                reg_df.loc[reg_df["Decade"] == dec, "mean_anom"].values
                for dec in decades
            ]

            color = region_colors[reg]
            fill_color = lighten_color(color, 0.5)

            # Violinler: sadece dağılım, extrema yok, median çizgisi var
            parts = ax.violinplot(
                data,
                positions=decades,
                widths=6.0,
                showmedians=True,
                showextrema=False,
            )

            for pc in parts["bodies"]:
                pc.set_facecolor(fill_color)
                pc.set_edgecolor(color)
                pc.set_alpha(0.8)

            if "cmedians" in parts:
                parts["cmedians"].set_color(color)
                parts["cmedians"].set_linewidth(1.0)

            # Y-ekseninde gözlenen aralık (panel bazında)
            y_min = np.nanmin(reg_df["mean_anom"].values)
            y_max = np.nanmax(reg_df["mean_anom"].values)
            if not np.isfinite(y_min) or not np.isfinite(y_max):
                y_min, y_max = -1.0, 1.0
            if np.isclose(y_min, y_max):
                delta = 0.1 if y_min == 0 else abs(y_min) * 0.2
                y_min -= delta
                y_max += delta
            pad = 0.05 * (y_max - y_min)
            ax.set_ylim(y_min - pad, y_max + pad)

            ax.axhline(0.0, color="0.5", linewidth=0.8, linestyle="--")
            ax.set_ylabel("mean anomaly (m)")
            ax.grid(True, axis="y", alpha=0.3, linewidth=0.5)

            # Konya kodundaki zaman serisi eksen stiline yakınlaştırma
            style_ts_axis(ax)

        axes[-1].set_xlabel("Decade")
        axes[-1].set_xticks(decades)
        axes[-1].set_xticklabels([str(d) for d in decades], rotation=45)

        fig.suptitle(
            f"Decadal distribution of station mean anomalies ({period_label})",
            fontsize=11,
        )
        fig.tight_layout(rect=[0, 0, 1, 0.96])

        if label == "overall":
            basename = "F13_decadal_station_mean_anomalies"
        else:
            basename = f"F13_decadal_station_mean_anomalies_{d_start}_{d_end}"

        save_figure(fig, cfg, basename)
        plt.close(fig)


def summarize_trends_by_region(
    trend_df: pd.DataFrame,
    label: str,
    cfg: Config | None = None,
) -> None:
    """
    Region x trend_class matrisi (adet ve yüzde) basar
    ve istenirse özet dosyasına da yazar.
    """
    if trend_df.empty:
        msg = f"   ! No trends for {label}"
        print(msg)
        if cfg is not None:
            append_to_summary_log(cfg, msg)
        return

    if "Region" not in trend_df.columns:
        trend_df = trend_df.assign(Region="Global")

    tbl = (
        trend_df
        .groupby(["Region", "trend_class"])
        .size()
        .unstack(fill_value=0)
        .sort_index()
    )

    # Toplam ve yüzde
    totals = tbl.sum(axis=1)
    pct = (tbl.T / totals).T * 100.0

    tbl_str = tbl.astype(int).to_string()
    pct_str = pct.round(1).to_string()

    lines: list[str] = []
    lines.append(f"=== Trend-class summary: {label} ===")
    lines.append(tbl_str)
    lines.append("")
    lines.append("(Percentages %)")
    lines.append(pct_str)
    lines.append("====================================")

    block = "\n".join(lines)

    # Konsol
    print("\n" + block + "\n")

    # Özet dosyası
    if cfg is not None:
        append_to_summary_log(cfg, block)


def attach_aquifer_centroids(
    trend_df: pd.DataFrame,
    cfg: Config,
) -> pd.DataFrame:
    """
    AQUIFER_ID (shapefile indeksi) üzerinden her akifere yaklaşık centroid
    (lon, lat) ekler. F3 haritasındaki centroid hesabıyla aynıdır.

    Dönüş:
        trend_df kopyası + 'lon' ve 'lat' kolonları.
    """
    if cfg.aquifer_shapefile is None:
        raise ValueError("Config.aquifer_shapefile is None; cannot attach centroids.")

    shp_path = cfg.aquifer_shapefile
    if not os.path.exists(shp_path):
        raise FileNotFoundError(f"Aquifer shapefile not found: {shp_path}")

    sf = shapefile.Reader(shp_path)
    n_shapes = len(sf.shapes())
    if n_shapes == 0:
        raise ValueError("Aquifer shapefile contains no shapes.")

    df = trend_df.copy()
    if "AQUIFER_ID" not in df.columns:
        raise ValueError("trend_df must contain 'AQUIFER_ID' column.")

    df["AQUIFER_ID_int"] = df["AQUIFER_ID"].astype(int)
    df = df[
        (df["AQUIFER_ID_int"] >= 0)
        & (df["AQUIFER_ID_int"] < n_shapes)
    ].copy()

    lons = []
    lats = []

    for idx in df["AQUIFER_ID_int"]:
        try:
            shp = sf.shape(int(idx))
        except IndexError:
            lons.append(np.nan)
            lats.append(np.nan)
            continue

        x_min, y_min, x_max, y_max = shp.bbox
        lon = 0.5 * (x_min + x_max)
        lat = 0.5 * (y_min + y_max)

        lons.append(lon)
        lats.append(lat)

    df["lon"] = lons
    df["lat"] = lats

    # Geçersiz centroid’leri at
    df = df[np.isfinite(df["lon"]) & np.isfinite(df["lat"])].copy()
    return df


def plot_aquifer_anomaly_stripes(
    aq_df: pd.DataFrame,
    cfg: Config,
) -> None:
    """
    F12: Aquifer anomaly 'stripes' – Global, US ve Europe için ayrı heatmap panelleri.

    Satır: akifer (AQUIFER_ID)
    Sütun: onyıl (Decade)
    Renk: anomaly metriği (tercihen mean_sum_anom; yoksa mean_ratio_neg + mean_ratio_pos)

    Yalnızca n_stn_decade >= trend_min_stations_per_decade olan onyıllar kullanılır.
    """

    if aq_df.empty:
        print("   ! F12: aquifer_decadal_stats is empty; skipping stripes.")
        return

    # Hangi metrik kullanılacak?
    use_mean_sum = "mean_sum_anom" in aq_df.columns
    use_ratios = {"mean_ratio_neg", "mean_ratio_pos"}.issubset(aq_df.columns)

    if use_mean_sum:
        metric_col = "mean_sum_anom"
        metric_label = "Decadal mean anomaly"
    elif use_ratios:
        metric_col = "_metric_stripes"
        aq_df = aq_df.copy()
        aq_df[metric_col] = aq_df["mean_ratio_neg"] + aq_df["mean_ratio_pos"]
        metric_label = "Decadal anomaly (ratio_neg + ratio_pos)"
    else:
        print("   ! F12: No suitable anomaly metric in aquifer_decadal_stats; skipping.")
        return

    # İstasyon sayısı filtresi
    df = aq_df[aq_df["n_stn_decade"] >= cfg.trend_min_stations_per_decade].copy()
    if df.empty:
        print("   ! F12: No decadal records after n_stn_decade filtering; skipping.")
        return

    # Decade eksenini belirle
    decades = np.sort(df["Decade"].unique())
    n_decades = len(decades)
    if n_decades == 0:
        print("   ! F12: No Decade values found; skipping.")
        return

    # Bölgesel ayrım için centroid + bbox (US / Europe)
    have_regional = False
    try:
        df_cent = attach_aquifer_centroids(
            df[["AQUIFER_ID"]].drop_duplicates("AQUIFER_ID"),
            cfg,
        )
        # AQUIFER_ID -> (lon, lat) sözlüğü
        centroids = (
            df_cent[["AQUIFER_ID", "lon", "lat"]]
            .drop_duplicates("AQUIFER_ID")
            .set_index("AQUIFER_ID")
        )
        df = df.merge(
            centroids,
            on="AQUIFER_ID",
            how="left",
            suffixes=("", "_cent"),
        )

        def _region_from_xy(lon: float, lat: float) -> str:
            if not np.isfinite(lon) or not np.isfinite(lat):
                return "Other world"
            for reg_name, bounds in REGION_BOUNDS.items():
                if reg_name == "global":
                    continue
                if (
                    (lon >= bounds["lon_min"]) and (lon <= bounds["lon_max"])
                    and (lat >= bounds["lat_min"]) and (lat <= bounds["lat_max"])
                ):
                    return reg_name
            return "Other world"

        df["Region_group"] = [
            _region_from_xy(lon, lat) for lon, lat in zip(df["lon"], df["lat"])
        ]
        have_regional = True
    except Exception as e:
        print(f"   ! F12: Could not attach centroids for regional stripes ({e}).")
        df["Region_group"] = "Other world"

    # Renk skalasını globalden (tüm akiferler) belirle – simetrik
    all_vals = df[metric_col].to_numpy(dtype=float)
    all_vals = all_vals[np.isfinite(all_vals)]
    if all_vals.size == 0:
        print("   ! F12: No finite anomaly values for stripes; skipping.")
        return

    try:
        vlim = float(np.nanpercentile(np.abs(all_vals), 99.0))
    except Exception:
        vlim = float(np.nanmax(np.abs(all_vals)))

    if not np.isfinite(vlim) or vlim <= 0:
        vlim = float(np.nanmax(np.abs(all_vals))) if all_vals.size > 0 else 1.0

    if vlim <= 0:
        vlim = 1.0

    vmin, vmax = -vlim, vlim

    # Hangi paneller çizilecek?
    panel_regions: list[str] = ["global"]
    if have_regional:
        # Eğer gerçekten veri varsa US/Europe panellerini de ekle
        for reg in ["US", "Europe"]:
            if (df["Region_group"] == reg).any():
                panel_regions.append(reg)

    n_panels = len(panel_regions)
    if n_panels == 0:
        print("   ! F12: No panels to draw; skipping.")
        return

    # Figür boyutu – panel sayısına göre dinamik
    fig_height = 1.5 + 1.4 * n_panels
    fig, axes = plt.subplots(
        n_panels,
        1,
        figsize=(7.0, fig_height),
        sharex=True,
        constrained_layout=True,
    )
    if n_panels == 1:
        axes = [axes]

    # Decade -> sütun indeks haritası
    decade_to_idx = {int(d): i for i, d in enumerate(decades)}

    # Özet log için bilgiler
    lines: list[str] = []
    lines.append("=== Aquifer anomaly stripes coverage (F12) ===")
    lines.append(f"   Metric used: {metric_col}")
    lines.append(f"   Decades: {int(decades[0])}–{int(decades[-1])}")

    last_im = None

    for ax, reg in zip(axes, panel_regions):
        if reg == "global":
            df_reg = df.copy()
            title_txt = "Global"
        else:
            df_reg = df[df["Region_group"] == reg].copy()
            title_txt = {"US": "United States", "Europe": "Europe"}.get(reg, reg)

        if df_reg.empty:
            ax.set_visible(False)
            continue

        # Satır: akifer – AQUIFER_ID’ye göre sıralı
        aquifers = sorted(df_reg["AQUIFER_ID"].unique())
        n_aq = len(aquifers)
        lines.append(f"   {title_txt}: n_aquifers={n_aq}")

        mat = np.full((n_aq, n_decades), np.nan, dtype=float)

        for i, aq_id in enumerate(aquifers):
            g = df_reg[df_reg["AQUIFER_ID"] == aq_id]
            for _, row in g.iterrows():
                dec = int(row["Decade"])
                j = decade_to_idx.get(dec)
                if j is None:
                    continue
                val = float(row[metric_col])
                if np.isfinite(val):
                    mat[i, j] = val

        im = ax.imshow(
            mat,
            aspect="auto",
            interpolation="nearest",
            origin="lower",
            cmap="RdBu_r",
            vmin=vmin,
            vmax=vmax,
        )
        last_im = im

        ax.set_title(title_txt, loc="left", fontsize=9)
        ax.set_yticks([])
        ax.set_ylabel("Aquifer index", fontsize=8)

        # Ortak stil
        ax.tick_params(axis="both", which="major", labelsize=8)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)

    # X-ekseni: onyıllar
    xticks_idx = np.arange(0, n_decades, max(1, n_decades // 8))
    xtick_labels = [str(int(decades[i])) for i in xticks_idx]
    axes[-1].set_xticks(xticks_idx)
    axes[-1].set_xticklabels(xtick_labels, rotation=45)
    axes[-1].set_xlabel("Decade", fontsize=9)

    # Ortak colorbar
    if last_im is not None:
        cbar = fig.colorbar(last_im, ax=axes, orientation="vertical", fraction=0.035, pad=0.02)
        cbar.set_label(
            f"{metric_label}",
            fontsize=9,
        )
        cbar.ax.tick_params(labelsize=8)

    # Özet log’a yaz
    lines.append("=" * 60)
    block = "\n".join(lines)
    print("\n" + block + "\n")
    append_to_summary_log(cfg, block)

    save_figure(fig, cfg, "F12_aquifer_anomaly_stripes")
    plt.close(fig)


def identify_negative_slope_hotspots(
    trend_df: pd.DataFrame,
    cfg: Config,
    label: str,
    period_tag: str | None = None,
) -> None:
    """
    En belirgin negatif eğim kümelerini (hotspot) arar ve raporlar.

    Adımlar:
        1) trend_sign == 'negative' olan akiferleri seç.
        2) |sen_slope| için hotspot_abs_slope_quantile üzerini al (daha güçlü eğimler).
        3) Bu akiferleri lon/lat gridine (hotspot_grid_size_deg) yerleştir.
        4) (Region, lon_bin, lat_bin) grupları için:
             - n_aquifers
             - median_slope
             - mean_slope
             - min_slope
             - lon/lat min-max
           hesapla.
        5) n_aquifers >= hotspot_min_count olan hücreleri 'hotspot' say.
        6) En belirgin kümeleri konsola yaz ve CSV olarak kaydet.
    """
    if trend_df.empty:
        print(f"[Hotspots] No trends for {label}")
        return

    if "trend_sign" not in trend_df.columns:
        print(f"[Hotspots] trend_sign column missing for {label}; skipping.")
        return

    df = trend_df.copy()

    # Sadece negatif eğimli akiferler
    df = df[df["trend_sign"] == "negative"].copy()
    df = df[np.isfinite(df["sen_slope"])]
    if df.empty:
        print(f"[Hotspots] No negative slopes for {label}")
        return

    # Mutlak eğime göre "daha güçlü" negatifleri seç
    abs_neg = df["sen_slope"].abs().values
    q = cfg.hotspot_abs_slope_quantile
    try:
        thr = np.nanquantile(abs_neg, q)
    except Exception:
        thr = np.nanmax(abs_neg)

    df = df[df["sen_slope"].abs() >= thr].copy()
    if df.empty:
        print(
            f"[Hotspots] No negative slopes above |slope| quantile {q:.2f} "
            f"for {label}"
        )
        return

    # Centroid ekle (lon/lat)
    try:
        df = attach_aquifer_centroids(df, cfg)
    except Exception as e:
        print(f"[Hotspots] Could not attach centroids for {label}: {e}")
        return

    if df.empty:
        print(f"[Hotspots] No valid centroids for {label}")
        return

    # Region yoksa Global kabul et
    if "Region" not in df.columns:
        df["Region"] = "Global"

    # Grid indeksleri
    gsize = cfg.hotspot_grid_size_deg
    df["lon_bin"] = np.floor(df["lon"] / gsize).astype(int)
    df["lat_bin"] = np.floor(df["lat"] / gsize).astype(int)

    grp = df.groupby(["Region", "lon_bin", "lat_bin"])

    agg = grp.agg(
        n_aquifers=("AQUIFER_ID", "nunique"),
        median_slope=("sen_slope", "median"),
        mean_slope=("sen_slope", "mean"),
        min_slope=("sen_slope", "min"),
        max_slope=("sen_slope", "max"),
        lon_min=("lon", "min"),
        lon_max=("lon", "max"),
        lat_min=("lat", "min"),
        lat_max=("lat", "max"),
    ).reset_index()

    # Yeterli sayıda akifer içeren grid hücreleri
    agg = agg[agg["n_aquifers"] >= cfg.hotspot_min_count].copy()
    if agg.empty:
        print(
            f"[Hotspots] No hotspot cells (>= {cfg.hotspot_min_count} aquifers) "
            f"for {label}"
        )
        return

    # "En belirgin"liği ölçmek için basit skor: n_aquifers * |median_slope|
    agg["score"] = agg["n_aquifers"] * agg["median_slope"].abs()

    # Daha negatif median_slope ve yüksek n_aquifers öncelikli olsun
    agg = agg.sort_values(["score", "n_aquifers", "median_slope"]).iloc[::-1].copy()

    # CSV'ye yaz
    tag = period_tag if period_tag is not None else "overall"
    out_name = f"aquifer_negative_hotspots_{tag}.csv"
    out_path = os.path.join(cfg.anomalies_dir, out_name)
    agg.to_csv(out_path, index=False)

    # Metin bloğunu hem konsola hem log dosyasına yaz
    top_k = min(10, len(agg))

    lines: list[str] = []
    lines.append(
        f"[Hotspots] Negative slope hotspots for {label} written to: {out_path}"
    )
    lines.append("")
    lines.append(f"=== Top {top_k} negative-slope hotspots: {label} ===")

    for _, row in agg.head(top_k).iterrows():
        reg = row["Region"]
        n = int(row["n_aquifers"])
        med = row["median_slope"]
        lon_c = 0.5 * (row["lon_min"] + row["lon_max"])
        lat_c = 0.5 * (row["lat_min"] + row["lat_max"])
        lines.append(
            f" - {reg}: n={n}, median slope={med:.4f}, "
            f"center≈({lon_c:.1f}°, {lat_c:.1f}°), "
            f"lon[{row['lon_min']:.1f},{row['lon_max']:.1f}], "
            f"lat[{row['lat_min']:.1f},{row['lat_max']:.1f}]"
        )

    lines.append("==============================================")

    block = "\n".join(lines)

    print("\n" + block + "\n")
    append_to_summary_log(cfg, block)


def compute_aquifer_asymmetry(
    aq_df: pd.DataFrame,
    cfg: Config,
) -> pd.DataFrame:
    """
    Compute an asymmetry index between negative and positive contributions
    for each aquifer over decadal scales.

    For each aquifer–decade record we define:
        R^- = mean_ratio_neg   (mean decadal contribution from negative anomalies)
        R^+ = mean_ratio_pos   (mean decadal contribution from positive anomalies)

    and the asymmetry index:

        I_asym = ( |R^-| - R^+ ) / ( |R^-| + R^+ )

    so that:
        I_asym ≈ +1  → strongly downward / negative-dominated regime
        I_asym ≈ -1  → strongly upward / positive-dominated regime
        I_asym ≈  0  → symmetric contributions

    Only decades with n_stn_decade >= cfg.trend_min_stations_per_decade are
    used, and an aquifer is included only if it has at least
    cfg.trend_min_decades_per_aquifer valid I_asym values.

    Returned columns:
        - AQUIFER_ID
        - n_decades_asym           (number of decades used)
        - mean_I_asym              (long-term mean asymmetry index)
        - I_asym_trend_slope       (Theil–Sen slope of I_asym vs Decade)
        - (optional) Region        (if region info is present)
        - lon, lat                 (centroid coordinates from aquifer shapefile)
    """
    required = {
        "AQUIFER_ID",
        "Decade",
        "n_stn_decade",
        "mean_ratio_neg",
        "mean_ratio_pos",
    }
    missing = required - set(aq_df.columns)
    if missing:
        raise ValueError(
            "Aquifer decadal stats are missing required columns for asymmetry "
            f"analysis: {sorted(missing)}"
        )

    records: list[dict] = []

    for aquifer_id, grp in aq_df.groupby("AQUIFER_ID"):
        # İstasyon sayısına göre filtre
        g = grp[grp["n_stn_decade"] >= cfg.trend_min_stations_per_decade].copy()
        g = g.sort_values("Decade")
        if g.empty:
            continue

        decades = g["Decade"].to_numpy(dtype=float)
        r_neg = g["mean_ratio_neg"].to_numpy(dtype=float)
        r_pos = g["mean_ratio_pos"].to_numpy(dtype=float)

        # I_asym = (|R^-| - R^+) / (|R^-| + R^+)
        num = np.abs(r_neg) - r_pos
        den = np.abs(r_neg) + r_pos
        i_asym = np.where(den > 0.0, num / den, np.nan)

        mask = np.isfinite(i_asym) & np.isfinite(decades)
        if mask.sum() < cfg.trend_min_decades_per_aquifer:
            continue

        decades_valid = decades[mask]
        i_valid = i_asym[mask]

        mean_i = float(np.nanmean(i_valid))
        slope_i = float(theil_sen_slope(decades_valid, i_valid))

        record = {
            "AQUIFER_ID": aquifer_id,
            "n_decades_asym": int(mask.sum()),
            "mean_I_asym": mean_i,
            "I_asym_trend_slope": slope_i,
        }

        # Eğer aquifer_decadal_stats içinde Region varsa, modunu aktar
        if "Region" in g.columns:
            region_mode = g["Region"].mode()
            if not region_mode.empty:
                record["Region"] = region_mode.iloc[0]

        records.append(record)

    asym_df = pd.DataFrame.from_records(records)
    if asym_df.empty:
        print("   ! No aquifer could be used for asymmetry analysis.")
        return asym_df

    # Centroid (lon, lat) ekle – F3 ve hotspot analizleri ile uyumlu
    try:
        asym_df = attach_aquifer_centroids(asym_df, cfg)
    except Exception as e:
        print(f"   ! Could not attach centroids to asymmetry table: {e}")

    return asym_df


def compute_aquifer_asymmetry_for_periods(
    aq_df: pd.DataFrame,
    cfg: Config,
    periods: list[tuple[str, int, int]] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Convenience wrapper: compute aquifer-level asymmetry indices (I_asym)
    for multiple non-overlapping time windows.

    Returns
    -------
    results : dict
        Dictionary mapping 'label' -> asymmetry DataFrame
        (same schema as compute_aquifer_asymmetry), with extra columns:
        - period_label
        - period_start
        - period_end
    """
    if periods is None:
        periods = MULTI_TREND_PERIODS

    results: dict[str, pd.DataFrame] = {}

    for label, d_start, d_end in periods:
        sub = aq_df[(aq_df["Decade"] >= d_start) & (aq_df["Decade"] <= d_end)].copy()
        if sub.empty:
            print(
                f"[Asymmetry] No aquifer decadal records within {d_start}–{d_end} "
                f"for period '{label}'."
            )
            continue

        print(
            f"[Asymmetry] Computing aquifer asymmetry indices for period "
            f"'{label}' ({d_start}–{d_end})."
        )
        asym = compute_aquifer_asymmetry(sub, cfg)
        if asym is None or asym.empty:
            print(
                f"[Asymmetry] No valid asymmetry indices for period '{label}' "
                f"after filtering."
            )
            continue

        asym = asym.copy()
        asym["period_label"] = label
        asym["period_start"] = d_start
        asym["period_end"] = d_end

        # CSV çıktısı (makale için sayısal referans tablosu)
        out_name = f"aquifer_asymmetry_summary_{d_start}_{d_end}.csv"
        out_path = os.path.join(cfg.anomalies_dir, out_name)
        ensure_dir(cfg.anomalies_dir)
        asym.to_csv(out_path, index=False)
        print(
            f"[Asymmetry] Aquifer asymmetry summary for period '{label}' written to: "
            f"{out_path}"
        )

        results[label] = asym

    return results


def compute_aquifer_persistence(
    aq_df: pd.DataFrame,
    cfg: Config,
) -> pd.DataFrame:
    """
    Compute decadal run-length persistence metrics for each aquifer.

    For each aquifer q we first construct a decadal anomaly metric A_q(d):

        A_q(d) =
            mean_sum_anom(d)                        if available
            mean_ratio_neg(d) + mean_ratio_pos(d)   otherwise

    We then code the sign of A_q(d) as:
        -1 for negative anomaly decades
         0 for zero / undefined (treated as break)
        +1 for positive anomaly decades

    and compute, for each aquifer:
        L_neg_max : maximum run length of consecutive negative decades
        L_pos_max : maximum run length of consecutive positive decades

    Only decades with n_stn_decade >= cfg.trend_min_stations_per_decade
    are used. An aquifer is included only if it has at least
    cfg.trend_min_decades_per_aquifer valid decades.

    Returns
    -------
    pd.DataFrame with columns:
        - AQUIFER_ID
        - n_decades_used
        - first_decade
        - last_decade
        - mean_anom_metric
        - L_neg_max
        - L_pos_max
        - mean_n_stn_decade
    """
    required = {"AQUIFER_ID", "Decade", "n_stn_decade"}
    missing = required - set(aq_df.columns)
    if missing:
        raise ValueError(
            "Aquifer decadal stats are missing required columns for persistence "
            f"analysis: {sorted(missing)}"
        )

    use_mean_sum = "mean_sum_anom" in aq_df.columns
    use_ratios = {"mean_ratio_neg", "mean_ratio_pos"}.issubset(aq_df.columns)
    if not (use_mean_sum or use_ratios):
        raise ValueError(
            "Aquifer decadal stats must contain either 'mean_sum_anom' or "
            "both 'mean_ratio_neg' and 'mean_ratio_pos' to compute persistence."
        )

    records: list[dict] = []

    for aquifer_id, grp in aq_df.groupby("AQUIFER_ID"):
        # Minimum istasyon sayısı filtresi (trend analizi ile aynı)
        g = grp[grp["n_stn_decade"] >= cfg.trend_min_stations_per_decade].copy()
        g = g.sort_values("Decade")
        if len(g) < cfg.trend_min_decades_per_aquifer:
            continue

        x = g["Decade"].values.astype(int)
        if use_mean_sum:
            y = g["mean_sum_anom"].values.astype(float)
        else:
            y = (g["mean_ratio_neg"] + g["mean_ratio_pos"]).values.astype(float)

        # Sadece sonlu değerler
        mask = np.isfinite(y)
        if not np.any(mask):
            continue

        x = x[mask]
        y = y[mask]
        if x.size < cfg.trend_min_decades_per_aquifer:
            continue

        # İşaret kodlama
        s = np.zeros_like(y, dtype=int)
        s[y < 0.0] = -1
        s[y > 0.0] = 1

        def _max_run_length(series: np.ndarray, target: int) -> int:
            max_run = 0
            cur = 0
            for val in series:
                if val == target:
                    cur += 1
                    if cur > max_run:
                        max_run = cur
                else:
                    cur = 0
            return int(max_run)

        L_neg_max = _max_run_length(s, -1)
        L_pos_max = _max_run_length(s, +1)

        rec = {
            "AQUIFER_ID": aquifer_id,
            "n_decades_used": int(x.size),
            "first_decade": int(x.min()),
            "last_decade": int(x.max()),
            "mean_anom_metric": float(np.nanmean(y)),
            "L_neg_max": int(L_neg_max),
            "L_pos_max": int(L_pos_max),
            "mean_n_stn_decade": float(g.loc[mask, "n_stn_decade"].mean()),
        }

        records.append(rec)

    if not records:
        return pd.DataFrame(
            columns=[
                "AQUIFER_ID",
                "n_decades_used",
                "first_decade",
                "last_decade",
                "mean_anom_metric",
                "L_neg_max",
                "L_pos_max",
                "mean_n_stn_decade",
            ]
        )

    return pd.DataFrame.from_records(records)


def compute_aquifer_persistence_for_periods(
    aq_df: pd.DataFrame,
    cfg: Config,
    periods: list[tuple[str, int, int]] = MULTI_TREND_PERIODS,
) -> dict[str, pd.DataFrame]:
    """
    Convenience wrapper: compute aquifer-level persistence metrics
    (L_neg_max, L_pos_max) for multiple time windows.

    Parameters
    ----------
    aq_df : DataFrame
        Aquifer-level decadal statistics (same as for trend analysis).
    cfg : Config
        Configuration.
    periods : list of (label, start_decade, end_decade)
        Time windows in decadal units.

    Returns
    -------
    dict
        {label: persistence_df}, where each persistence_df has the same
        schema as compute_aquifer_persistence(...) and additional columns:
        - period_label
        - period_start
        - period_end
    """
    results: dict[str, pd.DataFrame] = {}
    for label, d_start, d_end in periods:
        sub = aq_df[
            (aq_df["Decade"] >= d_start) & (aq_df["Decade"] <= d_end)
        ].copy()
        if sub.empty:
            continue

        pers = compute_aquifer_persistence(sub, cfg)
        if pers.empty:
            continue

        pers["period_label"] = label
        pers["period_start"] = int(d_start)
        pers["period_end"] = int(d_end)

        results[label] = pers

    return results


def save_aquifer_persistence_outputs(
    aquifer_dec_df: pd.DataFrame,
    cfg: Config,
    periods: list[tuple[str, int, int]] = MULTI_TREND_PERIODS,
) -> dict[str, Any]:
    """
    Write overall and period-specific aquifer persistence summaries to CSV.

    Outputs
    -------
    - cfg.anomalies_dir / aquifer_persistence_summary.csv
    - cfg.anomalies_dir / aquifer_persistence_summary_<start>_<end>.csv

    Returns
    -------
    dict with keys:
        - "overall": overall persistence dataframe
        - "periods": {label: period persistence dataframe}
        - "saved_paths": list of written CSV paths
    """
    ensure_dir(cfg.anomalies_dir)

    saved_paths: list[str] = []

    overall = compute_aquifer_persistence(aquifer_dec_df, cfg)
    if overall is not None and not overall.empty:
        out_path = os.path.join(cfg.anomalies_dir, "aquifer_persistence_summary.csv")
        overall.to_csv(out_path, index=False)
        print(f"   -> Overall aquifer persistence summary written to: {out_path}")
        saved_paths.append(out_path)
    else:
        overall = pd.DataFrame()
        print("   ! No overall aquifer persistence summary could be written.")

    period_results = compute_aquifer_persistence_for_periods(
        aquifer_dec_df,
        cfg,
        periods=periods,
    )

    for label, d_start, d_end in periods:
        p_df = period_results.get(label)
        if p_df is None or p_df.empty:
            continue

        period_tag = f"{d_start}_{d_end}"
        out_path = os.path.join(
            cfg.anomalies_dir,
            f"aquifer_persistence_summary_{period_tag}.csv",
        )
        p_df.to_csv(out_path, index=False)
        print(f"   -> Period '{label}' persistence summary written to: {out_path}")
        saved_paths.append(out_path)

    return {
        "overall": overall,
        "periods": period_results,
        "saved_paths": saved_paths,
    }


def summarize_persistence_by_region(
    aquifer_persist: pd.DataFrame,
    cfg: Config,
    label: str = "overall",
) -> None:
    """
    Print and log a simple regional summary of maximum negative
    decadal run length L_neg_max for:
        - US
        - Europe
        - Other world
        - Global (all aquifers)

    Region classification is based on centroid (lon, lat) and REGION_BOUNDS.
    """
    if aquifer_persist is None or aquifer_persist.empty:
        msg = f"[Persistence] No aquifer persistence data for {label}."
        print(msg)
        append_to_summary_log(cfg, msg)
        return

    df = aquifer_persist.copy()

    # Centroid yoksa shapefile üzerinden ekle
    if ("lon" not in df.columns) or ("lat" not in df.columns):
        try:
            df = attach_aquifer_centroids(df, cfg)
        except Exception as e:
            msg = f"[Persistence] Could not attach centroids for {label}: {e}"
            print(msg)
            append_to_summary_log(cfg, msg)
            return

    if df.empty:
        msg = f"[Persistence] No valid centroids for {label}."
        print(msg)
        append_to_summary_log(cfg, msg)
        return

    def _region_from_xy(lon: float, lat: float) -> str:
        b_us = REGION_BOUNDS["US"]
        b_eu = REGION_BOUNDS["Europe"]

        if (
            (lon >= b_us["lon_min"])
            and (lon <= b_us["lon_max"])
            and (lat >= b_us["lat_min"])
            and (lat <= b_us["lat_max"])
        ):
            return "US"

        if (
            (lon >= b_eu["lon_min"])
            and (lon <= b_eu["lon_max"])
            and (lat >= b_eu["lat_min"])
            and (lat <= b_eu["lat_max"])
        ):
            return "Europe"

        return "Other world"

    df["Region_group"] = [
        _region_from_xy(lon, lat) for lon, lat in zip(df["lon"].values, df["lat"].values)
    ]

    # Basit özet istatistik fonksiyonu
    def _summarize(sub: pd.DataFrame) -> tuple[int, float, float, float, float]:
        n = int(len(sub))
        if n == 0:
            return 0, np.nan, np.nan, np.nan, np.nan
        L = sub["L_neg_max"].to_numpy(dtype=float)
        mean_L = float(np.nanmean(L))
        median_L = float(np.nanmedian(L))
        frac_ge2 = float(np.mean(L >= 2.0)) * 100.0
        frac_ge3 = float(np.mean(L >= 3.0)) * 100.0
        return n, mean_L, median_L, frac_ge2, frac_ge3

    regions = ["US", "Europe", "Other world"]
    lines: list[str] = []

    lines.append(
        f"=== Aquifer persistence summary (maximum negative decadal run length, L^-_max): {label} ==="
    )
    lines.append("")
    lines.append(
        "Region_group  n  mean(L^-_max)  median(L^-_max)  %L^-_max>=2  %L^-_max>=3"
    )

    for reg in regions:
        sub = df[df["Region_group"] == reg]
        n, mean_L, median_L, frac_ge2, frac_ge3 = _summarize(sub)
        if n == 0:
            continue
        lines.append(
            f"{reg:11s}  {n:3d}  {mean_L:11.2f}  {median_L:14.2f}  {frac_ge2:9.1f}  {frac_ge3:9.1f}"
        )

    # Global (tüm akiferler)
    n_g, mean_g, med_g, frac2_g, frac3_g = _summarize(df)
    lines.append("")
    lines.append(
        f"{'Global':11s}  {n_g:3d}  {mean_g:11.2f}  {med_g:14.2f}  {frac2_g:9.1f}  {frac3_g:9.1f}"
    )
    lines.append("=" * 60)

    block = "\n".join(lines)
    print("\n" + block + "\n")
    append_to_summary_log(cfg, block)


def plot_aquifer_persistence_map(
    aquifer_persist: pd.DataFrame,
    cfg: Config,
    region: str = "global",
    world_gdf: gpd.GeoDataFrame | None = None,
    title_suffix: str | None = "1900–2020",
    filename_suffix: str | None = None,
) -> None:
    """
    F8: 'Maximum decadal persistence of negative anomalies' – aquifer map.

    Colour encodes, for each aquifer, the maximum run length of consecutive
    negative anomaly decades:

        L_neg_max = max run length of A_q(d) < 0

    We clip L_neg_max at 5 decades for visualisation (values >5 are shown
    at the top of the colour scale). Aquifers with L_neg_max == 0 (i.e. no
    negative decades in the window) are shown in light grey.
    """
    if aquifer_persist is None or aquifer_persist.empty:
        print("   ! No aquifer persistence data provided to F8.")
        return

    df = aquifer_persist.copy()

    # Centroid yoksa shapefile üzerinden ekle
    if ("lon" not in df.columns) or ("lat" not in df.columns):
        try:
            df = attach_aquifer_centroids(df, cfg)
        except Exception as e:
            print(f"[WARN] Could not attach centroids for F8: {e}")
            return

    if df.empty:
        print("   ! No valid centroids for F8.")
        return

    # --- REGION_BOUNDS anahtarlarına uygun normalize et ---
    reg_map = {
        "global": "global",
        "Global": "global",
        "world": "global",
        "World": "global",
        "US": "US",
        "us": "US",
        "Us": "US",
        "Europe": "Europe",
        "europe": "Europe",
        "EU": "Europe",
        "eu": "Europe",
    }
    region_key = reg_map.get(region, region)

    if region_key not in REGION_BOUNDS:
        raise ValueError(f"Unknown region '{region}' for F8 map.")

    bounds = REGION_BOUNDS[region_key]

    mask = (
        (df["lon"] >= bounds["lon_min"])
        & (df["lon"] <= bounds["lon_max"])
        & (df["lat"] >= bounds["lat_min"])
        & (df["lat"] <= bounds["lat_max"])
    )
    sub = df[mask].copy()
    if sub.empty:
        print(f"   ! No aquifers fall into region '{region_key}' for F8.")
        return

    L = sub["L_neg_max"].to_numpy(dtype=float)
    lon = sub["lon"].to_numpy(dtype=float)
    lat = sub["lat"].to_numpy(dtype=float)

    # L_neg_max == 0 (negatif onyıl yok) ve L_neg_max > 0 ayrımı
    mask_pos = L > 0.0

    fig, ax = plt.subplots(figsize=(7.0, 3.8))

    # Vektör world basemap (F3/F6 ile aynı yaklaşım)
    if world_gdf is not None:
        try:
            w = world_gdf
            # Burada kaba bir bbox filtresi kullanıyoruz; w.bounds bir DataFrame
            b = w.bounds
            w_mask = (
                (b["minx"] <= bounds["lon_max"])
                & (b["maxx"] >= bounds["lon_min"])
                & (b["miny"] <= bounds["lat_max"])
                & (b["maxy"] >= bounds["lat_min"])
            )
            w_sub = w[w_mask]
            w_sub.plot(
                ax=ax,
                facecolor="none",
                edgecolor="0.7",
                linewidth=0.6,
                zorder=0,
            )
        except Exception as e:
            print(f"[WARN] Could not plot world basemap in F8: {e}")

    # Negatif onyılı hiç olmayan akiferler (L_neg_max == 0) – açık gri
    if np.any(~mask_pos):
        ax.scatter(
            lon[~mask_pos],
            lat[~mask_pos],
            s=18.0,
            facecolor="0.90",
            edgecolor="0.70",
            linewidth=0.3,
            zorder=1.0,
        )

    # En az bir negatif onyılı olan akiferler
    if np.any(mask_pos):
        L_clip = np.clip(L[mask_pos], 1.0, 5.0)
        sc = ax.scatter(
            lon[mask_pos],
            lat[mask_pos],
            c=L_clip,
            cmap=plt.get_cmap("YlOrRd"),
            vmin=1.0,
            vmax=5.0,
            s=26.0,
            alpha=0.9,
            linewidths=0.35,
            edgecolors=COLOR_TREND_OUTLINE,
            zorder=2.0,
        )
        cbar = plt.colorbar(sc, ax=ax, orientation="vertical", pad=0.02)
        cbar.set_label(
            r"Max. negative decadal run length $L^{-}_{\max}$ (decades, clipped at 5)",
            fontsize=9,
        )
        cbar.ax.tick_params(labelsize=8)

    ax.set_xlim(bounds["lon_min"], bounds["lon_max"])
    ax.set_ylim(bounds["lat_min"], bounds["lat_max"])
    ax.set_xlabel("Longitude (°)", fontsize=9)
    ax.set_ylabel("Latitude (°)", fontsize=9)
    ax.tick_params(labelsize=8)

    ax.grid(True, linestyle="--", linewidth=0.3, alpha=0.4, zorder=-1)

    region_title_map = {
        "global": "Global",
        "US": "US",
        "Europe": "Europe",
    }
    region_title = region_title_map.get(region_key, region_key)

    if title_suffix:
        title = f"{region_title} maximum decadal persistence of negative anomalies ({title_suffix})"
    else:
        title = f"{region_title} maximum decadal persistence of negative anomalies"

    ax.set_title(title, fontsize=10)

    fig.tight_layout()

    basename = f"F8_aquifer_persistence_Lneg_{region_key.lower()}"
    if filename_suffix:
        basename = f"{basename}_{filename_suffix}"

    save_figure(fig, cfg, basename=basename)
    plt.close(fig)


def summarize_asymmetry_by_region(
    aquifer_asym: pd.DataFrame,
    cfg: Config,
    label: str = "overall",
) -> None:
    """
    Print and log a simple regional summary of the aquifer-level asymmetry
    index (mean_I_asym) for:
        - US
        - Europe
        - Other world

    Region classification is based on centroid (lon, lat) and REGION_BOUNDS.
    """
    if aquifer_asym is None or aquifer_asym.empty:
        msg = f"[Asymmetry] No aquifer asymmetry data for {label}."
        print(msg)
        append_to_summary_log(cfg, msg)
        return

    df = aquifer_asym.copy()

    # Centroid yoksa ekle (güvenlik için)
    if ("lon" not in df.columns) or ("lat" not in df.columns):
        try:
            df = attach_aquifer_centroids(df, cfg)
        except Exception as e:
            msg = f"[Asymmetry] Could not attach centroids for {label}: {e}"
            print(msg)
            append_to_summary_log(cfg, msg)
            return

    if df.empty:
        msg = f"[Asymmetry] No valid centroids for {label}."
        print(msg)
        append_to_summary_log(cfg, msg)
        return

    def _region_from_xy(lon: float, lat: float) -> str:
        b_us = REGION_BOUNDS["US"]
        b_eu = REGION_BOUNDS["Europe"]

        if (
            (lon >= b_us["lon_min"])
            and (lon <= b_us["lon_max"])
            and (lat >= b_us["lat_min"])
            and (lat <= b_us["lat_max"])
        ):
            return "US"

        if (
            (lon >= b_eu["lon_min"])
            and (lon <= b_eu["lon_max"])
            and (lat >= b_eu["lat_min"])
            and (lat <= b_eu["lat_max"])
        ):
            return "Europe"

        return "Other world"

    df["Region_group"] = [
        _region_from_xy(lon, lat) for lon, lat in zip(df["lon"].values, df["lat"].values)
    ]

    g = df.groupby("Region_group")["mean_I_asym"]
    summary = g.agg(["count", "mean", "median"]).reset_index()

    lines: list[str] = []
    lines.append(f"=== Aquifer asymmetry index by region: {label} ===")
    if not summary.empty:
        lines.append(
            summary.to_string(index=False, float_format=lambda v: f"{v: .3f}")
        )
    else:
        lines.append("No data.")
    lines.append("=" * 60)

    block = "\n".join(lines)

    print("\n" + block + "\n")
    append_to_summary_log(cfg, block)


def _simple_kmeans(
    X: np.ndarray,
    n_clusters: int,
    max_iter: int = 100,
    random_state: int = 42,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Küçük/orta boyutlu veri setleri için basit K-means implementasyonu.

    Girdi:
        X: (n_samples, n_features)
    Çıktı:
        labels: (n_samples,) cluster index (0..k-1)
        centers: (n_clusters, n_features) küme merkezleri
    """
    rng = np.random.default_rng(random_state)
    n_samples, n_features = X.shape
    if n_samples < n_clusters:
        raise ValueError("Number of samples is smaller than n_clusters.")

    # Başlangıç merkezleri: rastgele farklı örnekler
    indices = rng.choice(n_samples, size=n_clusters, replace=False)
    centers = X[indices].copy()
    labels = np.zeros(n_samples, dtype=int)

    for it in range(max_iter):
        # Squared Euclidean distance
        dists = np.sum((X[:, None, :] - centers[None, :, :]) ** 2, axis=2)
        new_labels = np.argmin(dists, axis=1)

        if it > 0 and np.array_equal(new_labels, labels):
            break

        labels = new_labels
        for k in range(n_clusters):
            mask = labels == k
            if np.any(mask):
                centers[k] = X[mask].mean(axis=0)

    return labels, centers


def cluster_station_regimes(
    station_features: pd.DataFrame,
    cfg: Config,
) -> pd.DataFrame:
    """
    İstasyon bazlı rejim özniteliklerini K-means ile cluster'lara ayırır.

    Kullanılan öznitelik vektörü:
        [mean_anom_station, trend_slope, mean_frac_neg, mean_frac_pos]

    Çıktı:
        station_features +:
            - regime_cluster_id  (1..K)
            - regime_cluster_label ("Cluster 1", ...)
    """
    feats = ["mean_anom_station", "trend_slope", "mean_frac_neg", "mean_frac_pos"]
    df = station_features.dropna(subset=feats).copy()
    if df.empty:
        print("   ! No station features available for regime clustering.")
        return station_features

    X = df[feats].values

    # Z-score normalizasyonu
    mean = X.mean(axis=0)
    std = X.std(axis=0, ddof=0)
    std[std == 0] = 1.0
    Xs = (X - mean) / std

    n_clusters = cfg.station_regime_n_clusters
    labels, centers_scaled = _simple_kmeans(
        Xs,
        n_clusters=n_clusters,
        max_iter=cfg.station_regime_max_iter,
        random_state=cfg.station_regime_random_state,
    )
    centers = centers_scaled * std + mean

    df["regime_cluster"] = labels
    df["regime_cluster_id"] = df["regime_cluster"] + 1
    df["regime_cluster_label"] = df["regime_cluster_id"].map(
        lambda i: f"Cluster {i}"
    )

    # Merkezleri CSV olarak kaydet (yorumlamak için faydalı)
    centers_df = pd.DataFrame(
        centers,
        columns=feats,
    )
    centers_df.insert(0, "regime_cluster_id", np.arange(1, n_clusters + 1))
    centers_out = os.path.join(
        cfg.anomalies_dir,
        "station_regime_cluster_centers.csv",
    )
    ensure_dir(cfg.anomalies_dir)
    centers_df.to_csv(centers_out, index=False)
    print(f"   -> Station regime cluster centers written to: {centers_out}")

        # Orijinal tabloya cluster bilgilerini merge et
    # Not: Farklı kaynaklardan gelen StnID tipleri (int/float/str) ve '.0' uzantıları
    # merge sonrası tüm cluster'ların NaN kalmasına yol açabilir. Bu nedenle join anahtarını normalize ediyoruz.
    base = station_features.copy()
    base["_StnID_norm"] = base["StnID"].map(_norm_stnid)

    d2 = df[["StnID", "regime_cluster_id", "regime_cluster_label"]].copy()
    d2["_StnID_norm"] = d2["StnID"].map(_norm_stnid)
    d2 = d2.drop(columns=["StnID"])

    merged = (
        base.merge(
            d2,
            on="_StnID_norm",
            how="left",
        )
        .drop(columns=["_StnID_norm"])
    )

    n_assigned = int(merged["regime_cluster_id"].notna().sum())
    if n_assigned == 0:
        print(
            "   ! WARNING: Station regime clustering merge produced 0 assigned clusters. "
            "This usually indicates an StnID consistency issue."
        )
    # Kısa özet: Region x cluster
    if "Region" in merged.columns:
        summary = (
            merged.dropna(subset=["regime_cluster_id"])
            .groupby(["Region", "regime_cluster_id"])
            .size()
            .unstack(fill_value=0)
        )

        lines: list[str] = []
        lines.append("=== Station regime clusters by Region ===")
        lines.append(summary.to_string())
        lines.append("=========================================")

        block = "\n".join(lines)

        print("\n" + block + "\n")
        append_to_summary_log(cfg, block)

    # Sonucu CSV'ye yaz
    out_path = os.path.join(cfg.anomalies_dir, "station_regime_clusters.csv")
    merged.to_csv(out_path, index=False)
    print(f"   -> Station regime classification written to: {out_path}")

    return merged


def plot_station_regime_maps(
    station_regimes: pd.DataFrame,
    cfg: Config,
    world_gdf: gpd.GeoDataFrame | None = None,
) -> None:
    """
    F5: 'Station-level regime types' – dünya haritasında istasyonlar.

    Girdi:
        station_regimes:
            compute_station_regime_features + cluster_station_regimes çıktısı.
            En az:
                StnID, Lat, Lon, Region, regime_cluster_id, regime_cluster_label

    Çıktı:
        F5_station_regime_types_global.*, F5_station_regime_types_US.*,
        F5_station_regime_types_Europe.*
    """
    if station_regimes.empty:
        print("   ! No station regimes available for F5.")
        return

    df = station_regimes.dropna(subset=["Lat", "Lon", "regime_cluster_id"]).copy()
    if df.empty:
        print("   ! No stations with assigned clusters for F5.")
        return

    # Region sütunu yoksa oluştur (NaN); sonra Lon/Lat'e göre jeo-bölgeyi hesaplayacağız.
    if "Region" not in df.columns:
        df["Region"] = np.nan

    # Region standardizasyonu (veri setleri arası olası isimlendirme farkları için)
    def _std_region_val(x) -> str:
        if pd.isna(x):
            return ""
        s = str(x).strip()
        if not s:
            return ""
        sl = s.lower()
        if sl in {"global", "globe", "world", "all", "other world", "other"}:
            return "global"
        if sl in {"us", "usa", "u.s.", "u.s.a.", "united states", "united states of america"}:
            return "US"
        if sl in {"europe", "eu"}:
            return "Europe"
        return s

    # Jeo-türetilmiş bölge (her istasyon için)
    df["_Region_geo"] = df.apply(lambda r: _region_from_lonlat(r["Lon"], r["Lat"]), axis=1)

    # Standart bölge: US/Europe gibi etiketler varsa onları tercih et; yoksa geo'dan doldur
    df["_Region_std"] = df["Region"].map(_std_region_val)
    mask_valid = df["_Region_std"].isin(["US", "Europe"])
    df.loc[~mask_valid, "_Region_std"] = df.loc[~mask_valid, "_Region_geo"]
    # Sadece 1..K arası cluster'ları al
    df["regime_cluster_id"] = df["regime_cluster_id"].astype(int)

    for region in ["global", "US", "Europe"]:
        if region == "global":
            sub = df.copy()
        else:
            sub = df[df["_Region_std"] == region].copy()

        if sub.empty:
            print(f"   ! No stations for region '{region}' in F5.")
            continue

        bounds = REGION_BOUNDS[region]

        fig, ax = plt.subplots(figsize=(7.0, 3.5))

        # Basemap: Natural Earth admin_0 (vektör)
        if world_gdf is not None and not world_gdf.empty:
            world_gdf.plot(
                ax=ax,
                facecolor="none",
                edgecolor="0.3",
                linewidth=0.7,
                zorder=1,
            )

        # Renk ataması
        cluster_ids = np.sort(sub["regime_cluster_id"].unique())
        colors = []
        for cid in sub["regime_cluster_id"]:
            idx = int(cid) - 1
            if 0 <= idx < len(COLOR_REGIME_CLUSTERS):
                colors.append(COLOR_REGIME_CLUSTERS[idx])
            else:
                colors.append("#999999")

        ax.scatter(
            sub["Lon"].values,
            sub["Lat"].values,
            s=6.0,
            c=colors,
            alpha=0.7,
            linewidths=0.0,
            zorder=2,
        )

        ax.set_xlim(bounds["lon_min"], bounds["lon_max"])
        ax.set_ylim(bounds["lat_min"], bounds["lat_max"])
        ax.set_aspect("equal", adjustable="box")

        ax.set_xlabel("Longitude", fontsize=9)
        ax.set_ylabel("Latitude", fontsize=9)
        ax.set_axisbelow(True)
        ax.grid(True, alpha=0.3, zorder=0)

        title_region = "Global" if region == "global" else region
        ax.set_title(
            f"{title_region} station-level regime types (K-means)",
            fontsize=10,
        )

        # Legenda: sadece mevcut cluster'lar
        from matplotlib.lines import Line2D

        legend_handles = []
        for cid in cluster_ids:
            idx = int(cid) - 1
            if (idx < 0) or (idx >= len(COLOR_REGIME_CLUSTERS)):
                col = "#999999"
            else:
                col = COLOR_REGIME_CLUSTERS[idx]
            label_series = sub.loc[sub["regime_cluster_id"] == cid, "regime_cluster_label"]
            if not label_series.empty:
                lab = label_series.iloc[0]
            else:
                lab = f"Cluster {cid}"

            legend_handles.append(
                Line2D(
                    [0], [0],
                    marker="o",
                    color="none",
                    markerfacecolor=col,
                    markeredgecolor="none",
                    markersize=5,
                    label=lab,
                )
            )

        ax.legend(
            handles=legend_handles,
            fontsize=8,
            frameon=False,
            loc="lower left",
        )

        fig.tight_layout()
        basename = f"F5_station_regime_types_{region}"
        save_figure(fig, cfg, basename=basename)
        plt.close(fig)


def plot_aquifer_decadal_trend_map(
    aquifer_trends: pd.DataFrame,
    cfg: Config,
    region: str = "global",
    title_suffix: str = "",
    filename_suffix: str | None = None,
    world_gdf: gpd.GeoDataFrame | None = None,
    classification: str = "significance",  # "significance" veya "direction"
) -> None:
    """
    F3: 'Global / regional map of aquifer decadal trends'.

    region:
        - "global": tüm akiferler
        - "US": sadece US bbox
        - "Europe": sadece Avrupa bbox

    classification:
        - "significance": renkler MK+p<alpha'ya göre (significant_negative / positive / no trend)
        - "direction": renkler sadece Sen's slope işaretine göre (negative / positive / ~zero)

    title_suffix:
        - Örn. "1900–2020", "1990–2020" vb.

    filename_suffix:
        - Çıktı dosya adında kullanılacak ek (örn. "US_1990_2020_dir").

    Basemap:
        - Ana Jasechko kodunda olduğu gibi, Natural Earth admin_0 countries
          shapefile'ı (vektör) world_gdf ile çizilir.
    """
    mode = classification.lower()
    if mode not in {"significance", "direction"}:
        raise ValueError(f"Unknown classification mode: {classification}")

    if aquifer_trends.empty:
        print("   ! No aquifer trends available for F3 map.")
        return

    if cfg.aquifer_shapefile is None:
        print("   ! Config.aquifer_shapefile is None. Skipping F3 trend map.")
        return

    if region not in REGION_BOUNDS:
        raise ValueError(f"Unknown region key: {region}")

    shp_path = cfg.aquifer_shapefile
    if not os.path.exists(shp_path):
        print(f"   ! Aquifer shapefile not found: {shp_path}. Skipping F3 trend map.")
        return

    try:
        sf = shapefile.Reader(shp_path)
    except Exception as exc:
        print(f"   ! Failed to read aquifer shapefile: {exc}. Skipping F3 trend map.")
        return

    n_shapes = len(sf.shapes())
    if n_shapes == 0:
        print("   ! Aquifer shapefile contains no shapes. Skipping F3 trend map.")
        return

    df = aquifer_trends.copy()
    df = df.dropna(subset=["AQUIFER_ID", "sen_slope"])

    # Region filtresi (varsa)
    if "Region" in df.columns and region != "global":
        df = df[df["Region"] == region].copy()

    if df.empty:
        print(f"   ! No aquifer trends for region '{region}'.")
        return

    df["AQUIFER_ID_int"] = df["AQUIFER_ID"].astype(int)
    df = df[
        (df["AQUIFER_ID_int"] >= 0)
        & (df["AQUIFER_ID_int"] < n_shapes)
    ]
    if df.empty:
        print(
            f"   ! No matching aquifers between trend summary and shapefile indices "
            f"for region '{region}'. Skipping F3 trend map."
        )
        return

    # Eğer trend_sign yoksa, burada da türetebiliriz (güvenlik için)
    if "trend_sign" not in df.columns:
        df["trend_sign"] = np.where(
            df["sen_slope"] < 0,
            "negative",
            np.where(df["sen_slope"] > 0, "positive", "zero"),
        )

    # Sen's slope mutlak değeri → marker boyutu
    abs_slope = df["sen_slope"].abs().values
    if np.isfinite(abs_slope).any() and np.nanmax(abs_slope) > 0:
        ref = np.nanpercentile(abs_slope, 95)
        if (not np.isfinite(ref)) or (ref <= 0):
            ref = np.nanmax(abs_slope)
    else:
        ref = None

    xs, ys, sizes, colors = [], [], [], []

    for row in df.itertuples():
        idx = row.AQUIFER_ID_int
        try:
            shp = sf.shape(idx)
        except IndexError:
            continue

        x_min, y_min, x_max, y_max = shp.bbox
        lon = 0.5 * (x_min + x_max)
        lat = 0.5 * (y_min + y_max)

        xs.append(lon)
        ys.append(lat)

        # --- Renk seçimi: iki moda göre ---
        if mode == "significance":
            if getattr(row, "trend_class", "no_significant_trend") == "significant_negative":
                col = COLOR_TREND_NEG
            elif getattr(row, "trend_class", "no_significant_trend") == "significant_positive":
                col = COLOR_TREND_POS
            else:
                col = COLOR_TREND_NS

        else:  # mode == "direction"
            sign = getattr(row, "trend_sign", None)
            if sign == "negative":
                col = COLOR_TREND_NEG
            elif sign == "positive":
                col = COLOR_TREND_POS
            else:
                col = COLOR_TREND_NS

        colors.append(col)

        if (ref is not None) and np.isfinite(row.sen_slope):
            scale = abs(row.sen_slope) / ref
            scale = min(scale, 1.0)
            s = 10.0 + 90.0 * scale
        else:
            s = 20.0
        sizes.append(s)

    if not xs:
        print(f"   ! No points could be generated for F3 in region '{region}'.")
        return

    bounds = REGION_BOUNDS[region]

    fig, ax = plt.subplots(figsize=(7.0, 3.5))

    # --- Basemap: Natural Earth admin_0, ana kodla aynı mantık ---
    if world_gdf is not None and not world_gdf.empty:
        world_gdf.plot(
            ax=ax,
            facecolor="none",
            edgecolor="0.3",
            linewidth=0.7,
            zorder=1,
        )

    sc = ax.scatter(
        xs,
        ys,
        s=sizes,
        c=colors,
        alpha=0.8,
        linewidths=0.2,
        edgecolors=COLOR_TREND_OUTLINE,
        zorder=2,
    )

    ax.set_xlim(bounds["lon_min"], bounds["lon_max"])
    ax.set_ylim(bounds["lat_min"], bounds["lat_max"])
    ax.set_aspect("equal", adjustable="box")

    ax.set_xlabel("Longitude", fontsize=9)
    ax.set_ylabel("Latitude", fontsize=9)
    ax.set_axisbelow(True)
    ax.grid(True, alpha=0.3, zorder=0)

    title_region = "Global" if region == "global" else region
    base_title = f"{title_region} map of aquifer decadal trends"

    if mode == "direction":
        base_title += " (sign of Sen's slope)"
    if title_suffix:
        base_title += f" – {title_suffix}"

    ax.set_title(base_title, fontsize=10)

    from matplotlib.lines import Line2D
    if mode == "significance":
        legend_handles = [
            Line2D(
                [0], [0],
                marker="o",
                color="none",
                markerfacecolor=COLOR_TREND_NEG,
                markeredgecolor=COLOR_TREND_OUTLINE,
                markersize=6,
                label="Significant negative trend (MK, p<α)",
            ),
            Line2D(
                [0], [0],
                marker="o",
                color="none",
                markerfacecolor=COLOR_TREND_POS,
                markeredgecolor=COLOR_TREND_OUTLINE,
                markersize=6,
                label="Significant positive trend (MK, p<α)",
            ),
            Line2D(
                [0], [0],
                marker="o",
                color="none",
                markerfacecolor=COLOR_TREND_NS,
                markeredgecolor=COLOR_TREND_OUTLINE,
                markersize=6,
                label="No significant trend",
            ),
        ]
    else:  # direction
        legend_handles = [
            Line2D(
                [0], [0],
                marker="o",
                color="none",
                markerfacecolor=COLOR_TREND_NEG,
                markeredgecolor=COLOR_TREND_OUTLINE,
                markersize=6,
                label="Negative slope (Sen < 0)",
            ),
            Line2D(
                [0], [0],
                marker="o",
                color="none",
                markerfacecolor=COLOR_TREND_POS,
                markeredgecolor=COLOR_TREND_OUTLINE,
                markersize=6,
                label="Positive slope (Sen > 0)",
            ),
            Line2D(
                [0], [0],
                marker="o",
                color="none",
                markerfacecolor=COLOR_TREND_NS,
                markeredgecolor=COLOR_TREND_OUTLINE,
                markersize=6,
                label="Near-zero slope",
            ),
        ]

    ax.legend(
        handles=legend_handles,
        fontsize=8,
        frameon=False,
        loc="lower left",
    )

    fig.tight_layout()

    base_suffix = "" if region == "global" else f"_{region}"
    if filename_suffix:
        base_suffix = f"_{filename_suffix}"  # doğrudan verilen suffix'i kullanmak daha net

    basename = f"F3_aquifer_decadal_trend_map{base_suffix}"
    save_figure(fig, cfg, basename=basename)
    plt.close(fig)


def plot_aquifer_trend_distribution(
    aquifer_trends: pd.DataFrame,
    cfg: Config,
) -> None:
    """
    Create F4: 'Distribution of aquifer trends'.

    x-axis: Sen's slope (decadal anomaly trend)
    Representation:
        - Overlapping, normalised histograms (density=True).
        - If a 'Region' column is available, separate curves are drawn for
          US / Europe / Other world.
        - Otherwise a single 'Global' distribution is shown.
    """
    if aquifer_trends.empty:
        print("   ! No aquifer trends available for F4 distribution.")
        return

    df = aquifer_trends.copy()
    df = df[np.isfinite(df["sen_slope"])]

    if df.empty:
        print("   ! No finite Sen slopes for F4 distribution.")
        return

    # Region grouping logic (optional)
    if "Region" in df.columns:
        def _region_group(val: str) -> str:
            v = str(val).lower()
            if v in {"us", "usa", "united states"}:
                return "US"
            if "europe" in v or v == "eu":
                return "Europe"
            return "Other world"

        df["Region_group"] = df["Region"].map(_region_group)
    else:
        df["Region_group"] = "Global"

    groups = df["Region_group"].unique()

    # Colour mapping for regions
    color_region = {
        "Global": "#636363",
        "US": COLOR_REGION_US,
        "Europe": COLOR_REGION_EUROPE,
        "Other world": "#bdbdbd",
    }

    slopes = df["sen_slope"].values
    slope_min = np.nanpercentile(slopes, 2)
    slope_max = np.nanpercentile(slopes, 98)
    if not np.isfinite(slope_min) or not np.isfinite(slope_max) or slope_max <= slope_min:
        slope_min = np.nanmin(slopes)
        slope_max = np.nanmax(slopes)

    bins = np.linspace(slope_min, slope_max, 40)

    fig, ax = plt.subplots(figsize=(6.5, 4.0))

    # Order groups for more consistent legend
    order_priority = {"Global": 0, "US": 1, "Europe": 2, "Other world": 3}
    for region in sorted(groups, key=lambda x: order_priority.get(x, 99)):
        vals = df.loc[df["Region_group"] == region, "sen_slope"].values
        vals = vals[np.isfinite(vals)]
        if vals.size == 0:
            continue

        col = color_region.get(region, "#969696")

        # Filled histogram (soft)
        ax.hist(
            vals,
            bins=bins,
            histtype="stepfilled",
            alpha=0.3,
            color=col,
            density=True,
            label=f"{region} (n={vals.size})",
        )
        # Outline for clarity
        ax.hist(
            vals,
            bins=bins,
            histtype="step",
            linewidth=1.0,
            color=col,
            density=True,
        )

    ax.axvline(0.0, color="#000000", linestyle="--", linewidth=0.8)

    ax.set_xlabel("Sen's slope of decadal anomaly", fontsize=9)
    ax.set_ylabel("Density", fontsize=9)
    ax.set_title("Distribution of aquifer decadal trends", fontsize=10)

    style_ts_axis(ax)
    ax.legend(fontsize=8, frameon=False, loc="upper right")

    fig.tight_layout()
    save_figure(fig, cfg, basename="F4_aquifer_trend_distribution")
    plt.close(fig)


def plot_aquifer_asymmetry_map(
    aquifer_asym: pd.DataFrame,
    cfg: Config,
    region: str = "global",
    world_gdf: gpd.GeoDataFrame | None = None,
    title_suffix: str | None = "1900–2020",
    filename_suffix: str | None = None,
) -> None:
    """
    F6: 'Aquifer-level asymmetry index map'.

    Colour encodes the long-term mean asymmetry index:
        mean_I_asym = mean( I_asym(decade) )

    with:
        I_asym ≈ +1 → downward / negative-dominated regime
        I_asym ≈ -1 → upward / positive-dominated regime

    region:
        - 'global'
        - 'US'
        - 'Europe'
    """
    if aquifer_asym is None or aquifer_asym.empty:
        print("   ! No aquifer asymmetry data for F6.")
        return

    if region not in REGION_BOUNDS:
        raise ValueError(f"Unknown region key for F6: {region}")

    df = aquifer_asym.copy()

    # Centroid yoksa ekle (normalde compute_aquifer_asymmetry eklemiş olacak)
    if ("lon" not in df.columns) or ("lat" not in df.columns):
        try:
            df = attach_aquifer_centroids(df, cfg)
        except Exception as e:
            print(f"   ! Could not attach centroids for F6: {e}")
            return

    if df.empty:
        print("   ! No valid centroids for F6.")
        return

    bounds = REGION_BOUNDS[region]

    # Sadece ilgili bounding box içindeki akiferler
    if region == "global":
        sub = df.copy()
    else:
        m = (
            (df["lon"] >= bounds["lon_min"])
            & (df["lon"] <= bounds["lon_max"])
            & (df["lat"] >= bounds["lat_min"])
            & (df["lat"] <= bounds["lat_max"])
        )
        sub = df[m].copy()

    if sub.empty:
        print(f"   ! No aquifers within bounds for region '{region}' in F6.")
        return

    vals = sub["mean_I_asym"].to_numpy(dtype=float)
    # Teorik aralık [-1, 1]; harita tutarlılığı için sabit skala
    vmin, vmax = -1.0, 1.0

    fig, ax = plt.subplots(figsize=(7.0, 3.5))

    # Vektör basemap (Natural Earth admin_0) – F3 ile aynı stil
    if world_gdf is not None and not world_gdf.empty:
        try:
            world_gdf.plot(
                ax=ax,
                facecolor="none",
                edgecolor="0.3",
                linewidth=0.7,
                zorder=1,
            )
        except Exception as e:
            print(f"[WARN] Could not plot world basemap in F6: {e}")

    sc = ax.scatter(
        sub["lon"].values,
        sub["lat"].values,
        c=vals,
        cmap=plt.get_cmap("RdBu_r"),
        vmin=vmin,
        vmax=vmax,
        s=20.0,
        alpha=0.8,
        linewidths=0.2,
        edgecolors=COLOR_TREND_OUTLINE,
        zorder=2,
    )

    ax.set_xlim(bounds["lon_min"], bounds["lon_max"])
    ax.set_ylim(bounds["lat_min"], bounds["lat_max"])
    ax.set_aspect("equal", adjustable="box")

    ax.set_xlabel("Longitude", fontsize=9)
    ax.set_ylabel("Latitude", fontsize=9)
    ax.set_axisbelow(True)
    ax.grid(True, alpha=0.3, zorder=0)

    title_region = "Global" if region == "global" else region
    title = f"{title_region} aquifer asymmetry index (I_asym)"
    if title_suffix:
        title += f" – {title_suffix}"
    ax.set_title(title, fontsize=10)

    cbar = plt.colorbar(sc, ax=ax, orientation="vertical", pad=0.02)
    cbar.set_label(r"Asymmetry index $I_{\mathrm{asym}}$", fontsize=9)

    fig.tight_layout()

    basename = f"F6_aquifer_asymmetry_map_{region}"
    if filename_suffix:
        basename = f"{basename}_{filename_suffix}"

    save_figure(fig, cfg, basename=basename)
    plt.close(fig)


def plot_global_network_coverage(
    annual_counts: pd.DataFrame,
    decadal_counts: pd.DataFrame,
    cfg: Config,
) -> None:
    """
    Create F1: 'Global groundwater anomaly observation count' figure with
    three panels:

        (a) Annual station count (global)
        (b) Decadal station count (global)
        (c) Decadal aquifer count (global)

    Panels use a consistent, colour, journal-quality styling.
    """
    fig, axes = plt.subplots(
        nrows=3,
        ncols=1,
        figsize=(6.5, 8.0),
        sharex=False,
    )

    # Panel (a): annual station counts (global) – line + darker markers
    ax = axes[0]
    ax.plot(
        annual_counts["Year"],
        annual_counts["n_station"],
        color=COLOR_GLOBAL_STATION,
        linewidth=1.6,
        marker="o",
        markersize=3.5,
        markerfacecolor=COLOR_GLOBAL_STATION_MARKER,
        markeredgecolor=COLOR_GLOBAL_STATION_MARKER,
        linestyle="-",
    )
    style_ts_axis(ax)
    ax.set_ylabel("Stations [count]", fontsize=9)
    ax.set_title("Annual station count (global)", fontsize=10)
    ax.text(
        0.02,
        0.96,
        "(a)",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=9,
        fontweight="bold",
    )

    # Panel (b): decadal station counts (global) – line + darker markers
    ax = axes[1]
    ax.plot(
        decadal_counts["Decade"],
        decadal_counts["n_station"],
        color=COLOR_GLOBAL_DECADAL_STATION,
        linewidth=1.6,
        marker="o",
        markersize=3.5,
        markerfacecolor=COLOR_GLOBAL_DECADAL_STATION_MARKER,
        markeredgecolor=COLOR_GLOBAL_DECADAL_STATION_MARKER,
        linestyle="-",
    )
    style_ts_axis(ax)
    ax.set_ylabel("Stations [count]", fontsize=9)
    ax.set_title("Decadal station count (global)", fontsize=10)
    ax.text(
        0.02,
        0.96,
        "(b)",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=9,
        fontweight="bold",
    )

    # Panel (c): decadal aquifer counts (global) – line + darker markers
    ax = axes[2]
    ax.plot(
        decadal_counts["Decade"],
        decadal_counts["n_aquifer"],
        color=COLOR_GLOBAL_AQUIFER,
        linewidth=1.6,
        marker="s",
        markersize=3.5,
        markerfacecolor=COLOR_GLOBAL_AQUIFER_MARKER,
        markeredgecolor=COLOR_GLOBAL_AQUIFER_MARKER,
        linestyle="-",
    )
    style_ts_axis(ax)
    ax.set_xlabel("Year / Decade", fontsize=9)
    ax.set_ylabel("Aquifers [count]", fontsize=9)
    ax.set_title("Decadal aquifer count (global)", fontsize=10)
    ax.text(
        0.02,
        0.96,
        "(c)",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=9,
        fontweight="bold",
    )

    fig.tight_layout()
    save_figure(fig, cfg, basename="F1_global_gw_observation_count")
    plt.close(fig)


def plot_global_anomaly_range_evolution(
    annual_df: pd.DataFrame,
    cfg: Config,
) -> None:
    """
    Create F2: 'Evolution of global anomaly range' – single-panel figure.

    x-axis: Year
    y-axis: Groundwater anomaly (m yr^-1)

    - Light blue band: full range of anomalies (data_min–data_max)
    - Darker blue band: trimmed range (q_min–q_max, e.g. 5–95th percentile)
    - Optional line: median anomaly per year, if a 'median' column is present
      in annual_max_min_anomalies.csv.

    This figure allows a visual assessment of:
        * whether the overall anomaly range is exploding over time,
        * how much data are excluded by trimming,
        * whether tail behaviour appears to be strengthening.
    """
    required_cols = {"Year", "data_min", "data_max", "q_min", "q_max"}
    missing = required_cols - set(annual_df.columns)
    if missing:
        raise ValueError(
            "annual_df is missing required columns for F2: "
            f"{sorted(missing)}"
        )

    # Focus on the global aggregation
    if "Region" in annual_df.columns:
        df = annual_df[annual_df["Region"].str.lower() == "global"].copy()
        if df.empty:
            raise ValueError(
                "No rows found in annual_max_min_anomalies.csv with Region == 'global'."
            )
    else:
        df = annual_df.copy()

    df = df.sort_values("Year").reset_index(drop=True)

    years = df["Year"].values
    data_min = df["data_min"].values
    data_max = df["data_max"].values
    q_min = df["q_min"].values
    q_max = df["q_max"].values

    fig, ax = plt.subplots(
        nrows=1,
        ncols=1,
        figsize=(6.5, 4.0),
    )

    # 1) Full raw anomaly range: data_min–data_max (light blue band)
    band_full = ax.fill_between(
        years,
        data_min,
        data_max,
        color=COLOR_ANOM_FULL_RANGE,
        alpha=0.4,
        label="Full range (min–max)",
    )

    # 2) Trimmed anomaly range: q_min–q_max (darker blue band)
    band_trim = ax.fill_between(
        years,
        q_min,
        q_max,
        color=COLOR_ANOM_TRIM_RANGE,
        alpha=0.6,
        label="Trimmed range (q05–q95)",
    )

    # 3) Optional median anomaly line, if provided in the CSV
    median_line = None
    if "median" in df.columns:
        median_line, = ax.plot(
            years,
            df["median"].values,
            color=COLOR_ANOM_MEDIAN_LINE,
            linewidth=1.6,
            label="Median anomaly",
        )

    # Journal-style axis formatting
    style_ts_axis(ax)

    ax.set_xlabel("Year", fontsize=9)
    ax.set_ylabel("Groundwater anomaly (m yr$^{-1}$)", fontsize=9)
    ax.set_title("Evolution of global groundwater anomaly range", fontsize=10)

    # Y-eksenini, gözlenen aralığı hafifçe yastıklayarak ayarla
    y_stack = np.vstack([data_min, data_max, q_min, q_max])
    y_min = np.nanmin(y_stack)
    y_max = np.nanmax(y_stack)
    if np.isfinite(y_min) and np.isfinite(y_max):
        pad = 0.05 * (y_max - y_min) if y_max > y_min else max(0.1, 0.05 * abs(y_max))
        ax.set_ylim(y_min - pad, y_max + pad)

    # Açık, dergi-kalitesinde bir lejant
    handles = [band_full, band_trim]
    labels = ["Full range (min–max)", "Trimmed range (q05–q95)"]
    if median_line is not None:
        handles.append(median_line)
        labels.append("Median anomaly")

    ax.legend(
        handles,
        labels,
        fontsize=8,
        frameon=False,
        loc="upper left",
        ncol=1,
    )

    fig.tight_layout()
    save_figure(fig, cfg, basename="F2_global_anomaly_range_evolution")
    plt.close(fig)


def plot_anomaly_range_evolution_variant(
    annual_df: pd.DataFrame,
    cfg: Config,
    *,
    region: str = "global",
    mode: str = "p0_p100",
    out_dir: str | None = None,
) -> None:
    """
    Create a variant of F2 (anomaly-range evolution) for the requested region and mode.

    Parameters
    ----------
    annual_df
        DataFrame produced by load_annual_stats(), expected to contain:
        Year, data_min, data_max, q_min, q_max, (optional) median, and Region.
    region
        - "global" : rows with Region == "global"
        - "US"     : rows with Region == "US"
        - "Europe" : rows with Region == "Europe"
    mode
        - "p0_p100" : plot full range band (data_min–data_max) only
        - "p5_p95"  : plot trimmed range band (q_min–q_max) only
    out_dir
        Optional output folder override (useful to separate p0–p100 vs p5–p95 variants).
    """
    if annual_df is None or annual_df.empty:
        print("   ! annual_df is empty; skipping anomaly-range evolution variant.")
        return

    # --- region selection ---
    df = annual_df.copy()
    if "Region" in df.columns:
        region_key = str(region).strip()
        if region_key.lower() == "global":
            df = df[df["Region"].astype(str).str.lower() == "global"].copy()
        else:
            df = df[df["Region"].astype(str).str.lower() == region_key.lower()].copy()

    if df.empty:
        print(f"   ! No annual records found for region '{region}'. Skipping variant plot.")
        return

    df = df.sort_values("Year").reset_index(drop=True)

    years = df["Year"].values

    # --- mode selection ---
    mode_key = str(mode).strip().lower()
    if mode_key in {"p0_p100", "p0-p100", "full", "full_range"}:
        y_lo = df["data_min"].values
        y_hi = df["data_max"].values
        band_label = "p0–p100 (min–max)"
        title_mode = "p0–p100"
        fname_mode = "p0_p100"
    elif mode_key in {"p5_p95", "p5-p95", "trim", "trimmed", "robust"}:
        y_lo = df["q_min"].values
        y_hi = df["q_max"].values
        band_label = "p5–p95 (trimmed)"
        title_mode = "p5–p95"
        fname_mode = "p5_p95"
    else:
        raise ValueError(f"Unknown mode for anomaly-range evolution: {mode}")

    fig, ax = plt.subplots(1, 1, figsize=(6.5, 3.2), constrained_layout=True)

    # Band (single)
    ax.fill_between(
        years,
        y_lo,
        y_hi,
        alpha=0.35,
        edgecolor="none",
        label=band_label,
    )

    # Optional median line (if present)
    if "median" in df.columns:
        med = df["median"].values
        ax.plot(
            years,
            med,
            linewidth=1.1,
            linestyle="-",
            label="Median anomaly",
        )

    # Zero reference
    ax.axhline(0.0, linewidth=0.8, linestyle="--", alpha=0.6)

    style_ts_axis(ax)

    title_region = "Global" if str(region).strip().lower() == "global" else str(region)
    ax.set_xlabel("Year", fontsize=9)
    ax.set_ylabel("Groundwater anomaly (m yr$^{-1}$)", fontsize=9)
    ax.set_title(
        f"Evolution of groundwater anomaly range ({title_region}; {title_mode})",
        fontsize=10,
    )

    # Padding on y axis
    y_stack = np.vstack([y_lo, y_hi])
    y_min = np.nanmin(y_stack)
    y_max = np.nanmax(y_stack)
    if np.isfinite(y_min) and np.isfinite(y_max):
        pad = 0.05 * (y_max - y_min) if (y_max > y_min) else 0.1
        ax.set_ylim(y_min - pad, y_max + pad)

    ax.legend(fontsize=8, frameon=False, loc="upper left")

    fig.tight_layout()

    region_key_out = "global" if str(region).strip().lower() == "global" else str(region).strip()
    basename = f"F2_anomaly_range_evolution_{region_key_out}_{fname_mode}"
    save_figure(fig, cfg, basename=basename, out_dir=out_dir)
    plt.close(fig)



def plot_regional_annual_station_counts(
    annual_df: pd.DataFrame,
    cfg: Config,
) -> None:
    """
    Create a multi-panel figure of annual station counts for
    three regions: global, US, and Europe.

    Uses annual_max_min_anomalies.csv with a 'Region' column
    (values such as 'global', 'US', 'Europe').

    Each panel is styled for journal-quality presentation with
    region-specific colours.
    """
    if "Region" not in annual_df.columns:
        raise ValueError(
            "annual_df must contain a 'Region' column to plot regional coverage."
        )

    df = annual_df.copy()
    df["Region_lower"] = df["Region"].str.lower()

    # Define regions to plot: key in data, pretty label for title
    regions = [
        ("global", "Global", COLOR_REGION_GLOBAL),
        ("us", "United States", COLOR_REGION_US),
        ("europe", "Europe", COLOR_REGION_EUROPE),
    ]
    panel_labels = ["(a)", "(b)", "(c)"]

    # Ortak Y-limitleri: üç bölgeyi karşılaştırılabilir skala ile göstermek için
    mask = df["Region_lower"].isin([r[0] for r in regions])
    if not df.loc[mask, "n_total"].empty:
        y_min = df.loc[mask, "n_total"].min()
        y_max = df.loc[mask, "n_total"].max()
        if y_max > y_min:
            y_pad = 0.06 * (y_max - y_min)
        else:
            y_pad = max(1.0, 0.06 * y_max)
    else:
        y_min, y_max, y_pad = 0.0, 1.0, 0.1

    fig, axes = plt.subplots(
        nrows=3,
        ncols=1,
        figsize=(6.5, 8.0),
        sharex=False,
    )

    x_min = df["Year"].min()
    x_max = df["Year"].max()

    for ax, (reg_key, reg_title, reg_colour), label in zip(axes, regions, panel_labels):
        sub = df[df["Region_lower"] == reg_key].copy()

        if sub.empty:
            style_ts_axis(ax)
            ax.text(
                0.5,
                0.5,
                f"No data for {reg_title}",
                transform=ax.transAxes,
                ha="center",
                va="center",
                fontsize=8,
            )
            ax.set_axisbelow(True)
            ax.text(
                0.02,
                0.96,
                label,
                transform=ax.transAxes,
                ha="left",
                va="top",
                fontsize=9,
                fontweight="bold",
            )
            continue

        sub = sub.sort_values("Year")

        ax.plot(
            sub["Year"],
            sub["n_total"],
            color=reg_colour,
            linewidth=1.6,
        )
        style_ts_axis(ax)

        ax.set_ylabel("Stations [count]", fontsize=9)
        ax.set_title(f"Annual station count ({reg_title})", fontsize=10)
        ax.set_xlim(x_min, x_max)
        ax.set_ylim(y_min - y_pad, y_max + y_pad)

        ax.text(
            0.02,
            0.96,
            label,
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=9,
            fontweight="bold",
        )

    # X etiketi sadece en alt panelde
    axes[-1].set_xlabel("Year", fontsize=9)

    fig.tight_layout()
    save_figure(fig, cfg, basename="F2_regional_annual_gw_observation_count")
    plt.close(fig)

def plot_trimming_time_series(
    annual_excl: pd.DataFrame,
    decadal_excl: pd.DataFrame,
    cfg: Config,
) -> None:
    """
    F9: 'Number of excluded values due to 5–95% trimming'

    İki panel:
      (a) Yıllık bazda (global), low vs high tail
      (b) Onyıllık bazda (global), low vs high tail

    Zaman serisi stil kuralı:
      - Çizgiler temel renkten biraz daha açık (alpha düşük)
      - Noktalar daha koyu tonda ve belirgin
      - low: mavi, kesikli çizgi, üçgen marker
      - high: kırmızı, düz çizgi, dairesel marker
    """

    if annual_excl.empty and decadal_excl.empty:
        print("   ! F9: No annual/decadal trimming records; skipping F9.")
        return

    # --- Temel temizlik ---
    ann = annual_excl.copy()
    dec = decadal_excl.copy()

    # side kolonunu standartlaştır
    if "side" in ann.columns:
        ann["side"] = ann["side"].astype(str).str.strip().str.lower()
    else:
        raise ValueError("annual_excl must contain 'side' column for F9.")

    if "side" in dec.columns:
        dec["side"] = dec["side"].astype(str).str.strip().str.lower()
    else:
        raise ValueError("decadal_excl must contain 'side' column for F9.")

    # Yıl / onyıl kolon isimleri garanti olsun
    if "Year" not in ann.columns:
        raise ValueError("annual_excl must contain 'Year' column for F9.")
    if "Decade" not in dec.columns:
        raise ValueError("decadal_excl must contain 'Decade' column for F9.")

    # --- Global toplamlar: yıllık ---
    ann_pivot = (
        ann.groupby(["Year", "side"])
        .size()
        .rename("n_excluded")
        .reset_index()
        .pivot(index="Year", columns="side", values="n_excluded")
        .fillna(0.0)
    )

    # --- Global toplamlar: onyıllık ---
    dec_pivot = (
        dec.groupby(["Decade", "side"])
        .size()
        .rename("n_excluded")
        .reset_index()
        .pivot(index="Decade", columns="side", values="n_excluded")
        .fillna(0.0)
    )

    # Eğer bazı tail'ler hiç yoksa, sütunları ekle
    for col in ["low", "high"]:
        if col not in ann_pivot.columns:
            ann_pivot[col] = 0.0
        if col not in dec_pivot.columns:
            dec_pivot[col] = 0.0

    # Y eksenini ortak skala ile ayarlayabilmek için global maksimum
    all_values = np.concatenate(
        [
            ann_pivot[["low", "high"]].to_numpy().ravel(),
            dec_pivot[["low", "high"]].to_numpy().ravel(),
        ]
    )
    y_max = float(np.nanmax(all_values)) if all_values.size > 0 else 1.0
    if y_max <= 0:
        y_max = 1.0
    y_max = y_max * 1.05

    # --- Şekil ve eksenler ---
    fig, axes = plt.subplots(
        nrows=2,
        ncols=1,
        figsize=(6.5, 6.0),
        sharex=False,
    )

    # Renk/stil tanımları (çizgiler hafif, noktalar koyu)
    color_low = "#1f78b4"   # mavi
    color_high = "#e31a1c"  # kırmızı

    def _plot_panel(ax, x, pivot, x_label: str, panel_label: str) -> None:
        # Low tail
        ax.plot(
            x,
            pivot["low"].values,
            color=color_low,
            linewidth=1.4,
            linestyle="--",
            alpha=0.5,        # çizgi biraz daha açık tonda
            zorder=2,
            label="Low tail (≤5th pct)",
        )
        ax.scatter(
            x,
            pivot["low"].values,
            s=18,
            marker="v",
            facecolor=color_low,
            edgecolor="white",
            linewidth=0.5,
            alpha=0.9,        # noktalar daha koyu
            zorder=3,
        )

        # High tail
        ax.plot(
            x,
            pivot["high"].values,
            color=color_high,
            linewidth=1.4,
            linestyle="-",
            alpha=0.5,
            zorder=2,
            label="High tail (≥95th pct)",
        )
        ax.scatter(
            x,
            pivot["high"].values,
            s=20,
            marker="o",
            facecolor=color_high,
            edgecolor="white",
            linewidth=0.5,
            alpha=0.9,
            zorder=3,
        )

        style_ts_axis(ax)
        ax.set_ylim(0.0, y_max)
        ax.set_ylabel("Excluded values [count]", fontsize=9)
        ax.text(
            0.02,
            0.96,
            panel_label,
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=9,
            fontweight="bold",
        )
        ax.set_xlabel(x_label, fontsize=9)

    # Panel (a): Annual
    x_ann = ann_pivot.index.values
    _plot_panel(axes[0], x_ann, ann_pivot, "Year", "(a) Annual (global)")

    # Panel (b): Decadal
    x_dec = dec_pivot.index.values
    _plot_panel(axes[1], x_dec, dec_pivot, "Decade", "(b) Decadal (global)")

    # Ortak legend (üst panelde, kenarda)
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        axes[0].legend(
            handles,
            labels,
            loc="upper right",
            fontsize=8,
            frameon=True,
        )

    fig.tight_layout()
    save_figure(fig, cfg, basename="F9_trimmed_excluded_counts")
    # Also save a copy under the dedicated p5–p95 folder (do not remove original output)
    p5p95_dir, _ = ensure_variant_fig_dirs(cfg)
    save_figure(fig, cfg, basename="F9_trimmed_excluded_counts", out_dir=p5p95_dir)
    plt.close(fig)


def plot_trimmed_location_maps(
    annual_excl: pd.DataFrame,
    decadal_excl: pd.DataFrame,
    cfg: Config,
    world_gdf: gpd.GeoDataFrame | None = None,
) -> None:
    """
    F10: 5–95% trimming ile dışlanan değerlerin mekânsal dağılımı.

    Panel (a): En çok trimming yapılan yıl (annual)
    Panel (b): En çok trimming yapılan onyıl (decadal)
    """
    if annual_excl.empty and decadal_excl.empty:
        print("[F10] No trimming exclusion data; skipping F10.")
        return

    base_low = "#1f78b4"
    base_high = "#e31a1c"

    fig, axes = plt.subplots(
        nrows=2,
        ncols=1,
        figsize=(7.0, 6.5),
        sharex=False,
    )

    # Global sınırlar
    gl = REGION_BOUNDS["global"]
    lon_min, lon_max = gl["lon_min"], gl["lon_max"]
    lat_min, lat_max = gl["lat_min"], gl["lat_max"]

    # --- Panel (a): Annual, en yoğun trimming yılı ---
    ax = axes[0]
    if not annual_excl.empty and {"Lon", "Lat"}.issubset(annual_excl.columns):
        year_counts = annual_excl.groupby("Year").size()
        year_peak = int(year_counts.idxmax())

        sub = annual_excl[annual_excl["Year"] == year_peak].copy()

        if world_gdf is not None:
            try:
                world_gdf.boundary.plot(
                    ax=ax, linewidth=0.4, color="0.7", zorder=0
                )
            except Exception as e:
                print(f"[WARN] Could not plot world basemap in F10 annual panel: {e}")

        for side, color, label in [
            ("low", base_low, "Low tail (≤5th percentile)"),
            ("high", base_high, "High tail (≥95th percentile)"),
        ]:
            ss = sub[sub["side"] == side]
            if ss.empty:
                continue
            ax.scatter(
                ss["Lon"],
                ss["Lat"],
                s=8,
                marker="o",
                linewidth=0.0,
                alpha=0.8,
                color=color,
                label=label,
                zorder=2,
            )

        ax.set_xlim(lon_min, lon_max)
        ax.set_ylim(lat_min, lat_max)
        ax.set_ylabel("Latitude (°)")
        ax.set_title(
            f"(a) Trimmed anomalies (global 5–95% threshold), year {year_peak}",
            loc="left",
        )
        ax.grid(True, linestyle=":", linewidth=0.4, alpha=0.4)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)
        ax.legend(fontsize=7, ncol=2, frameon=False, loc="lower left")
    else:
        ax.set_axis_off()
        ax.text(
            0.5,
            0.5,
            "No annual trimming locations available",
            transform=ax.transAxes,
            ha="center",
            va="center",
        )

    # --- Panel (b): Decadal, en yoğun trimming onyılı ---
    ax2 = axes[1]
    if not decadal_excl.empty and {"Lon", "Lat"}.issubset(decadal_excl.columns):
        dec_counts = decadal_excl.groupby("Decade").size()
        dec_peak = int(dec_counts.idxmax())

        subd = decadal_excl[decadal_excl["Decade"] == dec_peak].copy()

        if world_gdf is not None:
            try:
                world_gdf.boundary.plot(
                    ax=ax2, linewidth=0.4, color="0.7", zorder=0
                )
            except Exception as e:
                print(f"[WARN] Could not plot world basemap in F10 decadal panel: {e}")

        for side, color, label in [
            ("low", base_low, "Low tail (≤5th percentile)"),
            ("high", base_high, "High tail (≥95th percentile)"),
        ]:
            ss = subd[subd["side"] == side]
            if ss.empty:
                continue
            ax2.scatter(
                ss["Lon"],
                ss["Lat"],
                s=8,
                marker="o",
                linewidth=0.0,
                alpha=0.8,
                color=color,
                label=label,
                zorder=2,
            )

        ax2.set_xlim(lon_min, lon_max)
        ax2.set_ylim(lat_min, lat_max)
        ax2.set_xlabel("Longitude (°)")
        ax2.set_ylabel("Latitude (°)")
        ax2.set_title(
            f"(b) Trimmed decadal means (global 5–95% threshold), decade {dec_peak}",
            loc="left",
        )
        ax2.grid(True, linestyle=":", linewidth=0.4, alpha=0.4)
        for spine in ["top", "right"]:
            ax2.spines[spine].set_visible(False)
        ax2.legend(fontsize=7, ncol=2, frameon=False, loc="lower left")
    else:
        ax2.set_axis_off()
        ax2.text(
            0.5,
            0.5,
            "No decadal trimming locations available",
            transform=ax2.transAxes,
            ha="center",
            va="center",
        )

    fig.tight_layout()
    save_figure(fig, cfg, "F10_trimmed_locations")
    # Also save a copy under the dedicated p5–p95 folder (do not remove original output)
    p5p95_dir, _ = ensure_variant_fig_dirs(cfg)
    save_figure(fig, cfg, "F10_trimmed_locations", out_dir=p5p95_dir)
    plt.close(fig)


def plot_trimmed_location_maps_region(
    annual_excl: pd.DataFrame,
    decadal_excl: pd.DataFrame,
    cfg: Config,
    world_gdf: gpd.GeoDataFrame | None,
    region: str,
) -> None:
    """
    Ek F10 haritaları: 5–95% trimming ile dışlanan noktaların
    sadece tek bir bölge (global/US/Europe) için mekânsal dağılımı.
    Global F10 fonksiyonuna dokunmadan US ve Europe için ek harita üretir.
    """
    region_key = region.lower()
    if region_key not in ("global", "us", "europe"):
        print(f"   ! Unknown region '{region}' for F10 regional maps; skipping.")
        return

    ann_reg = _subset_points_by_region_bounds(annual_excl, region)
    dec_reg = _subset_points_by_region_bounds(decadal_excl, region)

    if (ann_reg is None or ann_reg.empty) and (dec_reg is None or dec_reg.empty):
        print(f"   ! No trimmed values for region {region}; skipping F10 regional map.")
        return

    region_label = "Global" if region_key == "global" else region

    # Harita projeksiyonu / eksenler
    if HAS_CARTOPY and world_gdf is not None:
        proj = ccrs.PlateCarree()
        fig, axes = plt.subplots(
            1,
            2,
            figsize=(7.5, 3.8),
            subplot_kw={"projection": proj},
            constrained_layout=True,
        )
    else:
        fig, axes = plt.subplots(
            1,
            2,
            figsize=(7.5, 3.8),
            constrained_layout=True,
        )

    bounds = REGION_BOUNDS.get(region, REGION_BOUNDS["global"])
    xlim = (bounds["lon_min"], bounds["lon_max"])
    ylim = (bounds["lat_min"], bounds["lat_max"])

    panels = [
        (ann_reg, "Annual anomalies (trimmed)"),
        (dec_reg, "Decadal mean anomalies (trimmed)"),
    ]

    for ax, (df_panel, title_suffix) in zip(axes, panels):
        if HAS_CARTOPY and world_gdf is not None:
            ax.set_extent([xlim[0], xlim[1], ylim[0], ylim[1]], crs=proj)
            world_gdf.boundary.plot(
                ax=ax, transform=proj, linewidth=0.3, color="0.6"
            )
        else:
            ax.set_xlim(*xlim)
            ax.set_ylim(*ylim)
            if world_gdf is not None:
                world_gdf.boundary.plot(ax=ax, linewidth=0.3, color="0.6")

        if df_panel is not None and not df_panel.empty:
            # Lon/Lat kolonlarını belirle
            lon_col = "Lon" if "Lon" in df_panel.columns else "lon"
            lat_col = "Lat" if "Lat" in df_panel.columns else "lat"

            for side, marker, label_side in [
                ("low", "v", "below p5"),
                ("high", "^", "above p95"),
            ]:
                sub = df_panel[df_panel["side"] == side]
                if sub.empty:
                    continue

                c = "tab:blue" if side == "low" else "tab:red"
                kwargs = dict(
                    s=8,
                    marker=marker,
                    c=c,
                    alpha=0.4,
                    linewidths=0.0,
                )
                if HAS_CARTOPY and world_gdf is not None:
                    ax.scatter(
                        sub[lon_col].values,
                        sub[lat_col].values,
                        transform=proj,
                        **kwargs,
                    )
                else:
                    ax.scatter(
                        sub[lon_col].values,
                        sub[lat_col].values,
                        **kwargs,
                    )

            # Lejand
            handles, labels = ax.get_legend_handles_labels()
            if not handles:
                # Legend'i manüel ekle
                from matplotlib.lines import Line2D

                h_low = Line2D(
                    [], [], marker="v", linestyle="None", color="tab:blue", label="below p5"
                )
                h_high = Line2D(
                    [], [], marker="^", linestyle="None", color="tab:red", label="above p95"
                )
                ax.legend(
                    handles=[h_low, h_high],
                    loc="lower left",
                    fontsize=7,
                    frameon=True,
                )

        ax.set_title(f"{region_label} – {title_suffix}", fontsize=9)
        if not (HAS_CARTOPY and world_gdf is not None):
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")

    fig.suptitle(
        f"Locations of values excluded by 5–95% trimming ({region_label})",
        fontsize=11,
    )
    basename = f"F10_trimmed_locations_{region_key}"
    save_figure(fig, cfg, basename)
    # Also save a copy under the dedicated p5–p95 folder (do not remove original output)
    p5p95_dir, _ = ensure_variant_fig_dirs(cfg)
    save_figure(fig, cfg, basename, out_dir=p5p95_dir)
    plt.close(fig)


def plot_repeated_outlier_station_map(
    station_summary: pd.DataFrame,
    cfg: Config,
    world_gdf: gpd.GeoDataFrame | None = None,
    min_repeats: int = 3,
) -> None:
    """
    F11: 'Stations with repeated global outliers'

    Girdi:
        station_summary: summarize_repeated_outlier_stations çıktısı
            (StnID, n_outliers, Lon, Lat, Region)
    """

    if station_summary.empty:
        print("   ! No station-level repeated-outlier summary; skipping F11.")
        return

    df = station_summary.copy()
    df = df[df["n_outliers"] >= min_repeats].copy()
    if df.empty:
        print(f"   ! No stations with ≥{min_repeats} outliers; skipping F11.")
        return

    # Marker boyutu outlier sayısına göre
    df["marker_size"] = np.clip((df["n_outliers"] - min_repeats + 1) * 14.0, 18.0, 120.0)

    # Region renkleri (US / Europe / global)
    region_colors = {
        "US": "#1b9e77",
        "Europe": "#d95f02",
        "global": "#7570b3",
        "Other world": "#7570b3",
    }
    df["Region_plot"] = df["Region"].where(df["Region"].isin(region_colors), "global")

    fig, ax = plt.subplots(figsize=(7.0, 3.8))

    # Basemap: Natural Earth world_gdf (diğer haritalarla uyumlu)
    if world_gdf is not None and not world_gdf.empty:
        world_gdf.plot(
            ax=ax,
            color="#f0f0f0",
            edgecolor="#b0b0b0",
            linewidth=0.5,
            zorder=0,
        )
    else:
        ax.set_facecolor("white")

    # İstasyonlar
    for reg, sub in df.groupby("Region_plot"):
        color = region_colors.get(reg, "#7570b3")
        ax.scatter(
            sub["Lon"],
            sub["Lat"],
            s=sub["marker_size"],
            facecolor=color,
            edgecolor="white",
            linewidth=0.5,
            alpha=0.9,
            zorder=3,
            label=f"{reg} (n={len(sub)})",
        )

    ax.set_xlabel("Longitude", fontsize=9)
    ax.set_ylabel("Latitude", fontsize=9)
    ax.set_title(
        f"Stations with repeated global outliers (≥{min_repeats} events)",
        fontsize=10,
    )
    ax.grid(True, linestyle=":", linewidth=0.4, alpha=0.5)
    ax.set_axisbelow(True)

    # Legend
    ax.legend(
        loc="upper right",
        fontsize=8,
        frameon=True,
        title="Region",
        title_fontsize=8,
    )

    fig.tight_layout()
    save_figure(fig, cfg, basename="F11_repeated_outlier_stations")
    plt.close(fig)


def plot_repeated_outlier_station_map_region(
    station_summary: pd.DataFrame,
    cfg: Config,
    world_gdf: gpd.GeoDataFrame | None,
    region: str = "US",
    min_repeats: int = 3,
) -> None:
    """
    F11: Sadece seçilen bölge (US/Europe) için tekrarlayan outlier istasyon haritası.
    """
    region_key = region.lower()
    if region_key not in ("global", "us", "europe"):
        print(f"   ! Unknown region '{region}' for F11 regional map; skipping.")
        return

    if station_summary is None or station_summary.empty:
        print(f"   ! Empty station summary; skipping F11 regional map for {region}.")
        return

    df = station_summary.copy()

    # ---- KOLON ADINI GÜVENLİ ŞEKİLDE EŞLE (n_outlier vs n_outliers) ----
    if "n_outliers" in df.columns:
        pass  # zaten doğru isim
    elif "n_outlier" in df.columns:
        df = df.rename(columns={"n_outlier": "n_outliers"})
    else:
        print("   ! Station summary missing 'n_outliers' column; skipping regional F11.")
        return

    # Minimum tekrar sayısına göre süz
    df = df[df["n_outliers"] >= min_repeats].copy()
    if df.empty:
        print(f"   ! No stations with ≥{min_repeats} outliers in region {region}; skipping F11 regional map.")
        return

    # Bölge sınırlarına göre alt küme
    df = _subset_points_by_region_bounds(df, region)
    if df is None or df.empty:
        print(f"   ! No stations within region bounds for {region}; skipping F11 regional map.")
        return

    # Lon/Lat kolonları
    lon_col = "Lon" if "Lon" in df.columns else "lon"
    lat_col = "Lat" if "Lat" in df.columns else "lat"

    region_label = "Global" if region_key == "global" else region

    # Harita projeksiyonu
    if HAS_CARTOPY and world_gdf is not None:
        proj = ccrs.PlateCarree()
        fig, ax = plt.subplots(
            1,
            1,
            figsize=(6.5, 3.8),
            subplot_kw={"projection": proj},
            constrained_layout=True,
        )
        bounds = REGION_BOUNDS.get(region, REGION_BOUNDS["global"])
        ax.set_extent(
            [bounds["lon_min"], bounds["lon_max"], bounds["lat_min"], bounds["lat_max"]],
            crs=proj,
        )
        world_gdf.boundary.plot(ax=ax, transform=proj, linewidth=0.3, color="0.6")
    else:
        fig, ax = plt.subplots(1, 1, figsize=(6.5, 3.8), constrained_layout=True)
        bounds = REGION_BOUNDS.get(region, REGION_BOUNDS["global"])
        ax.set_xlim(bounds["lon_min"], bounds["lon_max"])
        ax.set_ylim(bounds["lat_min"], bounds["lat_max"])
        if world_gdf is not None:
            world_gdf.boundary.plot(ax=ax, linewidth=0.3, color="0.6")

    # ---- Kategoriler: 3–4, 5–9, ≥10 outlier aynı global F11 gibi ----
    categories = [
        (3, 4, "3–4 outliers", "tab:orange"),
        (5, 9, "5–9 outliers", "tab:red"),
        (10, np.inf, "≥10 outliers", "purple"),
    ]

    legend_handles: list[Line2D] = []

    for lo, hi, label_side, color in categories:
        mask = (df["n_outliers"] >= lo) & (df["n_outliers"] <= hi)
        sub = df.loc[mask]
        if sub.empty:
            continue

        kwargs = dict(
            s=15,
            marker="o",
            c=color,
            alpha=0.7,
            linewidths=0.2,
            edgecolors="black",
        )

        if HAS_CARTOPY and world_gdf is not None:
            ax.scatter(
                sub[lon_col].values,
                sub[lat_col].values,
                transform=proj,
                **kwargs,
            )
        else:
            ax.scatter(
                sub[lon_col].values,
                sub[lat_col].values,
                **kwargs,
            )

        n = len(sub)
        legend_handles.append(
            Line2D(
                [0],
                [0],
                marker="o",
                linestyle="",
                markersize=4,
                markerfacecolor=color,
                markeredgecolor="black",
                label=f"{label_side} (n={n})",
            )
        )

    ax.set_title(f"Stations with repeated global outliers ({region_label})", fontsize=10)
    if not (HAS_CARTOPY and world_gdf is not None):
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

    if legend_handles:
        ax.legend(
            handles=legend_handles,
            loc="lower left",
            fontsize=7,
            frameon=True,
        )

    basename = f"F11_repeated_outlier_stations_{region_key}"
    save_figure(fig, cfg, basename)
    plt.close(fig)


def _subset_points_by_region_bounds(
    df: pd.DataFrame,
    region: str,
    lon_col: str = "Lon",
    lat_col: str = "Lat",
) -> pd.DataFrame:
    """
    Region = 'global', 'US' veya 'Europe' için Lon/Lat bazında alt küme seçer.
    Eğer region='global' ise tüm veriyi döndürür.
    Lon/Lat kolonları bulunamazsa boş DataFrame döner.
    """
    if df is None or df.empty:
        return df

    if region.lower() == "global":
        return df.copy()

    # Lon / Lat kolon isimlerini tespit et
    if lon_col not in df.columns or lat_col not in df.columns:
        alt_lon, alt_lat = "lon", "lat"
        if alt_lon in df.columns and alt_lat in df.columns:
            lon_col, lat_col = alt_lon, alt_lat
        else:
            # Koordinat yoksa bölge filtresi yapamayız
            return df.iloc[0:0].copy()

    bounds = REGION_BOUNDS.get(region, REGION_BOUNDS["global"])
    mask = (
        (df[lon_col] >= bounds["lon_min"])
        & (df[lon_col] <= bounds["lon_max"])
        & (df[lat_col] >= bounds["lat_min"])
        & (df[lat_col] <= bounds["lat_max"])
    )
    return df.loc[mask].copy()


def run_trimming_diagnostics(cfg: Config, world_gdf: gpd.GeoDataFrame | None) -> None:
    """
    F9–F11: trimming ve global outlier diagnostikleri.
    - F9: yıllık/onyıllık trimmed sayıları (zaman serisi)
    - F10: trimmed değerlerin konumu (global + US + Europe)
    - F11: tekrarlayan outlier istasyonları (global + US + Europe)
    """
    print("[Extra] Trimming and outlier diagnostics (F9–F11)...")

    # 1) Annual ve decadal trimmed veri
    annual_excl = load_annual_trimmed_anomalies(cfg)
    decadal_excl = load_decadal_trimmed_anomalies(cfg)

    summarize_trimming_by_region(annual_excl, decadal_excl, cfg)
    plot_trimming_time_series(annual_excl, decadal_excl, cfg)

    # 2) Global outlier'lar ve istasyon bazlı özet
    outlier_df = load_global_outlier_anomalies(cfg)
    station_summary = summarize_repeated_outlier_stations(outlier_df, cfg)

    # 3) F10 – global trimmed location haritası (mevcut fonksiyon)
    plot_trimmed_location_maps(annual_excl, decadal_excl, cfg, world_gdf)

    # 4) F10 – US ve Europe için ek trimmed location haritaları
    for reg in ["US", "Europe"]:
        print(f"   -> F10 trimmed locations map for {reg}")
        plot_trimmed_location_maps_region(
            annual_excl,
            decadal_excl,
            cfg,
            world_gdf,
            region=reg,
        )

    # 5) F11 – global repeated-outlier haritası (mevcut fonksiyon)
    plot_repeated_outlier_station_map(station_summary, cfg, world_gdf)

    # 6) F11 – US ve Europe için ek repeated-outlier haritaları
    for reg in ["US", "Europe"]:
        print(f"   -> F11 repeated-outlier station map for {reg}")
        plot_repeated_outlier_station_map_region(
            station_summary,
            cfg,
            world_gdf,
            region=reg,
        )


def run_repeated_outlier_diagnostics(cfg: Config, world_gdf: gpd.GeoDataFrame | None = None) -> None:
    """
    5.2: Tekrarlayan uç değer istasyonları (F11).

    Adımlar:
      - global_outlier_anomalies.csv dosyasını yükle
      - istasyon bazında outlier sayısını hesapla
      - 3+, 5+, 10+ için özet üret (log + CSV)
      - ≥3 outlier içeren istasyonları F11 haritasında göster
    """
    outlier_df = load_global_outlier_anomalies(cfg)
    if outlier_df.empty:
        print("[F11] No global outliers found; skipping repeated-outlier diagnostics.")
        return

    station_summary = summarize_repeated_outlier_stations(outlier_df, cfg)
    if station_summary.empty:
        print("[F11] Station summary is empty; skipping F11 map.")
        return

    print("   -> F11 map: stations with repeated global outliers (≥3 outliers).")
    plot_repeated_outlier_station_map(
        station_summary,
        cfg,
        world_gdf=world_gdf,
        min_repeats=3,
    )


def _norm_stnid(x) -> str:
    """
    Normalize station IDs (StnID):
      - Trim whitespace
      - Remove BOM / zero-width chars
      - '1030007597.0' -> '1030007597'
    """
    if pd.isna(x):
        return ""
    s = str(x).strip().replace("\u200b", "")
    s2 = s.replace(",", ".")
    try:
        f = float(s2)
        if f.is_integer():
            return str(int(f))
        return s
    except Exception:
        return s


# Globalde zaten var: MULTI_TREND_PERIODS = [
#    ("overall", None, None),
#    ("1900_1959", 1900, 1959),
#    ("1960_1989", 1960, 1989),
#    ("1990_2020", 1990, 2020),
# ]
# bunu doğrudan kullanacağız.

def summarize_station_drought_metrics(
    drought_df: pd.DataFrame,
    station_features: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """
    SGI-tabanlı yıllık olay metriklerinden:
      - overall (tüm yıllar)
      - 1900–1959
      - 1960–1989
      - 1990–2020
    için istasyon-bazlı özet DataFrame'ler döndürür.

    Dönüş:
        {period_label: df}, df kolonları:
          ['StnID', 'mean_MaxDroughtDuration', 'mean_CumulativeDeficit',
           'mean_NumEvents', 'n_years', 'Lon', 'Lat', 'Region']
    """
    results: dict[str, pd.DataFrame] = {}

    if drought_df is None or drought_df.empty:
        return results

    if "Year" not in drought_df.columns:
        print("   ! 'Year' column missing in drought_df; cannot summarize station drought metrics.")
        return results

    # İstasyon metadatasından Lon/Lat/Region çek
    meta_cols = [c for c in ["StnID", "Lon", "Lat", "Region"] if c in station_features.columns]
    station_meta = station_features[meta_cols].drop_duplicates()

    # Dönem listesi: overall + MULTI_TREND_PERIODS
    periods: list[tuple[str, int | None, int | None]] = [("overall", None, None)]
    periods.extend(MULTI_TREND_PERIODS)

    for label, y_start, y_end in periods:
        df = drought_df.copy()

        if (y_start is not None) and (y_end is not None):
            df = df[(df["Year"] >= y_start) & (df["Year"] <= y_end)]

        if df.empty:
            continue

        agg = (
            df.groupby("StnID", dropna=True)
            .agg(
                mean_MaxDroughtDuration=("MaxDroughtDuration", "mean"),
                mean_CumulativeDeficit=("CumulativeDeficit", "mean"),
                mean_NumEvents=("NumEvents", "mean"),
                n_years=("Year", "nunique"),
            )
            .reset_index()
        )

        merged = agg.merge(station_meta, on="StnID", how="left")
        results[label] = merged

    return results


def plot_station_drought_metric_maps(
    station_summaries: dict[str, pd.DataFrame],
    cfg: Config,
    world_gdf: gpd.GeoDataFrame | None,
    metric: str = "mean_MaxDroughtDuration",
    min_years: int = 10,
) -> None:
    """
    F14: İstasyon-bazlı olay metrikleri için Global/US/Europe haritaları.

    station_summaries:
        summarize_station_drought_metrics() çıktısı:
        {period_label: df}, df kolonları:
          ['StnID', metric, 'n_years', 'Lon', 'Lat', 'Region']

    metric:
        'mean_MaxDroughtDuration' veya 'mean_CumulativeDeficit' vb.
    """
    if not station_summaries:
        print("[F14] No station drought summaries; skipping F14.")
        return

    # Dönem listesi: overall + MULTI_TREND_PERIODS
    periods: list[tuple[str, int | None, int | None]] = [("overall", None, None)]
    periods.extend(MULTI_TREND_PERIODS)

    for label, d_start, d_end in periods:
        df = station_summaries.get(label)
        if df is None or df.empty:
            continue

        if metric not in df.columns:
            print(f"[F14] Metric '{metric}' not in station summary for {label}; skipping.")
            continue

        # Yıl sayısı filtresi
        df = df[df["n_years"] >= min_years].copy()
        if df.empty:
            print(f"[F14] No stations with ≥{min_years} years for period {label}; skipping.")
            continue

        # Renk skalası için global p2–p98
        vals = df[metric].to_numpy(dtype=float)
        vals = vals[np.isfinite(vals)]
        if vals.size == 0:
            continue
        p2, p98 = np.percentile(vals, [2, 98])
        data_min, data_max = float(np.nanmin(vals)), float(np.nanmax(vals))
        norm, cmap, vmin, vmax = _safe_diverging_norm_and_cmap(
            data_min, data_max, p2, p98
        )

        # Global, US, Europe panelleri
        regions = ["global", "US", "Europe"]
        n_pan = len(regions)

        if HAS_CARTOPY and world_gdf is not None:
            proj = ccrs.PlateCarree()
            fig, axes = plt.subplots(
                1,
                n_pan,
                figsize=(3.8 * n_pan, 3.6),
                subplot_kw={"projection": proj},
                constrained_layout=True,
            )
        else:
            fig, axes = plt.subplots(
                1,
                n_pan,
                figsize=(3.8 * n_pan, 3.6),
                constrained_layout=True,
            )

        if n_pan == 1:
            axes = [axes]

        for ax, reg in zip(axes, regions):
            if reg == "global":
                reg_df = df.copy()
            else:
                reg_df = df[df["Region"] == reg].copy()

            if reg_df.empty:
                ax.set_axis_off()
                continue

            if HAS_CARTOPY and world_gdf is not None:
                ax.set_global()
                world_gdf.boundary.plot(ax=ax, linewidth=0.3, color="0.4")
            else:
                ax.set_aspect("equal", adjustable="box")

            # Bölgesel sınır kutuları
            if reg in REGION_BOUNDS:
                lon_min, lon_max, lat_min, lat_max = REGION_BOUNDS[reg]
                ax.set_xlim(lon_min, lon_max)
                ax.set_ylim(lat_min, lat_max)

            sc = ax.scatter(
                reg_df["Lon"].values,
                reg_df["Lat"].values,
                c=reg_df[metric].values,
                s=10,
                cmap=cmap,
                norm=norm,
                vmin=vmin,
                vmax=vmax,
                edgecolor="k",
                linewidth=0.2,
                transform=ccrs.PlateCarree() if HAS_CARTOPY and world_gdf is not None else None,
            )

            ax.set_title(reg, fontsize=8)

            if not HAS_CARTOPY or world_gdf is None:
                ax.set_xlabel("Lon", fontsize=7)
                ax.set_ylabel("Lat", fontsize=7)
                ax.tick_params(labelsize=7)

        # Ortak renk çubuğu
        cbar = fig.colorbar(sc, ax=axes, orientation="horizontal", fraction=0.06, pad=0.08)
        cbar.ax.set_xlabel(metric, fontsize=8)
        cbar.ax.tick_params(labelsize=7)

        if label == "overall":
            period_label = "overall"
            basename = f"F14_station_drought_{metric}_overall"
        else:
            period_label = label
            basename = f"F14_station_drought_{metric}_{label}"

        fig.suptitle(
            f"Station-level drought metric ({metric}, period: {period_label})",
            fontsize=9,
        )

        save_figure(fig, cfg, basename=basename)
        plt.close(fig)


def compute_aquifer_drought_metrics_from_station_events(
    drought_df: pd.DataFrame,
    decadal_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    İstasyon-yıl bazlı olay metriklerini AQUIFER_ID × Decade düzeyine agg eder.

    Girdiler:
        drought_df:
            ['StnID', 'Year', 'MaxDroughtDuration', 'CumulativeDeficit', 'NumEvents', ...]
        decadal_df:
            station_decadal_stats_with_aquifer.csv tablosu
            en azından ['StnID', 'AQUIFER_ID'] içermeli.

    Çıktı:
        ['AQUIFER_ID', 'Decade',
         'mean_MaxDroughtDuration', 'mean_CumulativeDeficit',
         'mean_NumEvents', 'n_station_years']
    """
    if drought_df is None or drought_df.empty:
        return pd.DataFrame()

    if "AQUIFER_ID" not in decadal_df.columns:
        raise ValueError("decadal_df must contain 'AQUIFER_ID' column.")

    # 1) StnID -> AQUIFER_ID eşlemesi (mod/first)
    map_df = (
        decadal_df[["StnID", "AQUIFER_ID"]]
        .dropna(subset=["AQUIFER_ID"])
        .drop_duplicates()
    )

    # Bazı istasyonlar birden fazla aquifer'e düşüyorsa, en sık görüleni seç
    map_df = (
        decadal_df.dropna(subset=["AQUIFER_ID"])
        .groupby("StnID")["AQUIFER_ID"]
        .agg(lambda s: s.value_counts().index[0])
        .reset_index()
    )

    # 2) Year -> Decade
    df = drought_df.copy()
    df["Decade"] = (df["Year"] // 10) * 10

    # 3) Aquifer ID ekle
    df = df.merge(map_df, on="StnID", how="left")
    df = df.dropna(subset=["AQUIFER_ID"])

    if df.empty:
        return pd.DataFrame()

    agg = (
        df.groupby(["AQUIFER_ID", "Decade"], dropna=True)
        .agg(
            mean_MaxDroughtDuration=("MaxDroughtDuration", "mean"),
            mean_CumulativeDeficit=("CumulativeDeficit", "mean"),
            mean_NumEvents=("NumEvents", "mean"),
            n_station_years=("MaxDroughtDuration", "count"),
        )
        .reset_index()
    )

    return agg


def plot_aquifer_drought_heatmap(
    aq_df: pd.DataFrame,
    cfg: Config,
    metric_col: str = "mean_MaxDroughtDuration",
    label: str = "overall",
) -> None:
    """
    F15: Aquifer × Decade heatmap (örneğin ortalama MaxDroughtDuration).

    aq_df:
        compute_aquifer_drought_metrics_from_station_events çıktısı +
        centroid’ler eklenmiş (lon, lat).
    """
    if aq_df is None or aq_df.empty:
        print(f"[F15] Empty aquifer drought metrics for {label}; skipping.")
        return

    df = aq_df.copy()
    if metric_col not in df.columns:
        print(f"[F15] Metric '{metric_col}' not in aquifer drought metrics; skipping.")
        return

    # Centroid yoksa ekle
    if ("lon" not in df.columns) or ("lat" not in df.columns):
        try:
            df = attach_aquifer_centroids(df, cfg)
        except Exception as e:
            print(f"[F15] Could not attach centroids for {label}: {e}")
            return

    if df.empty:
        print(f"[F15] No valid aquifer centroids for {label}; skipping.")
        return

    # Bölge grupları (US / Europe / Other world) – F12’dekiyle aynı mantık
    def _region_from_xy(lon: float, lat: float) -> str:
        b_us = REGION_BOUNDS["US"]
        b_eu = REGION_BOUNDS["Europe"]
        if (
            (lon >= b_us["lon_min"])
            and (lon <= b_us["lon_max"])
            and (lat >= b_us["lat_min"])
            and (lat <= b_us["lat_max"])
        ):
            return "US"
        if (
            (lon >= b_eu["lon_min"])
            and (lon <= b_eu["lon_max"])
            and (lat >= b_eu["lat_min"])
            and (lat <= b_eu["lat_max"])
        ):
            return "Europe"
        return "Other world"

    df["Region_group"] = [
        _region_from_xy(lon, lat) for lon, lat in zip(df["lon"].values, df["lat"].values)
    ]

    # Değer aralığı
    vals = df[metric_col].to_numpy(dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        print(f"[F15] No finite values for {metric_col}; skipping.")
        return
    p2, p98 = np.percentile(vals, [2, 98])
    data_min, data_max = float(np.nanmin(vals)), float(np.nanmax(vals))
    norm, cmap, vmin, vmax = _safe_diverging_norm_and_cmap(data_min, data_max, p2, p98)

    regions = ["US", "Europe", "Other world"]
    fig, axes = plt.subplots(
        len(regions),
        1,
        figsize=(7.2, 2.2 * len(regions)),
        constrained_layout=True,
    )
    if len(regions) == 1:
        axes = [axes]

    # On yıllar
    decades = sorted(df["Decade"].dropna().unique())
    decade_labels = [str(int(d)) for d in decades]

    for ax, reg in zip(axes, regions):
        sub = df[df["Region_group"] == reg].copy()
        if sub.empty:
            ax.axis("off")
            continue

        # Aquifer’leri enlemine göre sıralayalım
        sub = sub.sort_values(["lat", "AQUIFER_ID"])
        aq_ids = sub["AQUIFER_ID"].drop_duplicates().tolist()

        # Pivot: satır = aquifer, sütun = decade
        mat = (
            sub.pivot_table(
                index="AQUIFER_ID",
                columns="Decade",
                values=metric_col,
                aggfunc="mean",
            )
            .reindex(index=aq_ids, columns=decades)
        )

        im = ax.imshow(
            mat.values,
            aspect="auto",
            interpolation="none",
            cmap=cmap,
            norm=norm,
            vmin=vmin,
            vmax=vmax,
        )

        ax.set_yticks(np.arange(len(aq_ids)))
        ax.set_yticklabels([str(aq) for aq in aq_ids], fontsize=6)
        ax.set_xticks(np.arange(len(decades)))
        ax.set_xticklabels(decade_labels, rotation=45, ha="right", fontsize=7)
        ax.set_ylabel("AQUIFER_ID", fontsize=7)
        ax.set_title(reg, fontsize=9)

    cax = fig.add_axes([0.92, 0.15, 0.015, 0.7])
    cb = fig.colorbar(im, cax=cax)
    cb.set_label(metric_col, fontsize=9)

    title = f"Aquifer × decade {metric_col} ({label})"
    fig.suptitle(title, fontsize=11)

    metric_tag = metric_col.replace("mean_", "").lower()
    if label == "overall":
        basename = f"F15_aquifer_drought_{metric_tag}"
    else:
        basename = f"F15_aquifer_drought_{metric_tag}_{label}"

    save_figure(fig, cfg, basename)
    plt.close(fig)


def drought_metrics_from_annual_anomaly(
    annual_df: pd.DataFrame,
    threshold: float = 0.0,
) -> pd.DataFrame:
    """
    station-year anomaly (long format) -> yıllık olay benzeri metrikler.

    Girdi:
        annual_df:
            columns: ['StnID', 'Year', 'anomaly'] (veya benzer)
        threshold:
            anomaly < threshold -> "negatif" kabul edilir.

    Çıktı:
        ['StnID', 'Year', 'is_negative', 'run_length_neg',
         'MaxNegRunUpToYear', 'NumSignChangesUpToYear', ...]
    """
    df = annual_df.copy()
    df = df.sort_values(["StnID", "Year"])
    df["is_negative"] = df["anomaly"] < threshold

    rows: list[dict] = []

    for stn, sub in df.groupby("StnID", sort=False):
        years = sub["Year"].to_numpy(dtype=int)
        neg = sub["is_negative"].to_numpy(dtype=bool)

        max_run_so_far = 0
        sign_changes_so_far = 0
        prev_sign = None
        current_run = 0

        for y, flag in zip(years, neg):
            # Run length (sadece negatif bloklar)
            if flag:
                current_run += 1
                max_run_so_far = max(max_run_so_far, current_run)
            else:
                current_run = 0

            # İşaret değişimleri (negatif <-> pozitif)
            if prev_sign is not None and (flag != prev_sign):
                sign_changes_so_far += 1
            prev_sign = flag

            rows.append(
                {
                    "StnID": _norm_stnid(stn),
                    "Year": int(y),
                    "is_negative": bool(flag),
                    "run_length_neg": int(current_run if flag else 0),
                    "MaxNegRunUpToYear": int(max_run_so_far),
                    "NumSignChangesUpToYear": int(sign_changes_so_far),
                }
            )

    return pd.DataFrame(rows)


def compute_station_trends_from_annual_anomaly(
    station_annual_df: pd.DataFrame,
    decadal_df: pd.DataFrame,
    min_years: int = 10,
    alpha_fdr: float = 0.10,
) -> pd.DataFrame:
    """
    Her istasyon için yıllık anomaly serisinden (station_annual_anomalies)
    Mann–Kendall tau, S, p ve Theil–Sen eğimini hesaplar ve
    Benjamini–Hochberg FDR düzeltmesi uygular.

    Parameters
    ----------
    station_annual_df : DataFrame
        create_station_annual_anomalies_from_decadal çıkışı.
        En azından şu alanları içermeli:
            - StnID
            - Year
            - anomaly
            - (tercihen Region)
    decadal_df : DataFrame
        station_decadal_stats_with_aquifer.csv (load_decadal_stats çıktısı);
        istasyon koordinatları ve AQUIFER_ID eşleştirmesi için kullanılır.
    min_years : int, optional
        Bir istasyonda MK testi uygulayabilmek için gereken minimum yıl sayısı.
    alpha_fdr : float, optional
        FDR hedef seviyesi (örn. 0.10).

    Returns
    -------
    trends : DataFrame
        Her satır bir istasyonu temsil eder:
            - StnID
            - n_years
            - tau, S, p
            - slope (Theil–Sen, anomaly/year)
            - q_fdr, sig_fdr (FDR-BH sonuçları)
            - trend_dir ('positive', 'negative', 'near_zero')
            - Lon, Lat, Region (mümkünse)
            - AQUIFER_ID (mümkünse)
    """
    if station_annual_df is None or station_annual_df.empty:
        print("   ! compute_station_trends_from_annual_anomaly: empty station_annual_df.")
        return pd.DataFrame(
            columns=[
                "StnID",
                "n_years",
                "tau",
                "S",
                "p",
                "slope",
                "q_fdr",
                "sig_fdr",
                "trend_dir",
                "Lon",
                "Lat",
                "Region",
                "AQUIFER_ID",
            ]
        )

    df = station_annual_df.copy()

    required = {"StnID", "Year", "anomaly"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(
            f"compute_station_trends_from_annual_anomaly: missing columns {missing}"
        )

    df = df.dropna(subset=["StnID", "Year", "anomaly"]).copy()
    if df.empty:
        print("   ! compute_station_trends_from_annual_anomaly: no valid rows after dropna.")
        return pd.DataFrame(
            columns=[
                "StnID",
                "n_years",
                "tau",
                "S",
                "p",
                "slope",
                "q_fdr",
                "sig_fdr",
                "trend_dir",
                "Lon",
                "Lat",
                "Region",
                "AQUIFER_ID",
            ]
        )

    # StnID normalizasyonu (Konya mantığı ile uyumlu)
    df["StnID"] = df["StnID"].map(_norm_stnid)
    df["Year"] = df["Year"].astype(int)

    has_region = "Region" in df.columns

    records: list[dict[str, Any]] = []

    for stn_id, g in df.groupby("StnID", sort=False):
        g = g.sort_values("Year")
        vals = pd.to_numeric(g["anomaly"], errors="coerce").to_numpy()
        years = g["Year"].to_numpy(dtype=int)
        mask = np.isfinite(vals)

        if mask.sum() < min_years:
            continue

        vals = vals[mask]
        years = years[mask]

        tau, S, p = mann_kendall_test(vals)
        slope = theil_sen_slope(years, vals)

        if has_region and not g["Region"].dropna().empty:
            region = g["Region"].dropna().iloc[0]
        else:
            region = "global"

        records.append(
            {
                "StnID": stn_id,
                "n_years": int(mask.sum()),
                "tau": tau,
                "S": S,
                "p": p,
                "slope": slope,
                "Region": region,
            }
        )

    if not records:
        print(
            "   ! compute_station_trends_from_annual_anomaly: "
            "no stations with sufficient years for MK."
        )
        return pd.DataFrame(
            columns=[
                "StnID",
                "n_years",
                "tau",
                "S",
                "p",
                "slope",
                "q_fdr",
                "sig_fdr",
                "trend_dir",
                "Lon",
                "Lat",
                "Region",
                "AQUIFER_ID",
            ]
        )

    trends = pd.DataFrame.from_records(records)

    # Koordinat ve AQUIFER_ID bilgisini decadal_df üzerinden ekleyelim
    if decadal_df is not None and not decadal_df.empty:
        dec = decadal_df.copy()
        dec["StnID_norm"] = dec["StnID"].map(_norm_stnid)

        meta_cols = [c for c in ["StnID_norm", "Lon", "Lat", "Region", "AQUIFER_ID"] if c in dec.columns]
        if meta_cols:
            station_meta = (
                dec[meta_cols]
                .drop_duplicates("StnID_norm")
                .rename(columns={"StnID_norm": "StnID"})
            )

            trends = trends.merge(
                station_meta,
                on="StnID",
                how="left",
                suffixes=("", "_dec"),
            )

            # Region yoksa decadal'dan doldur
            if "Region_dec" in trends.columns:
                if "Region" not in trends.columns:
                    trends["Region"] = trends["Region_dec"]
                else:
                    trends["Region"] = trends["Region"].fillna(trends["Region_dec"])
                trends = trends.drop(columns=["Region_dec"])
        else:
            if "Lon" not in trends.columns:
                trends["Lon"] = np.nan
            if "Lat" not in trends.columns:
                trends["Lat"] = np.nan
            if "AQUIFER_ID" not in trends.columns:
                trends["AQUIFER_ID"] = np.nan
    else:
        if "Lon" not in trends.columns:
            trends["Lon"] = np.nan
        if "Lat" not in trends.columns:
            trends["Lat"] = np.nan
        if "AQUIFER_ID" not in trends.columns:
            trends["AQUIFER_ID"] = np.nan

    # FDR-BH düzeltmesi
    reject, qvals = fdr_bh(trends["p"].to_numpy(dtype=float), alpha=alpha_fdr)
    trends["q_fdr"] = qvals
    trends["sig_fdr"] = reject

    # Trend yön kategorisi
    eps = 1e-6
    slopes = trends["slope"].to_numpy(dtype=float)
    trend_dir = np.full(slopes.shape, "near_zero", dtype=object)
    trend_dir[slopes > eps] = "positive"
    trend_dir[slopes < -eps] = "negative"
    trends["trend_dir"] = trend_dir

    return trends


def compute_aquifer_share_of_station_trends(
    station_trends: pd.DataFrame,
    decadal_df: pd.DataFrame | None = None,
    min_stations: int = 5,
    min_years: int | None = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Her aquifer için, FDR anlamlı pozitif/negatif eğime sahip istasyon yüzdelerini hesaplar.

    Parameters
    ----------
    station_trends : DataFrame
        compute_station_trends_from_annual_anomaly çıktısı.
    decadal_df : DataFrame, optional
        AQUIFER_ID eksikse StnID→AQUIFER_ID eşlemesi için kullanılır.
    min_stations : int, optional
        Bir aquiferi özetleyebilmek için gereken minimum istasyon sayısı.


    min_years : int, optional
        (Opsiyonel) İstasyon trend tablosu içinde 'n_years' varsa, aquifer özetine dahil etmek
        için gereken minimum yıllık gözlem sayısı. Veri setleri arası uyumsuzluklara karşı
        savunmacı bir filtredir; genellikle station_trends zaten bu eşiği sağlayacak şekilde üretilir.
    Returns
    -------
    aq_df : DataFrame
        Her satır bir aquiferi temsil eder:
            - AQUIFER_ID
            - n_stations
            - n_sig_neg, n_sig_pos
            - frac_sig_neg, frac_sig_pos
    """
    if station_trends is None or station_trends.empty:
        print("   ! compute_aquifer_share_of_station_trends: empty station_trends.")
        return pd.DataFrame(
            columns=[
                "AQUIFER_ID",
                "n_stations",
                "n_sig_neg",
                "n_sig_pos",
                "frac_sig_neg",
                "frac_sig_pos",
            ]
        )

    df = station_trends.copy()


    # Optional: enforce a minimum record length at station level (defensive)
    # (station_trends is typically already filtered by min_years during computation)
    if min_years is not None:
        try:
            my = int(min_years)
        except Exception:
            my = None
        if my is not None and my > 0:
            if "n_years" in df.columns:
                df = df[df["n_years"] >= my].copy()
            elif "n_years_total" in df.columns:
                df = df[df["n_years_total"] >= my].copy()
    # AQUIFER_ID yoksa decadal_df ile eşle
    if ("AQUIFER_ID" not in df.columns) or df["AQUIFER_ID"].isna().all():
        if decadal_df is None or decadal_df.empty or "AQUIFER_ID" not in decadal_df.columns:
            print(
                "   ! compute_aquifer_share_of_station_trends: "
                "no AQUIFER_ID information available."
            )
            return pd.DataFrame(
                columns=[
                    "AQUIFER_ID",
                    "n_stations",
                    "n_sig_neg",
                    "n_sig_pos",
                    "frac_sig_neg",
                    "frac_sig_pos",
                ]
            )

        dec = decadal_df.copy()
        dec["StnID_norm"] = dec["StnID"].map(_norm_stnid)
        map_df = (
            dec.dropna(subset=["AQUIFER_ID"])
            .groupby("StnID_norm")["AQUIFER_ID"]
            .agg(lambda s: s.value_counts().index[0])
            .reset_index()
            .rename(columns={"StnID_norm": "StnID"})
        )

        df = df.merge(
            map_df,
            on="StnID",
            how="left",
            suffixes=("", "_map"),
        )

        if "AQUIFER_ID_map" in df.columns:
            df["AQUIFER_ID"] = df["AQUIFER_ID"].fillna(df["AQUIFER_ID_map"])
            df = df.drop(columns=["AQUIFER_ID_map"])

    df = df.dropna(subset=["AQUIFER_ID"]).copy()
    if df.empty:
        print("   ! compute_aquifer_share_of_station_trends: no rows with AQUIFER_ID.")
        return pd.DataFrame(
            columns=[
                "AQUIFER_ID",
                "n_stations",
                "n_sig_neg",
                "n_sig_pos",
                "frac_sig_neg",
                "frac_sig_pos",
            ]
        )

    records: list[dict[str, Any]] = []

    for aq, g in df.groupby("AQUIFER_ID"):
        n_tot = g["StnID"].nunique()
        if n_tot < min_stations:
            continue

        sig = g["sig_fdr"].astype(bool).to_numpy()
        slopes = g["slope"].to_numpy(dtype=float)

        n_sig_neg = int(np.logical_and(sig, slopes < 0.0).sum())
        n_sig_pos = int(np.logical_and(sig, slopes > 0.0).sum())

        rec = {
            "AQUIFER_ID": aq,
            "n_stations": int(n_tot),
            "n_sig_neg": n_sig_neg,
            "n_sig_pos": n_sig_pos,
            "frac_sig_neg": n_sig_neg / n_tot if n_tot > 0 else np.nan,
            "frac_sig_pos": n_sig_pos / n_tot if n_tot > 0 else np.nan,
        }
        records.append(rec)

    if not records:
        print(
            "   ! compute_aquifer_share_of_station_trends: "
            "no aquifers with enough stations."
        )
        return pd.DataFrame(
            columns=[
                "AQUIFER_ID",
                "n_stations",
                "n_sig_neg",
                "n_sig_pos",
                "frac_sig_neg",
                "frac_sig_pos",
            ]
        )

    aq_df = pd.DataFrame.from_records(records)
    aq_df = aq_df.sort_values("n_stations", ascending=False).reset_index(drop=True)
    return aq_df


def plot_station_trend_map(
    station_trends: pd.DataFrame,
    cfg: Config,
    world_gdf: gpd.GeoDataFrame | None = None,
    region: str = "global",
    period_label: str = "1900–2020",
    filename_suffix: str = "overall",
) -> None:
    """
    F16 – İstasyon-bazlı MK+Sen trend haritası (yıllık anomaly serisi üzerinden).

    Renk: Sen eğimi (anomaly/year, diverging cmap)
    İşaret:
        - ^ : pozitif eğim
        - v : negatif eğim
        - o : eğim ~0
    Kenar çizgisi:
        - kalın, koyu: FDR anlamlı (sig_fdr=True)
        - daha açık: FDR anlamsız
    """
    if station_trends is None or station_trends.empty:
        print("   ! plot_station_trend_map: empty station_trends; skipping.")
        return

    df = station_trends.copy()

    if not {"Lon", "Lat", "slope"}.issubset(df.columns):
        print(
            "   ! plot_station_trend_map: required columns (Lon, Lat, slope) "
            "are missing; skipping."
        )
        return

    df = df.dropna(subset=["Lon", "Lat", "slope"]).copy()
    if df.empty:
        print("   ! plot_station_trend_map: no rows with valid coords and slope.")
        return

    # Bölge filtresi (REGION_BOUNDS ile uyumlu)
    region_key = region
    region_map = {
        "global": "global",
        "Global": "global",
        "US": "US",
        "USA": "US",
        "Europe": "Europe",
        "EU": "Europe",
    }
    region_key = region_map.get(region_key, region_key)

    if region_key not in REGION_BOUNDS:
        print(f"   ! plot_station_trend_map: unknown region '{region}'; skipping.")
        return

    bounds = REGION_BOUNDS[region_key]

    mask = (
        (df["Lon"] >= bounds["lon_min"])
        & (df["Lon"] <= bounds["lon_max"])
        & (df["Lat"] >= bounds["lat_min"])
        & (df["Lat"] <= bounds["lat_max"])
    )
    sub = df[mask].copy()

    if sub.empty:
        print(f"   ! plot_station_trend_map: no stations in region '{region_key}'.")
        return

    slopes = sub["slope"].to_numpy(dtype=float)

    # Diverging norm + cmap (0 merkezli, robust p2–p98)
    vals = slopes[np.isfinite(slopes)]
    if vals.size == 0:
        print("   ! plot_station_trend_map: all slopes are NaN; skipping.")
        return

    p2, p98 = np.percentile(vals, [2, 98])
    data_min = float(np.nanmin(vals))
    data_max = float(np.nanmax(vals))
    norm, cmap, vmin, vmax = _safe_diverging_norm_and_cmap(data_min, data_max, p2, p98)

    sig = sub["sig_fdr"].astype(bool).to_numpy()
    pos = slopes > 0.0
    neg = slopes < 0.0
    zero = ~(pos | neg)

    pos_sig = pos & sig
    pos_nonsig = pos & ~sig
    neg_sig = neg & sig
    neg_nonsig = neg & ~sig
    zero_sig = zero & sig
    zero_nonsig = zero & ~sig

    n_points = len(sub)
    marker_size = get_marker_size(n_points)

    fig, ax = plt.subplots(figsize=(7.0, 3.5))

    # Basemap
    if world_gdf is not None and not world_gdf.empty:
        world_gdf.plot(
            ax=ax,
            facecolor="none",
            edgecolor="0.3",
            linewidth=0.7,
            zorder=1,
        )

    def _scatter(mask, marker, edgecolor, label, z):
        if not np.any(mask):
            return None
        sc = ax.scatter(
            sub.loc[mask, "Lon"],
            sub.loc[mask, "Lat"],
            c=sub.loc[mask, "slope"],
            cmap=cmap,
            norm=norm,
            s=marker_size,
            marker=marker,
            linewidth=0.4,
            edgecolors=edgecolor,
            zorder=z,
            label=label,
        )
        return sc

    # Önce anlamsız, sonra anlamlı grupları çizelim (anlamlılar üstte olsun)
    _scatter(neg_nonsig, "v", "0.6", "Negative (FDR ns)", 2.0)
    _scatter(pos_nonsig, "^", "0.6", "Positive (FDR ns)", 2.0)
    _scatter(zero_nonsig, "o", "0.6", "Near-zero (FDR ns)", 2.0)

    _scatter(neg_sig, "v", "k", "Negative (FDR sig)", 3.0)
    _scatter(pos_sig, "^", "k", "Positive (FDR sig)", 3.0)
    _scatter(zero_sig, "o", "k", "Near-zero (FDR sig)", 3.0)

    ax.set_xlim(bounds["lon_min"], bounds["lon_max"])
    ax.set_ylim(bounds["lat_min"], bounds["lat_max"])
    ax.set_aspect("equal", adjustable="box")

    ax.set_xlabel("Longitude", fontsize=9)
    ax.set_ylabel("Latitude", fontsize=9)
    ax.set_axisbelow(True)
    ax.grid(True, alpha=0.3, zorder=0)

    title_region = "Global" if region_key == "global" else region_key
    ax.set_title(
        f"{title_region} station MK/Sen trends (annual anomalies, {period_label})",
        fontsize=10,
    )

    # Renkbar
    cbar = fig.colorbar(
        plt.cm.ScalarMappable(norm=norm, cmap=cmap),
        ax=ax,
        shrink=0.8,
        pad=0.02,
    )
    cbar.set_label("Sen slope (anomaly units per year)", fontsize=8)

    # Lejand: basitleştirilmiş, ama işaret yönü + FDR anlamlılığı ayrışacak
    from matplotlib.lines import Line2D

    legend_handles = [
        Line2D(
            [0],
            [0],
            marker="^",
            color="none",
            markerfacecolor="none",
            markeredgecolor="0.2",
            markersize=5,
            label="Positive trend (FDR sig)",
        ),
        Line2D(
            [0],
            [0],
            marker="v",
            color="none",
            markerfacecolor="none",
            markeredgecolor="0.2",
            markersize=5,
            label="Negative trend (FDR sig)",
        ),
        Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            markerfacecolor="none",
            markeredgecolor="0.2",
            markersize=5,
            label="Near-zero trend (FDR sig)",
        ),
        Line2D(
            [0],
            [0],
            marker="^",
            color="none",
            markerfacecolor="none",
            markeredgecolor="0.6",
            markersize=5,
            label="Positive trend (FDR ns)",
        ),
        Line2D(
            [0],
            [0],
            marker="v",
            color="none",
            markerfacecolor="none",
            markeredgecolor="0.6",
            markersize=5,
            label="Negative trend (FDR ns)",
        ),
        Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            markerfacecolor="none",
            markeredgecolor="0.6",
            markersize=5,
            label="Near-zero trend (FDR ns)",
        ),
    ]

    ax.legend(
        handles=legend_handles,
        loc="lower left",
        fontsize=6,   # önce 7 idi
        frameon=True,
        framealpha=0.9,
    )

    basename = f"F16_station_MK_trends_{region_key}_{filename_suffix}"
    save_figure(fig, cfg, basename)
    plt.close(fig)


def _norm_stnid(x) -> str:
    """
    Station ID'leri normalize eder:
      - whitespace temizler,
      - olası görünmez karakterleri siler,
      - '1030007597.0' -> '1030007597' vb.
    """
    if pd.isna(x):
        return ""
    s = str(x).strip().replace("\u200b", "")  # zero-width vs.
    s2 = s.replace(",", ".")
    try:
        f = float(s2)
        if f.is_integer():
            return str(int(f))
        return s
    except Exception:
        return s


def create_station_annual_anomalies_from_decadal(
    decadal_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    station_decadal_stats_with_aquifer.csv içindeki istasyon–onyıl istatistiklerinden
    yaklaşık istasyon–yıl anomaly serisi üretir.

    Varsayım:
      - decadal_df kolonları en az:
          ['StnID', 'Decade', 'sum_anom', 'n_anom'] ve tercihen 'Region'
      - sum_anom: ilgili onyıldaki yıllık anomaly değerlerinin toplamı
      - n_anom  : ilgili onyılda geçerli yılların sayısı

    Yöntem:
      * Her istasyon–onyıl için mean_anom = sum_anom / n_anom
      * Decade (ör. 1960) başlangıç kabul edilerek, [Decade, Decade + n_anom - 1]
        yıllarına bu mean_anom değeri atanır (n_anom en fazla 10'a kırpılır).

    Çıktı:
      columns = ['StnID', 'Year', 'anomaly', 'Region']
      (Region yoksa 'global' atanır)
    """
    if decadal_df is None or decadal_df.empty:
        return pd.DataFrame(columns=["StnID", "Year", "anomaly", "Region"])

    required = {"StnID", "Decade", "sum_anom", "n_anom"}
    if not required.issubset(decadal_df.columns):
        print(
            "   ! create_station_annual_anomalies_from_decadal: "
            f"required columns {sorted(required)} missing; returning empty."
        )
        return pd.DataFrame(columns=["StnID", "Year", "anomaly", "Region"])

    df = decadal_df.dropna(subset=["StnID", "Decade"]).copy()
    df["Decade"] = df["Decade"].astype(int)
    df["n_anom"] = df["n_anom"].fillna(0).astype(int)
    df = df[df["n_anom"] > 0].copy()

    # Onyıl ortalama anomaly
    df["mean_anom"] = df["sum_anom"] / df["n_anom"]
    df = df[np.isfinite(df["mean_anom"])].copy()

    if df.empty:
        print("   ! No finite mean_anom values; cannot build station_annual_anomalies.")
        return pd.DataFrame(columns=["StnID", "Year", "anomaly", "Region"])

    records: list[dict] = []
    has_region = "Region" in df.columns

    for _, row in df.iterrows():
        stn = row["StnID"]
        dec = int(row["Decade"])
        n = int(row["n_anom"])
        mean_anom = float(row["mean_anom"])
        region = row["Region"] if has_region else "global"

        # Teorik olarak n_anom ≤ 10, yine de güvenlik için kırpıyoruz
        max_n = min(n, 10)
        for k in range(max_n):
            year = dec + k
            records.append(
                {
                    "StnID": stn,
                    "Year": year,
                    "anomaly": mean_anom,
                    "Region": region,
                }
            )

    out = pd.DataFrame.from_records(records)
    if out.empty:
        return pd.DataFrame(columns=["StnID", "Year", "anomaly", "Region"])

    out["StnID"] = out["StnID"].map(_norm_stnid)
    out = out.sort_values(["StnID", "Year"]).reset_index(drop=True)
    return out


def drought_metrics_from_annual_anomaly(
    annual_df: pd.DataFrame,
    threshold: float = 0.0,
) -> pd.DataFrame:
    """
    İstasyon-bazlı yıllık anomaly serilerinden olay-tabanlı kuraklık metrikleri üretir.

    Girdi:
        annual_df:
            columns (en az):
                - StnID
                - Year
                - anomaly
              isteğe bağlı:
                - Region

        threshold:
            anomaly < threshold olan yıllar "negatif" kabul edilir (default 0.0).

    Çıktı:
        columns:
            - StnID
            - Year
            - anomaly
            - is_negative             : anomaly < threshold?
            - run_length_neg          : ilgili yılda biten negatif run uzunluğu (yıl)
            - MaxNegRunUpToYear       : istasyon için o yıla kadar gözlenen maksimum negatif run
            - NumSignChangesUpToYear  : o yıla kadar işaret değişimi (negatif/pozitif) sayısı
            - Region (varsa)
    """
    if annual_df is None or annual_df.empty:
        print("   ! drought_metrics_from_annual_anomaly: empty input; returning empty.")
        return pd.DataFrame(
            columns=[
                "StnID",
                "Year",
                "anomaly",
                "is_negative",
                "run_length_neg",
                "MaxNegRunUpToYear",
                "NumSignChangesUpToYear",
                "Region",
            ]
        )

    required = {"StnID", "Year", "anomaly"}
    if not required.issubset(annual_df.columns):
        print(
            "   ! drought_metrics_from_annual_anomaly: "
            f"required columns {sorted(required)} missing; returning empty."
        )
        return pd.DataFrame(
            columns=[
                "StnID",
                "Year",
                "anomaly",
                "is_negative",
                "run_length_neg",
                "MaxNegRunUpToYear",
                "NumSignChangesUpToYear",
                "Region",
            ]
        )

    df = annual_df.copy()
    df = df.dropna(subset=["StnID", "Year"]).copy()
    df["Year"] = df["Year"].astype(int)
    df["StnID"] = df["StnID"].map(_norm_stnid)

    has_region = "Region" in df.columns

    records: list[dict] = []

    for stn_id, g in df.groupby("StnID", sort=False):
        g = g.sort_values("Year")
        years = g["Year"].to_numpy(dtype=int)
        vals = pd.to_numeric(g["anomaly"], errors="coerce").to_numpy(dtype=float)

        # Sadece sonlu anomaly değerlerini kullan
        valid = np.isfinite(vals)
        years = years[valid]
        vals = vals[valid]

        if years.size == 0:
            continue

        flags = vals < threshold  # negatif mi?
        region_val = g["Region"].iloc[0] if has_region else "global"

        max_run_so_far = 0
        sign_changes = 0
        prev_sign: bool | None = None
        current_run = 0

        for year, val, flag in zip(years, vals, flags):
            # Negatif run uzunluğu
            if flag:
                current_run += 1
                if current_run > max_run_so_far:
                    max_run_so_far = current_run
            else:
                current_run = 0

            # İşaret değişimi
            if prev_sign is not None and (flag != prev_sign):
                sign_changes += 1
            prev_sign = bool(flag)

            records.append(
                {
                    "StnID": stn_id,
                    "Year": int(year),
                    "anomaly": float(val),
                    "is_negative": bool(flag),
                    "run_length_neg": int(current_run if flag else 0),
                    "MaxNegRunUpToYear": int(max_run_so_far),
                    "NumSignChangesUpToYear": int(sign_changes),
                    "Region": region_val,
                }
            )

    out = pd.DataFrame.from_records(records)
    if out.empty:
        print("   ! drought_metrics_from_annual_anomaly: no valid records produced.")
    return out


def main(cfg: Config | None = None) -> None:
    if cfg is None:
        cfg = Config()

    print("[1/9] Loading annual anomaly statistics...")
    annual_df = load_annual_stats(cfg)
    print(
        f"   -> {len(annual_df)} annual records loaded "
        f"({int(annual_df['Year'].min())}–{int(annual_df['Year'].max())}), "
        f"{annual_df['Region'].nunique()} regions"
    )

    print("[2/9] Loading decadal station statistics (and attaching aquifer IDs if needed)...")
    decadal_df = load_decadal_stats(cfg)
    print(
        f"   -> {len(decadal_df)} station–decade records loaded "
        f"(decades {int(decadal_df['Decade'].min())}–{int(decadal_df['Decade'].max())})"
    )

    print("[3/9] Computing coverage metrics and plotting F1 (network coverage)...")
    annual_counts, decadal_counts = compute_network_counts(annual_df, decadal_df)
    plot_global_network_coverage(annual_counts, decadal_counts, cfg)

    print("[4/9] Plotting F2: evolution of global anomaly range (global)...")
    plot_global_anomaly_range_evolution(annual_df, cfg)

    # Additional F2 variants:
    # - p5–p95 only (trimmed band) and p0–p100 only (full band)
    # - produced for global, US, and Europe
    print("   -> Creating additional F2 variants (p5–p95 and p0–p100) for Global/US/Europe...")
    p5p95_dir, p0p100_dir = ensure_variant_fig_dirs(cfg)

    for reg in ["global", "US", "Europe"]:
        plot_anomaly_range_evolution_variant(
            annual_df,
            cfg,
            region=reg,
            mode="p5_p95",
            out_dir=p5p95_dir,
        )
        plot_anomaly_range_evolution_variant(
            annual_df,
            cfg,
            region=reg,
            mode="p0_p100",
            out_dir=p0p100_dir,
        )

  
    print("[5/9] Loading aquifer-level decadal statistics and computing overall trends...")
    aquifer_dec_df = load_aquifer_decadal_stats(cfg, decadal_df)
    
    # 3.2 F7 – Decadal share of negative vs positive anomaly years
    print("   -> Computing F7: decadal fraction of negative vs positive anomaly years...")
    decadal_frac = compute_decadal_frac_series(aquifer_dec_df, cfg)
    if decadal_frac is not None and not decadal_frac.empty:
        plot_decadal_fraction_negative_positive(decadal_frac, cfg)
    else:
        print("   ! Skipping F7: no valid decadal fraction series.")
    
    # Devam: trend hesapları (daha önce olduğu gibi)
    aquifer_trends = compute_aquifer_trends(aquifer_dec_df, cfg)
    summarize_trends_by_region(aquifer_trends, "overall (all decades)")
    multi_trends = compute_aquifer_trends_for_periods(aquifer_dec_df, cfg)
    
    for label, d_start, d_end in MULTI_TREND_PERIODS:
        tdf = multi_trends.get(label)
        if tdf is None or tdf.empty:
            continue
        summarize_trends_by_region(tdf, f"period {label}")

    # 3. Negatif/pozitif katkı asimetrisi (I_asym) – tüm dönem
    aquifer_asym = compute_aquifer_asymmetry(aquifer_dec_df, cfg)
    if aquifer_asym is not None and not aquifer_asym.empty:
        print(
            f"   -> {len(aquifer_asym)} aquifers with valid asymmetry index "
            f"(min_decades={cfg.trend_min_decades_per_aquifer}, "
            f"min_stations_per_decade={cfg.trend_min_stations_per_decade})"
        )
        summarize_asymmetry_by_region(aquifer_asym, cfg, "overall (all decades)")
    else:
        print("   ! No aquifer asymmetry indices could be computed.")

    print(
        f"   -> {len(aquifer_trends)} aquifers with sufficient data for overall trend "
        f"(min_decades={cfg.trend_min_decades_per_aquifer}, "
        f"min_stations_per_decade={cfg.trend_min_stations_per_decade})"
    )

    # Genel (tüm tarihsel) trend özeti
    trend_out_path = os.path.join(cfg.anomalies_dir, cfg.aquifer_trend_summary_file)
    aquifer_trends.to_csv(trend_out_path, index=False)
    print(f"   -> Overall aquifer trend summary written to: {trend_out_path}")

    # Genel dönem için negatif eğim hotspot'ları
    identify_negative_slope_hotspots(
        aquifer_trends,
        cfg,
        label="overall (all decades)",
        period_tag="overall",
    )

    # Dünya basemap (Natural Earth)
    world = None
    if os.path.isfile(cfg.path_world_shp):
        try:
            world = gpd.read_file(cfg.path_world_shp)
            if world.crs is None:
                world = world.set_crs("EPSG:4326")
            elif world.crs.to_string() != "EPSG:4326":
                world = world.to_crs("EPSG:4326")
        except Exception as e:
            print(f"[WARN] Could not load world basemap from {cfg.path_world_shp}: {e}")
            world = None
    else:
        print(
            f"[WARN] World basemap shapefile not found at {cfg.path_world_shp}. "
            "Download a Natural Earth 'admin 0 countries' shapefile and update this path if needed."
        )

    # 3.1 Asimetri haritaları (F6) – tüm dönem + üç dönem
    # 5d) Çok dönemli (3 alt pencere) asimetri ve F6 haritaları
    multi_asym = compute_aquifer_asymmetry_for_periods(aquifer_dec_df, cfg)

    for label, d_start, d_end in MULTI_TREND_PERIODS:
        asym_df = multi_asym.get(label)
        if asym_df is None or asym_df.empty:
            continue

        period_tag = f"{d_start}_{d_end}"

        # Bölgesel özet (log dosyasına da yazılıyor)
        summarize_asymmetry_by_region(
            asym_df,
            cfg,
            label=f"period {label} ({d_start}–{d_end})",
        )

        # Her dönem + bölge için F6
        for reg in ["global", "US", "Europe"]:
            print(f"   -> F6 asymmetry map ({label}, {reg})")
            plot_aquifer_asymmetry_map(
                asym_df,
                cfg,
                region=reg,
                world_gdf=world,
                title_suffix=f"{d_start}–{d_end}",
                filename_suffix=period_tag,   # <-- ÖNEMLİ
            )

    # 5e) Persistens analizi ve F8 haritaları (maksimum negatif onyıl run'u)
    print("   -> Computing decadal persistence (run lengths), writing CSV summaries, and F8 maps...")
    persist_pack = save_aquifer_persistence_outputs(aquifer_dec_df, cfg)
    aquifer_persist = persist_pack["overall"]
    multi_persist = persist_pack["periods"]

    if aquifer_persist is not None and not aquifer_persist.empty:
        summarize_persistence_by_region(aquifer_persist, cfg, "overall (all decades)")

        # F8: tüm dönem için (1900–2020) Global / US / Europe
        for reg in ["global", "US", "Europe"]:
            print(f"   -> F8 persistence map (overall, {reg})")
            plot_aquifer_persistence_map(
                aquifer_persist,
                cfg,
                region=reg,
                world_gdf=world,
                title_suffix="1900–2020",
                filename_suffix="1900_2020",
            )

        # Üç alt dönem için persistens ve F8 haritaları
        for label, d_start, d_end in MULTI_TREND_PERIODS:
            p_df = multi_persist.get(label)
            if p_df is None or p_df.empty:
                continue

            period_tag = f"{d_start}_{d_end}"

            summarize_persistence_by_region(
                p_df,
                cfg,
                label=f"period {label} ({d_start}–{d_end})",
            )

            for reg in ["global", "US", "Europe"]:
                print(f"   -> F8 persistence map ({label}, {reg})")
                plot_aquifer_persistence_map(
                    p_df,
                    cfg,
                    region=reg,
                    world_gdf=world,
                    title_suffix=f"{d_start}–{d_end}",
                    filename_suffix=period_tag,
                )
    else:
        print("   ! Skipping F8 maps; no aquifer persistence data available.")

    print("[6/9] Plotting F3 (overall trend maps) and F4 (overall trend distribution)...")

    # 6a) MK anlamlılık temelli haritalar (F3, overall)
    for reg in ["global", "US", "Europe"]:
        print(f"   -> F3 overall map (significance) for region: {reg}")
        plot_aquifer_decadal_trend_map(
            aquifer_trends,
            cfg,
            region=reg,
            title_suffix="1900–2020",
            filename_suffix=f"{reg}_1900_2020",
            world_gdf=world,
            classification="significance",
        )

    # 6b) Yön (Sen's slope işareti) temelli haritalar (F3, overall)
    for reg in ["global", "US", "Europe"]:
        print(f"   -> F3 overall map (direction) for region: {reg}")
        plot_aquifer_decadal_trend_map(
            aquifer_trends,
            cfg,
            region=reg,
            title_suffix="1900–2020",
            filename_suffix=f"{reg}_1900_2020_dir",
            world_gdf=world,
            classification="direction",
        )

    # 6c) Trend dağılımı (F4)
    plot_aquifer_trend_distribution(aquifer_trends, cfg)

    print("[7/9] Multi-period trend analysis and F3 maps per period...")
    multi_trends = compute_aquifer_trends_for_periods(aquifer_dec_df, cfg)

    for label, d_start, d_end in MULTI_TREND_PERIODS:
        tdf = multi_trends.get(label)
        if tdf is None or tdf.empty:
            continue

        period_tag = f"{d_start}_{d_end}"
        out_path = os.path.join(
            cfg.anomalies_dir,
            f"aquifer_trend_summary_{period_tag}.csv",
        )
        tdf.to_csv(out_path, index=False)
        print(f"   -> Period '{label}' trend summary written to: {out_path}")

        identify_negative_slope_hotspots(
            tdf,
            cfg,
            label=f"period {label}",
            period_tag=period_tag,
        )

        # F3 – MK anlamlılık haritaları (period bazlı)
        for reg in ["global", "US", "Europe"]:
            print(f"      · F3 map (significance) for period {label}, region {reg}")
            fname_suffix = f"{reg}_{period_tag}"
            plot_aquifer_decadal_trend_map(
                tdf,
                cfg,
                region=reg,
                title_suffix=label,
                filename_suffix=fname_suffix,
                world_gdf=world,
                classification="significance",
            )

        # F3 – yön haritaları (period bazlı)
        for reg in ["global", "US", "Europe"]:
            print(f"      · F3 map (direction) for period {label}, region {reg}")
            fname_suffix = f"{reg}_{period_tag}_dir"
            plot_aquifer_decadal_trend_map(
                tdf,
                cfg,
                region=reg,
                title_suffix=label,
                filename_suffix=fname_suffix,
                world_gdf=world,
                classification="direction",
            )

    # 2.2 Station-level regime classification (F5)
    print("[8/9] Station-level regime classification and F5 maps...")
    station_features = compute_station_regime_features(decadal_df, cfg)
    print(
        f"   -> {len(station_features)} stations with at least "
        f"{cfg.station_regime_min_decades} decades for regime analysis"
    )

    station_regimes = cluster_station_regimes(station_features, cfg)

    # world_gdf daha önce F3 için yüklenmişti; aynı nesneyi kullanıyoruz
    plot_station_regime_maps(station_regimes, cfg, world)

    # Extra: Yıllık anomaly tabanlı olay-tabanlı kuraklık metrikleri (3.1)
    print("[Extra] Event-based drought metrics from annual anomalies (3.1)...")
    try:
        station_annual_df = None

        # Preferred: directly use QC-filtered station-year anomalies.
        # (This eliminates any ambiguity from decadal back-calculation.)
        try:
            station_annual_df = load_station_anomalies_all(cfg)
        except Exception as e:
            print(
                "   ! Could not load station_anomalies_all.csv; "
                "falling back to decadal-based reconstruction. "
                f"Reason: {e}"
            )
            station_annual_df = create_station_annual_anomalies_from_decadal(decadal_df)

        if station_annual_df is None or station_annual_df.empty:
            print(
                "   ! Station annual anomalies are not available; "
                "skipping annual anomaly-based event metrics."
            )
            station_annual_df = None
        else:
            # Optional sidecar export for inspection/debugging (write once; do not overwrite).
            station_annual_path = os.path.join(
                cfg.anomalies_dir,
                "station_annual_anomalies.csv",
            )
            if not os.path.exists(station_annual_path):
                station_annual_df.to_csv(station_annual_path, index=False)
                try:
                    station_annual_df.to_excel(
                        os.path.splitext(station_annual_path)[0] + ".xlsx",
                        index=False,
                    )
                except Exception as e:
                    print(f"   ! Could not write Excel station_annual_anomalies: {e}")
            print(
                f"   -> Station-level annual anomalies written to: "
                f"{station_annual_path}"
            )

            annual_drought_metrics = drought_metrics_from_annual_anomaly(
                station_annual_df,
                threshold=0.0,
            )
            if annual_drought_metrics is None or annual_drought_metrics.empty:
                print(
                    "   ! drought_metrics_from_annual_anomaly produced no records; "
                    "skipping write-out."
                )
            else:
                dm_path = os.path.join(
                    cfg.anomalies_dir,
                    "station_drought_metrics_from_annual_anomaly.csv",
                )
                annual_drought_metrics.to_csv(dm_path, index=False)
                print(
                    "   -> Station-level annual drought metrics written to: "
                    f"{dm_path}"
                )
    except Exception as e:
        print(
            "   ! Error while computing annual anomaly-based event metrics: "
            f"{e}"
        )
        station_annual_df = None

    # 3.2 Station-level MK + Sen + FDR-BH trend analizi (yıllık anomaly)
    print("[Extra] Station-level MK+Sen trend analysis from annual anomalies (3.2)...")
    try:
        if station_annual_df is None or station_annual_df.empty:
            print(
                "   ! station_annual_df is not available; "
                "skipping station-level MK+Sen trends (3.2)."
            )
        else:
            # --- Genel dönem (1900–2020) istasyon trendleri ---
            station_trends = compute_station_trends_from_annual_anomaly(
                station_annual_df,
                decadal_df,
                min_years=10,
                alpha_fdr=0.10,
            )
            if station_trends is None or station_trends.empty:
                print(
                    "   ! No station-level MK+Sen trends could be computed from "
                    "station_annual_anomalies.csv; skipping F16 maps."
                )
            else:
                st_trend_path = os.path.join(
                    cfg.anomalies_dir,
                    "station_trends_from_annual_anomaly_MK.csv",
                )
                station_trends.to_csv(st_trend_path, index=False)
                print(
                    "   -> Station-level MK+Sen+FDR trend summary written to: "
                    f"{st_trend_path}"
                )

                # Aquifer-scale share of significantly decreasing vs increasing stations
                try:
                    aq_station_share = compute_aquifer_share_of_station_trends(
                        station_trends,
                        decadal_df,
                        min_years=10,
                    )
                except Exception as e2:
                    print(
                        "   ! Error while computing aquifer-level share of station trends "
                        f"(3.2): {e2}"
                    )
                    aq_station_share = None

                if aq_station_share is not None and not aq_station_share.empty:
                    aq_share_path = os.path.join(
                        cfg.anomalies_dir,
                        "aquifer_share_of_station_trends_from_annual_anomaly.csv",
                    )
                    aq_station_share.to_csv(aq_share_path, index=False)
                    print(
                        "   -> Aquifer-scale share of station trends written to: "
                        f"{aq_share_path}"
                    )

                # F16: Station trend maps (global, US, Europe; overall)
                for reg in ["global", "US", "Europe"]:
                    print(
                        f"   -> F16 station MK trend map from annual anomalies "
                        f"(overall, {reg})"
                    )
                    plot_station_trend_map(
                        station_trends,
                        cfg,
                        world_gdf=world,
                        region=reg,
                        period_label="1900–2020",
                        filename_suffix=f"{reg}_1900_2020",
                    )

                # ---------------------------------------------------------
                # 3 alt dönem için ek istasyon trendleri ve F16 haritaları
                # ---------------------------------------------------------
                for label, y_start, y_end in MULTI_TREND_PERIODS:
                    print(
                        f"   -> Station MK+Sen trends for period {label} "
                        f"({y_start}–{y_end})"
                    )

                    # İlgili dönem için yıllık anomaly alt kümesi
                    sub_annual = station_annual_df[
                        (station_annual_df["Year"] >= y_start)
                        & (station_annual_df["Year"] <= y_end)
                    ].copy()

                    if sub_annual.empty:
                        print(
                            f"      ! No station annual anomalies for period {label}; "
                            "skipping."
                        )
                        continue

                    st_trends_period = compute_station_trends_from_annual_anomaly(
                        sub_annual,
                        decadal_df,
                        min_years=10,
                        alpha_fdr=0.10,
                    )

                    if st_trends_period is None or st_trends_period.empty:
                        print(
                            f"      ! No station MK+Sen trends for period {label}; "
                            "skipping."
                        )
                        continue

                    period_tag = f"{y_start}_{y_end}"

                    # İstasyon trend özetini kaydet
                    st_trend_path_p = os.path.join(
                        cfg.anomalies_dir,
                        f"station_trends_from_annual_anomaly_MK_{period_tag}.csv",
                    )
                    st_trends_period.to_csv(st_trend_path_p, index=False)
                    print(
                        "      -> Station-level MK+Sen+FDR trend summary written to: "
                        f"{st_trend_path_p}"
                    )

                    # Aquifer-scale paylar (isteğe bağlı ama tutarlı olsun diye ekliyoruz)
                    try:
                        aq_station_share_p = compute_aquifer_share_of_station_trends(
                            st_trends_period,
                            decadal_df,
                            min_years=10,
                        )
                    except Exception as e3:
                        print(
                            "      ! Error while computing aquifer-level share of station "
                            f"trends for period {label}: {e3}"
                        )
                        aq_station_share_p = None

                    if aq_station_share_p is not None and not aq_station_share_p.empty:
                        aq_share_path_p = os.path.join(
                            cfg.anomalies_dir,
                            f"aquifer_share_of_station_trends_from_annual_anomaly_{period_tag}.csv",
                        )
                        aq_station_share_p.to_csv(aq_share_path_p, index=False)
                        print(
                            "      -> Aquifer-scale share of station trends written to: "
                            f"{aq_share_path_p}"
                        )

                    # F16: Station trend maps (global, US, Europe; period bazlı)
                    for reg in ["global", "US", "Europe"]:
                        print(
                            f"      -> F16 station MK trend map from annual anomalies "
                            f"({label}, {reg})"
                        )
                        plot_station_trend_map(
                            st_trends_period,
                            cfg,
                            world_gdf=world,
                            region=reg,
                            period_label=label,
                            filename_suffix=f"{reg}_{period_tag}",
                        )
    except Exception as e:
        print(
            "   ! Error while computing station-level MK+Sen trends (3.2): "
            f"{e}"
        )

    # 6.1 Aquifer anomaly "stripes" (F12)
    print("[Extra] Aquifer anomaly stripes (F12)...")
    plot_aquifer_anomaly_stripes(aquifer_dec_df, cfg)

    print("[Extra] Trimming and outlier diagnostics (F9–F10)...")
    run_trimming_diagnostics(cfg, world)

    print("[Extra] Repeated global-outlier station diagnostics (F11)...")
    run_repeated_outlier_diagnostics(cfg, world)

    # 6.2 Decadal distribution of station mean anomalies (F13)
    print("[9/9] Plotting F13: decadal distribution of station mean anomalies...")
    plot_decadal_station_mean_anomaly_distributions(decadal_df, cfg)

    print("Done. Figures saved to:", cfg.out_fig_dir)
    # Variant output folders (added without removing existing outputs)
    p5p95_dir, p0p100_dir = ensure_variant_fig_dirs(cfg)
    print("   -> p5–p95 (trimmed) variants:", p5p95_dir)
    print("   -> p0–p100 (full-range) variants:", p0p100_dir)

if __name__ == "__main__":
    main()
