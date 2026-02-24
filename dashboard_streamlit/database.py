"""
DuckDB Database Wrapper for Canadian Trade Dashboard

Implements a three-tier data architecture for fast cold-start loading and
on-demand detail access:

  Tier 1 — summary_chapter.parquet  (~25 MB)   cold-start download
  Tier 2 — summary_heading.parquet  (~80 MB)   lazy-loaded on chapter select
  Tier 3 — trade_by_year/trade_YYYY.parquet     lazy-loaded per year selected

Query routing:
  KPI, time_series, top_destinations, top_provinces, top_hs_codes → Tier 1
  top_hs_headings (chapter selected)                               → Tier 2
  commodities, hs_headings filter, hs_commodities                  → Tier 3

Display helpers:
  dest_display_name(raw)  — strip ISO prefix and apply name overrides
  get_countries()         — returns {display_name: raw_value} dict
"""

import duckdb
import pandas as pd
import requests
import os
import json
import glob as _glob
import streamlit as st
from pathlib import Path
from typing import Dict, List, Any, Optional

# ---------------------------------------------------------------------------
# Country display name overrides
# Stored as "ISO - Name" in the parquet. This map produces the clean label
# shown in the UI (no ISO prefix, corrected/shortened names where needed).
# Keys must exactly match the raw destination string in trade_records.parquet.
# ---------------------------------------------------------------------------
_DEST_DISPLAY_NAMES: Dict[str, str] = {
    # Historical name stored in data — remap to modern name
    "UKR - Ukraine Soviet Socialist Republic": "Ukraine",
    "CUW - Netherlands Antilles": "Netherlands Antilles (historical)",
    "SRB - Former Yugoslavia": "Former Yugoslavia (historical)",

    # Long/formal UN names → common English names
    "USA - United States of America": "United States",
    "GBR - United Kingdom": "United Kingdom",
    "RUS - Russian Federation": "Russia",
    "IRN - Iran": "Iran",
    "PRK - Korea, North": "North Korea",
    "KOR - Korea, South": "South Korea",
    "COD - Congo, Democratic Republic of the": "DR Congo",
    "COG - Congo, Republic of the": "Republic of Congo",
    "TZA - Tanzania, United Republic of": "Tanzania",
    "MDA - Moldova, Republic of": "Moldova",
    "VNM - Viet Nam": "Vietnam",
    "BRN - Brunei Darussalam": "Brunei",
    "SYR - Syria": "Syria",
    "LAO - Laos": "Laos",
    "FSM - Micronesia, Federated States of": "Micronesia",
    "MKD - North Macedonia": "North Macedonia",
    "ZAF - South Africa, Republic of": "South Africa",
    "BOL - Bolivia": "Bolivia",
    "VEN - Venezuela": "Venezuela",
    "GMB - Gambia": "Gambia",
    "VAT - Holy See (Vatican City State)": "Vatican City",
    "SHN - Saint Helena, Ascension and Tristan da Cunha": "Saint Helena",
    "FLK - Falkland Islands (Malvinas)": "Falkland Islands",
    "MAC - Macao": "Macao",
    "XZZ - High Seas": "High Seas",
    "TUR - Türkiye": "Turkey",
    "CPV - Cabo Verde": "Cape Verde",
    "SWZ - Eswatini": "Eswatini",
    "BES - Bonaire, Sint Eustatius and Saba": "Bonaire & Sint Eustatius",
    "CIV - Côte d'Ivoire": "Côte d'Ivoire",
    "PSE - Palestine, State of": "Palestine",
    "TWN - Taiwan": "Taiwan",
}


def dest_display_name(raw: str) -> str:
    """
    Convert a raw 'ISO - Name' destination string to its clean display label.
    Falls back to stripping the ISO prefix if no explicit override exists.
    """
    if raw in _DEST_DISPLAY_NAMES:
        return _DEST_DISPLAY_NAMES[raw]
    if " - " in raw:
        return raw.split(" - ", 1)[1]
    return raw


# ---------------------------------------------------------------------------
# Destination display-name helpers
# ---------------------------------------------------------------------------

_DEST_DISPLAY_NAMES: Dict[str, str] = {
    # Historical / renamed countries
    "UKR - Ukraine Soviet Socialist Republic": "Ukraine",
    "CUW - Netherlands Antilles": "Netherlands Antilles (historical)",
    "SRB - Former Yugoslavia": "Former Yugoslavia (historical)",
    # Spelling / official name simplifications
    "USA - United States of America": "United States",
    "GBR - United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    "GBR - United Kingdom": "United Kingdom",
    "RUS - Russian Federation": "Russia",
    "VNM - Viet Nam": "Vietnam",
    "TUR - Türkiye": "Turkey",
    "IRN - Iran (Islamic Republic of)": "Iran",
    "PRK - Korea (Democratic People's Republic of)": "North Korea",
    "KOR - Korea (Republic of)": "South Korea",
    "MDA - Moldova (Republic of)": "Moldova",
    "SYR - Syrian Arab Republic": "Syria",
    "TZA - Tanzania (United Republic of)": "Tanzania",
    "BOL - Bolivia (Plurinational State of)": "Bolivia",
    "VEN - Venezuela (Bolivarian Republic of)": "Venezuela",
    "LAO - Lao People's Democratic Republic": "Laos",
    "MKD - North Macedonia": "North Macedonia",
    "PSE - Palestine, State of": "Palestine",
    "COD - Congo, Democratic Republic of": "DR Congo",
    "COG - Congo": "Republic of Congo",
    "CIV - Côte d'Ivoire": "Côte d'Ivoire",
    "FSM - Micronesia (Federated States of)": "Micronesia",
    "VCT - Saint Vincent and the Grenadines": "St. Vincent & Grenadines",
    "KNA - Saint Kitts and Nevis": "St. Kitts & Nevis",
    "STP - Sao Tome and Principe": "São Tomé & Príncipe",
    "TTO - Trinidad and Tobago": "Trinidad & Tobago",
    "ATG - Antigua and Barbuda": "Antigua & Barbuda",
    "BIH - Bosnia and Herzegovina": "Bosnia & Herzegovina",
    "SLE - Sierra Leone": "Sierra Leone",
    "GNB - Guinea-Bissau": "Guinea-Bissau",
    "ARE - United Arab Emirates": "UAE",
}


def dest_display_name(raw: str) -> str:
    """
    Convert a raw destination string (e.g. 'USA - United States of America')
    to a clean display label ('United States').

    Priority:
      1. Exact match in _DEST_DISPLAY_NAMES override table
      2. Strip leading 'ISO - ' prefix
      3. Return raw unchanged
    """
    if raw in _DEST_DISPLAY_NAMES:
        return _DEST_DISPLAY_NAMES[raw]
    if " - " in raw:
        return raw.split(" - ", 1)[1]
    return raw


# ---------------------------------------------------------------------------
# GitHub Release URLs (update after uploading to a release)
# ---------------------------------------------------------------------------
_RELEASE_BASE = (
    "https://github.com/pocketpiston/Canada-Trade-Dashboard"
    "/releases/latest/download"
)
_TIER1_URL = f"{_RELEASE_BASE}/summary_chapter.parquet"
_TIER2_URL = f"{_RELEASE_BASE}/summary_heading.parquet"
_TIER3_URL_TEMPLATE = f"{_RELEASE_BASE}/trade_{{year}}.parquet"

# Legacy monolithic file (fallback / build-from-source path)
_LEGACY_URL = f"{_RELEASE_BASE}/trade_records.parquet"


# ---------------------------------------------------------------------------
# TradeDatabase
# ---------------------------------------------------------------------------

class TradeDatabase:
    """
    Interface to Canadian trade data stored in tiered Parquet files.

    Uses DuckDB to query Parquet files directly without loading into memory.
    Tier 1 (chapter summary) is downloaded on cold-start.
    Tier 2 (heading summary) and Tier 3 (per-year commodity) are lazy-loaded.
    """

    def __init__(self, data_dir: str = None):
        """
        Initialize database connection.

        Args:
            data_dir: Directory containing Parquet files.
                      Defaults to <project_root>/data/processed/
        """
        if data_dir is None:
            base_dir = Path(__file__).resolve().parent.parent
            self.data_dir = base_dir / "data" / "processed"
        else:
            self.data_dir = Path(data_dir)

        self.data_dir.mkdir(parents=True, exist_ok=True)

        # ── Tier paths ──────────────────────────────────────────────────────
        self.tier1_file   = self.data_dir / "summary_chapter.parquet"
        self.tier2_file   = self.data_dir / "summary_heading.parquet"
        self.tier3_dir    = self.data_dir / "trade_by_year"
        self.metadata_file = self.data_dir / "tier_metadata.json"

        # Legacy monolithic parquet (still used as fallback / source)
        self.legacy_parquet = self.data_dir / "trade_records.parquet"

        # ── State flags ─────────────────────────────────────────────────────
        self._tier2_loaded: bool = False
        self._tier3_years_loaded: set = set()

        # ── DuckDB (in-memory) ───────────────────────────────────────────────
        self.conn = duckdb.connect(':memory:')

        # ── Initialize views from whatever data is available ─────────────────
        self._initialize_views()

    # ────────────────────────────────────────────────────────────────────────
    # Data availability checks
    # ────────────────────────────────────────────────────────────────────────

    def has_tier1(self) -> bool:
        return self.tier1_file.exists()

    def has_tier2(self) -> bool:
        return self.tier2_file.exists()

    def has_tier3_year(self, year: int) -> bool:
        return (self.tier3_dir / f"trade_{year}.parquet").exists()

    def has_data(self) -> bool:
        """True if at least Tier 1 (or legacy) data is present."""
        return self.has_tier1() or self.legacy_parquet.exists()

    # ────────────────────────────────────────────────────────────────────────
    # Downloader helpers
    # ────────────────────────────────────────────────────────────────────────

    def _download_file(
        self,
        url: str,
        dest: Path,
        label: str = "data",
    ) -> None:
        """
        Stream-download *url* to *dest* with a Streamlit progress bar.

        Raises st.stop() on failure so the dashboard surfaces a clear error.
        """
        try:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            dest.parent.mkdir(parents=True, exist_ok=True)

            if total_size > 0:
                pbar = st.progress(0, text=f"Downloading {label}…")
                downloaded = 0
                with open(dest, "wb") as f:
                    for chunk in response.iter_content(chunk_size=65_536):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            pct = min(downloaded / total_size, 1.0)
                            mb_done = downloaded / 1_048_576
                            mb_total = total_size / 1_048_576
                            pbar.progress(
                                pct,
                                text=f"Downloading {label}: "
                                     f"{mb_done:.1f} / {mb_total:.1f} MB",
                            )
                pbar.empty()
            else:
                # No content-length header — download without progress
                with open(dest, "wb") as f:
                    f.write(response.content)

            st.success(f"✅ {label} downloaded successfully!")

        except requests.exceptions.RequestException as e:
            st.error(f"❌ Failed to download {label}: {e}")
            st.info(
                f"**Manual option:** download from `{url}` "
                f"and place at `{dest}`"
            )
            st.stop()
        except Exception as e:
            st.error(f"❌ Unexpected error downloading {label}: {e}")
            st.stop()

    def _download_tier1(self) -> None:
        """Download Tier 1 (chapter summary, ~25 MB) on cold-start."""
        self._download_file(_TIER1_URL, self.tier1_file, "chapter summary (Tier 1)")

    def _ensure_tier2(self) -> None:
        """Download Tier 2 (heading summary, ~80 MB) if not already present."""
        if not self.has_tier2():
            with st.spinner("Loading heading detail data (first time only)…"):
                self._download_file(
                    _TIER2_URL, self.tier2_file, "heading summary (Tier 2)"
                )
            # Register the new view
            self._register_tier2_view()

    def _ensure_tier3_year(self, year: int) -> None:
        """Download a single Tier 3 year file (~12 MB) if not already present."""
        if not self.has_tier3_year(year):
            dest = self.tier3_dir / f"trade_{year}.parquet"
            self.tier3_dir.mkdir(parents=True, exist_ok=True)
            url = _TIER3_URL_TEMPLATE.format(year=year)
            with st.spinner(f"Loading {year} commodity data (first time only)…"):
                self._download_file(url, dest, f"commodity data {year} (Tier 3)")

    # ────────────────────────────────────────────────────────────────────────
    # View registration
    # ────────────────────────────────────────────────────────────────────────

    def _register_tier1_view(self) -> None:
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW tier1 AS
            SELECT * FROM read_parquet('{self.tier1_file}')
        """)

    def _register_tier2_view(self) -> None:
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW tier2 AS
            SELECT * FROM read_parquet('{self.tier2_file}')
        """)

    def _register_tier3_view(self) -> None:
        """Register a glob view over all downloaded Tier 3 year files."""
        pattern = str(self.tier3_dir / "trade_*.parquet")
        files = _glob.glob(pattern)
        if files:
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW tier3 AS
                SELECT * FROM read_parquet('{pattern}')
            """)

    def _register_legacy_view(self) -> None:
        """Fallback: register the old monolithic parquet as both tier1 and trade_records."""
        pattern = str(self.data_dir / "trade_records*.parquet")
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW trade_records AS
            SELECT * FROM read_parquet('{pattern}')
        """)
        # Expose the legacy data as tier1 using pre-computation at query time
        # (legacy file has raw columns without enrichment columns)
        self.conn.execute("""
            CREATE OR REPLACE VIEW tier1 AS
            SELECT
                date_trunc('month', date)::DATE AS date,
                CAST(year AS SMALLINT) AS year,
                trade_type,
                province,
                destination,
                CASE WHEN destination LIKE '% - %'
                     THEN regexp_replace(destination, '^[^-]+ - ', '')
                     ELSE destination END AS destination_name,
                CASE WHEN destination LIKE '% - %'
                     THEN split_part(destination, ' - ', 1)
                     ELSE NULL END AS destination_iso,
                hs_chapter,
                CASE WHEN chapter LIKE '% - %'
                     THEN regexp_replace(chapter, '^[^-]+ - ', '')
                     ELSE chapter END AS chapter_name,
                NULL AS chapter_summary,
                NULL AS category,
                NULL AS category_color,
                SUM(value)   AS value,
                COUNT(*)     AS record_count
            FROM trade_records
            WHERE hs_chapter IS NOT NULL
            GROUP BY ALL
        """)

    def _register_empty_views(self) -> None:
        """Register empty views so the app loads without error when no data exists."""
        empty_tier1 = """
            SELECT
                CAST(NULL AS DATE)    AS date,
                CAST(NULL AS SMALLINT) AS year,
                CAST(NULL AS VARCHAR) AS trade_type,
                CAST(NULL AS VARCHAR) AS province,
                CAST(NULL AS VARCHAR) AS destination,
                CAST(NULL AS VARCHAR) AS destination_name,
                CAST(NULL AS VARCHAR) AS destination_iso,
                CAST(NULL AS VARCHAR) AS hs_chapter,
                CAST(NULL AS VARCHAR) AS chapter_name,
                CAST(NULL AS VARCHAR) AS chapter_summary,
                CAST(NULL AS VARCHAR) AS category,
                CAST(NULL AS VARCHAR) AS category_color,
                CAST(0 AS DOUBLE)     AS value,
                CAST(0 AS BIGINT)     AS record_count
            WHERE 1=0
        """
        self.conn.execute(f"CREATE OR REPLACE VIEW tier1 AS {empty_tier1}")
        self.conn.execute(f"CREATE OR REPLACE VIEW tier2 AS {empty_tier1}")

    def _initialize_views(self) -> None:
        """
        Register DuckDB views based on whatever files are available:
          1. Tier 1 parquet → preferred
          2. Legacy monolithic parquet → fallback
          3. Empty views → no data yet
        """
        if self.has_tier1():
            self._register_tier1_view()
        elif self.legacy_parquet.exists():
            self._register_legacy_view()
        else:
            self._register_empty_views()

        # Tier 2 view (optional — registered if file already exists)
        if self.has_tier2():
            self._register_tier2_view()

        # Tier 3 view (optional — registered if any year files already exist)
        if self.tier3_dir.exists() and list(self.tier3_dir.glob("trade_*.parquet")):
            self._register_tier3_view()

    # ────────────────────────────────────────────────────────────────────────
    # Filter helpers
    # ────────────────────────────────────────────────────────────────────────

    def get_common_options(self) -> Dict[str, Any]:
        """
        Return static filter options (chapters, date range, trade types).
        These are read from Tier 1.
        """
        try:
            chapters = self.conn.execute("""
                SELECT DISTINCT hs_chapter, chapter_name
                FROM tier1
                WHERE hs_chapter IS NOT NULL
                ORDER BY hs_chapter
            """).df().rename(columns={"chapter_name": "chapter"}).to_dict("records")

            date_res = self.conn.execute("""
                SELECT
                    MIN(date)  AS min_date,
                    MAX(date)  AS max_date,
                    MIN(year)  AS min_year,
                    MAX(year)  AS max_year
                FROM tier1
            """).df()

            if date_res.empty or date_res.iloc[0]["min_date"] is None:
                date_range = {
                    "min_date": "2023-01-01",
                    "max_date": "2025-12-31",
                    "min_year": 2023,
                    "max_year": 2025,
                }
            else:
                date_range = date_res.iloc[0].to_dict()

            trade_types = self.conn.execute("""
                SELECT DISTINCT trade_type
                FROM tier1
                WHERE trade_type IS NOT NULL
                ORDER BY trade_type
            """).df()["trade_type"].tolist()

            if not trade_types:
                trade_types = ["Export", "Import"]

            return {
                "chapters": chapters,
                "date_range": date_range,
                "trade_types": trade_types,
            }
        except Exception:
            return {
                "chapters": [],
                "date_range": {
                    "min_date": "2023-01-01",
                    "max_date": "2025-12-31",
                    "min_year": 2023,
                    "max_year": 2025,
                },
                "trade_types": ["Export", "Import"],
            }

    def get_provinces(self, trade_type: str = "All") -> List[str]:
        """Return distinct provinces from Tier 1, optionally filtered by trade type."""
        where = "province != 'Canada (Total)'"
        if trade_type != "All":
            where += f" AND trade_type = '{trade_type}'"
        return self.conn.execute(f"""
            SELECT DISTINCT province FROM tier1 WHERE {where} ORDER BY province
        """).df()["province"].tolist()

    def get_countries(self, trade_type: str = 'All') -> Dict[str, str]:
        """
        Get partner countries/destinations/origins filtered by trade type.

        Returns:
            Dict mapping clean display name -> raw parquet value.
            e.g. {"United States": "USA - United States of America", ...}
            Sorted alphabetically by display name.
        """
        where_clause = "destination IS NOT NULL"
        if trade_type != 'All':
            where_clause += f" AND trade_type = '{trade_type}'"

        raw_list = self.conn.execute(f"""
            SELECT DISTINCT destination
            FROM trade_records
            WHERE {where_clause}
            ORDER BY destination
        """).df()['destination'].tolist()

        # Build display_name -> raw mapping, deduplicating on display name
        # (historical duplicates like Netherlands Antilles / Curaçao share ISO
        # but get distinct override labels, so they stay as separate entries)
        result: Dict[str, str] = {}
        for raw in raw_list:
            display = dest_display_name(raw)
            result[display] = raw

        return dict(sorted(result.items()))
    
    def get_filter_options(self) -> Dict[str, Any]:
        """Legacy support — returns all options defaulting to All."""
        common = self.get_common_options()
        common['provinces'] = self.get_provinces('All')
        # Returns dict {display_name: raw_value}
        common['destinations'] = self.get_countries('All')
        return common

    def get_hs_headings(self, chapter: str = None) -> List[Dict[str, str]]:
        """
        Return HS headings, optionally filtered by chapter.
        Uses Tier 2 if available (lazy-loaded), otherwise Tier 3.
        """
        self._ensure_tier2()

        if chapter and chapter != "All":
            query = f"""
                SELECT DISTINCT hs_heading, heading_name AS heading
                FROM tier2
                WHERE hs_chapter = '{chapter}' AND hs_heading IS NOT NULL
                ORDER BY hs_heading
            """
        else:
            query = """
                SELECT DISTINCT hs_heading, heading_name AS heading
                FROM tier2
                WHERE hs_heading IS NOT NULL
                ORDER BY hs_heading
            """
        return self.conn.execute(query).df().to_dict("records")

    def get_hs_commodities(
        self, chapter: str = None, heading: str = None, years: List[int] = None
    ) -> List[Dict[str, str]]:
        """
        Return HS commodity codes. Queries Tier 3 (per-year) if available.

        Args:
            chapter: 2-digit HS chapter code or None
            heading: 4-digit HS heading code or None
            years:   list of years to search (downloads only those year files)
        """
        # Ensure at least the first requested year is available
        if years:
            for yr in years:
                self._ensure_tier3_year(yr)
            self._register_tier3_view()
        elif self.tier3_dir.exists() and not list(self.tier3_dir.glob("trade_*.parquet")):
            # No year files downloaded yet — nothing to query
            return []

        view = "tier3"

        where_parts = []
        if chapter and chapter != "All":
            where_parts.append(f"hs_chapter = '{chapter}'")
        if heading and heading != "All":
            where_parts.append(f"hs_heading = '{heading}'")
        if years:
            year_list = ", ".join(str(y) for y in years)
            where_parts.append(f"year IN ({year_list})")

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"

        try:
            return self.conn.execute(f"""
                SELECT DISTINCT hs_code, commodity
                FROM {view}
                WHERE {where_clause} AND hs_code IS NOT NULL
                ORDER BY hs_code
                LIMIT 1000
            """).df().to_dict("records")
        except Exception:
            return []

    # ────────────────────────────────────────────────────────────────────────
    # WHERE clause builder
    # ────────────────────────────────────────────────────────────────────────

    def _build_where_clause(
        self, filters: Dict[str, Any], table_alias: str = ""
    ) -> List[str]:
        """
        Build WHERE clause components from filters.

        Args:
            filters:     Dictionary of filter values
            table_alias: Optional prefix (e.g. 't1.') for column references

        Returns:
            List of WHERE clause string fragments
        """
        where_parts = []
        
        # Date range
        if 'start_date' in filters:
            where_parts.append(f"date >= '{filters['start_date']}'")
        if 'end_date' in filters:
            where_parts.append(f"date <= '{filters['end_date']}'")
        
        # Trade type
        if filters.get('trade_type') != 'All' and 'trade_type' in filters:
            where_parts.append(f"trade_type = '{filters['trade_type']}'")
        
        # Province
        if filters.get('province') != 'All' and 'province' in filters:
            where_parts.append(f"province = '{filters['province']}'")
        
        # HS Chapter
        if filters.get('hs_chapter') != 'All' and 'hs_chapter' in filters:
            where_parts.append(f"hs_chapter = '{filters['hs_chapter']}'")
        
        # HS Heading
        if filters.get('hs_heading') != 'All' and 'hs_heading' in filters:
            where_parts.append(f"hs_heading = '{filters['hs_heading']}'")
        
        # HS Commodity (full 8-digit code)
        if filters.get('hs_commodity') != 'All' and 'hs_commodity' in filters:
            where_parts.append(f"hs_code = '{filters['hs_commodity']}'")
        
        # Destination Country
        if filters.get('destination') != 'All' and 'destination' in filters:
            where_parts.append(f"destination = '{filters['destination']}'")
        
        # Exclude USA filter (raw parquet value is "USA - United States of America")
        if filters.get('exclude_usa') == True:
            where_parts.append("destination NOT LIKE 'USA - %'")
        
        # Default to TRUE if no filters (return all data)
        if not where_parts:
            where_parts.append('1=1')
        
        return where_parts
    
    def query_concentration_metrics(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate market and product concentration risk using Tier 1.

        Returns:
            {market_concentration, product_concentration, dependency_matrix}
        """
        where_parts = self._build_where_clause(filters)
        where_clause = " AND ".join(where_parts)

        # Tier 1 clause — strip hs_heading (not a Tier 1 column)
        tier1_filters = {k: v for k, v in filters.items() if k != "hs_heading"}
        tier1_where_parts = self._build_where_clause(tier1_filters)
        where_clause = " AND ".join(tier1_where_parts)

        # Market concentration
        market_data = self.conn.execute(f"""
            WITH totals AS (
                SELECT destination, destination_name, SUM(value) AS value
                FROM tier1
                WHERE {where_clause}
                GROUP BY destination, destination_name
            ),
            grand AS (SELECT SUM(value) AS total FROM totals)
            SELECT
                destination,
                destination_name,
                value,
                ROUND(100.0 * value / grand.total, 2) AS pct
            FROM totals, grand
            ORDER BY value DESC
            LIMIT 10
        """).df().to_dict("records")

        top1_market = market_data[0]["pct"] if market_data else 0
        top3_market = sum(d["pct"] for d in market_data[:3])
        top5_market = sum(d["pct"] for d in market_data[:5])

        # Product concentration
        product_data = self.conn.execute(f"""
            WITH totals AS (
                SELECT hs_chapter, chapter_name, chapter_summary,
                       category, category_color, SUM(value) AS value
                FROM tier1
                WHERE {where_clause} AND hs_chapter IS NOT NULL
                GROUP BY hs_chapter, chapter_name, chapter_summary, category, category_color
            ),
            grand AS (SELECT SUM(value) AS total FROM totals)
            SELECT
                hs_chapter,
                chapter_name AS chapter,
                chapter_summary,
                category,
                category_color,
                value,
                ROUND(100.0 * value / grand.total, 2) AS pct
            FROM totals, grand
            ORDER BY value DESC
            LIMIT 10
        """).df().to_dict("records")

        top1_product = product_data[0]["pct"] if product_data else 0
        top3_product = sum(d["pct"] for d in product_data[:3])
        top5_product = sum(d["pct"] for d in product_data[:5])

        # Dependency matrix (Province × Country) — only without province filter
        dependency_matrix = []
        if filters.get("province", "All") in ("All", None, ""):
            dependency_matrix = self.conn.execute(f"""
                WITH pc AS (
                    SELECT province, destination, destination_name, SUM(value) AS value
                    FROM tier1
                    WHERE {where_clause} AND province != 'Canada (Total)'
                    GROUP BY province, destination, destination_name
                ),
                pt AS (
                    SELECT province, SUM(value) AS total
                    FROM pc
                    GROUP BY province
                )
                SELECT
                    pc.province,
                    pc.destination,
                    pc.destination_name,
                    pc.value,
                    ROUND(100.0 * pc.value / pt.total, 2) AS pct_of_province_total
                FROM pc
                JOIN pt ON pc.province = pt.province
                WHERE pc.value > 0
                ORDER BY pc.province, pc.value DESC
            """).df().to_dict("records")

        return {
            "market_concentration": {
                "top1_pct":    top1_market,
                "top3_pct":    top3_market,
                "top5_pct":    top5_market,
                "top_countries": market_data,
            },
            "product_concentration": {
                "top1_pct":    top1_product,
                "top3_pct":    top3_product,
                "top5_pct":    top5_product,
                "top_chapters": product_data,
            },
            "dependency_matrix": dependency_matrix,
        }

    # ────────────────────────────────────────────────────────────────────────
    # Sankey — Tier 1 routed
    # ────────────────────────────────────────────────────────────────────────

    def query_sankey_data(
        self, filters: Dict[str, Any], flow_type: str = "export"
    ) -> Dict[str, Any]:
        """
        Get Province → Country → Chapter flow data for Sankey diagram.

        Args:
            filters:   Standard filter dictionary
            flow_type: 'export' or 'import' (not used for routing — kept for API compat)

        Returns:
            {'flows': [{province, destination, destination_name,
                        hs_chapter, chapter_name, chapter_summary,
                        category, category_color, value}, …]}
        """
        where_parts = self._build_where_clause(filters)
        # Tier 1 clause — strip hs_heading (not a Tier 1 column)
        tier1_filters = {k: v for k, v in filters.items() if k != "hs_heading"}
        tier1_where_parts = self._build_where_clause(tier1_filters)
        where_clause = " AND ".join(tier1_where_parts)

        flows = self.conn.execute(f"""
            SELECT
                province,
                destination,
                destination_name,
                hs_chapter,
                chapter_name,
                chapter_summary,
                category,
                category_color,
                SUM(value) AS value
            FROM tier1
            WHERE {where_clause}
                AND province != 'Canada (Total)'
                AND hs_chapter IS NOT NULL
            GROUP BY province, destination, destination_name,
                     hs_chapter, chapter_name, chapter_summary,
                     category, category_color
            HAVING SUM(value) > 0
            ORDER BY value DESC
            LIMIT 200
        """).df()

        return {"flows": flows.to_dict("records")}

    # ────────────────────────────────────────────────────────────────────────
    # Province comparison — Tier 1 routed
    # ────────────────────────────────────────────────────────────────────────

    def query_province_comparison_metrics(
        self, filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Get per-province summary metrics (ignores province filter).

        Returns:
            [{province, total_value, num_countries, num_chapters,
              top_destination, top_destination_name, top_chapter}, …]
        """
        filters_copy = {**filters, "province": "All"}
        where_parts = self._build_where_clause(filters_copy)
        # Tier 1 clause — strip hs_heading (not a Tier 1 column)
        tier1_filters = {k: v for k, v in filters_copy.items() if k != "hs_heading"}
        tier1_where_parts = self._build_where_clause(tier1_filters)
        where_clause = " AND ".join(tier1_where_parts)

        return self.conn.execute(f"""
            WITH stats AS (
                SELECT
                    province,
                    SUM(value)                  AS total_value,
                    COUNT(DISTINCT destination)  AS num_countries,
                    COUNT(DISTINCT hs_chapter)   AS num_chapters
                FROM tier1
                WHERE {where_clause} AND province != 'Canada (Total)'
                GROUP BY province
            ),
            top_dest AS (
                SELECT
                    province,
                    destination,
                    destination_name,
                    ROW_NUMBER() OVER (PARTITION BY province ORDER BY SUM(value) DESC) AS rn
                FROM tier1
                WHERE {where_clause} AND province != 'Canada (Total)'
                GROUP BY province, destination, destination_name
            ),
            top_ch AS (
                SELECT
                    province,
                    hs_chapter || ' - ' || chapter_name AS top_chapter,
                    ROW_NUMBER() OVER (PARTITION BY province ORDER BY SUM(value) DESC) AS rn
                FROM tier1
                WHERE {where_clause}
                    AND province != 'Canada (Total)'
                    AND hs_chapter IS NOT NULL
                GROUP BY province, hs_chapter, chapter_name
            )
            SELECT
                s.province,
                s.total_value,
                s.num_countries,
                s.num_chapters,
                td.destination       AS top_destination,
                td.destination_name  AS top_destination_name,
                tc.top_chapter
            FROM stats s
            LEFT JOIN top_dest td ON s.province = td.province AND td.rn = 1
            LEFT JOIN top_ch   tc ON s.province = tc.province AND tc.rn  = 1
            ORDER BY s.total_value DESC
        """).df().to_dict("records")

    # ────────────────────────────────────────────────────────────────────────
    # Utility
    # ────────────────────────────────────────────────────────────────────────

    def load_tier_metadata(self) -> Optional[Dict[str, Any]]:
        """Load tier_metadata.json if present."""
        if self.metadata_file.exists():
            with open(self.metadata_file, encoding="utf-8") as f:
                return json.load(f)
        return None

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self.conn:
            self.conn.close()


# ---------------------------------------------------------------------------
# CLI smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import time

    print("=" * 70)
    print("TESTING DUCKDB DATABASE WRAPPER (tiered)")
    print("=" * 70 + "\n")

    db = TradeDatabase()
    print(f"Tier 1 present: {db.has_tier1()}")
    print(f"Tier 2 present: {db.has_tier2()}")
    print()

    print("📋 Filter options …")
    t = time.time()
    opts = db.get_common_options()
    print(f"   Chapters:    {len(opts['chapters'])}")
    print(f"   Date range:  {opts['date_range']['min_date']} → {opts['date_range']['max_date']}")
    print(f"   Trade types: {opts['trade_types']}")
    print(f"   ⏱  {time.time()-t:.3f}s\n")

    print("📋 Countries …")
    t = time.time()
    countries = db.get_countries()
    print(f"   {len(countries)} destinations")
    sample = list(countries.items())[:5]
    for display, raw in sample:
        print(f"   {display!r:40} ← {raw!r}")
    print(f"   ⏱  {time.time()-t:.3f}s\n")

    print("📊 Dashboard stats (all data) …")
    t = time.time()
    filters = {
        "start_date": "2023-01-01",
        "end_date":   "2025-12-31",
        "trade_type": "All",
        "province":   "All",
        "hs_chapter": "All",
    }
    res = db.query_dashboard_stats(filters)
    print(f"   total_value:   ${res['kpi']['total_value']:,.0f} CAD")
    print(f"   total_records: {res['kpi']['total_records']:,}")
    print(f"   time_series:   {len(res['time_series'])} points")
    print(f"   top dest:      {res['top_destinations'][0]['destination_name'] if res['top_destinations'] else 'n/a'}")
    print(f"   ⏱  {time.time()-t:.3f}s\n")

    db.close()
    print("=" * 70)
    print("✅ ALL TESTS PASSED")
    print("=" * 70)
