# Data Processing Scripts

This directory contains scripts for extracting and processing Canadian trade data from Statistics Canada's API.

## Overview

The data pipeline consists of two main steps:

1. **Extract** - Download raw trade data from Statistics Canada API
2. **Convert** - Process and convert raw JSON files to optimized Parquet format

## Prerequisites

```bash
pip install -r requirements.txt
```

**Required packages:**
- `requests` - API calls
- `pandas` - Data processing
- `pyarrow` - Parquet file handling

---

## Pipeline Steps

### Step 1: Extract Raw Data

**Script:** `extract_all_trade.py`

Downloads trade data from Statistics Canada's International Merchandise Trade API.

**Data Source:**
- API: `https://www150.statcan.gc.ca/t1/cimt/rest/getReport/`
- Documentation: [Statistics Canada CIMT API](https://www.statcan.gc.ca/en/developers/cimt/user-guide)

**What it does:**
- Fetches monthly trade data for all provinces and territories
- Downloads both Export and Import data
- Organizes data by Year → HS Chapter → Month
- Saves raw JSON files to `data/raw/` (exports) and `data/raw_imports/` (imports)

**Configuration:**
```python
YEARS = [2008, 2009, 2010]  # Years to extract
MONTHS = range(1, 13)        # All months
FLOWS = [0, 1]               # 0=Export, 1=Import
```

**Usage:**
```bash
python scripts/extract_all_trade.py
```

**Output:**
```
data/raw/{year}/{chapter}/{month:02d}_{province_id}.json
data/raw_imports/{year}/{chapter}/{month:02d}_{province_id}.json
```

**Note:** This script uses concurrent requests (10 workers) and includes retry logic for reliability.

---

### Step 2: Convert to Parquet

**Script:** `convert_to_parquet.py`

Processes all raw JSON files and converts them to optimized Parquet format for fast querying with DuckDB.

**What it does:**
- Scans all JSON files in `data/raw/` and `data/raw_imports/`
- Enriches data with HS code descriptions
- Normalizes schema and data types
- Filters duplicate/invalid records
- Saves to compressed Parquet files

**Usage:**
```bash
python scripts/convert_to_parquet.py
```

**Output:**
```
data/processed/trade_records.parquet  # Main trade data (~350 MB)
data/processed/hs_lookup.parquet      # HS code hierarchy
data/processed/metadata.json          # Processing metadata
```

**Processing Features:**
- **Deduplication** - Removes Canada-level US data (handled by provinces)
- **Enrichment** - Adds HS chapter/heading descriptions
- **Optimization** - Snappy compression for fast queries
- **Validation** - Type checking and date parsing

---

## Data Schema

### Trade Records (`trade_records.parquet`)

| Column | Type | Description |
|--------|------|-------------|
| `date` | datetime | Trade date (YYYY-MM-DD) |
| `year` | int | Year |
| `month` | int | Month (1-12) |
| `trade_type` | string | "Export" or "Import" |
| `province` | string | Province/territory name |
| `province_code` | int | Province ID |
| `destination` | string | Trading partner country |
| `destination_iso` | string | ISO country code |
| `destination_state` | string | US state (if applicable) |
| `hs_code` | string | 8-digit HS commodity code |
| `hs_chapter` | string | 2-digit HS chapter |
| `hs_heading` | string | 4-digit HS heading |
| `chapter` | string | Chapter description |
| `heading` | string | Heading description |
| `commodity` | string | Commodity description |
| `value` | float | Trade value (CAD) |
| `quantity` | float | Quantity traded |
| `uom` | string | Unit of measure |

### HS Lookup (`hs_lookup.parquet`)

| Column | Type | Description |
|--------|------|-------------|
| `hs_level` | string | "chapter", "heading", or "commodity" |
| `hs_code` | string | HS code |
| `hs_chapter` | string | Parent chapter |
| `hs_heading` | string | Parent heading (if applicable) |
| `description` | string | Full description |
| `uom` | string | Unit of measure (commodities only) |

---

## Reference Data

The scripts use reference data from `data/reference/`:

- `chapters.json` - HS chapter codes and descriptions
- `provinces.json` - Province/territory codes and names

These files are included in the repository and required for data enrichment.

---

## Full Pipeline Example

```bash
# 1. Extract raw data (may take several hours)
python scripts/extract_all_trade.py

# 2. Convert to Parquet (processes all JSON files)
python scripts/convert_to_parquet.py

# 3. Verify output
ls -lh data/processed/
```

**Expected output:**
```
trade_records.parquet  (~350 MB for 3 years of data)
hs_lookup.parquet      (~1 MB)
metadata.json          (~2 KB)
```

---

## Performance Notes

- **Extraction:** ~10-30 minutes per year (depends on network speed)
- **Conversion:** ~5-10 minutes for 3 years of data
- **Storage:** ~120 MB per year of trade data (compressed)

---

## Troubleshooting

### "No records found to process"
- Ensure `data/raw/` contains JSON files
- Check that `extract_all_trade.py` completed successfully

### "Failed to load reference data"
- Verify `data/reference/chapters.json` and `provinces.json` exist
- These files should be included in the repository

### API rate limiting
- The extraction script includes retry logic
- If you encounter persistent errors, reduce `max_workers` in `extract_all_trade.py`

---

## Data Updates

To update with new data:

1. Modify `YEARS` in `extract_all_trade.py`
2. Run extraction for new years only
3. Re-run conversion to include new data

The conversion script will process all JSON files, including newly added ones.

---

## License

Data is sourced from Statistics Canada and subject to their [Open Government License](https://open.canada.ca/en/open-government-licence-canada).
