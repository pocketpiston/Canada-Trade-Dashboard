# Data Directory

This directory contains the trade data files used by the dashboard.

## Quick Start

The dashboard will automatically download the required data file (`trade_records.parquet`) from GitHub Releases on first run. No manual setup required!

## Data Files

### Automatically Downloaded
- `trade_records.parquet` (350 MB) - Main trade records database
  - Downloaded from GitHub Releases on first run
  - Contains all trade transactions with full details

### Included in Repository
- `reference/chapters.json` - HS chapter codes and descriptions
- `reference/provinces.json` - Province/territory codes and names

## Manual Data Generation

If you prefer to generate your own data or update with new years:

```bash
# 1. Extract raw data from Statistics Canada API
python scripts/extract_all_trade.py

# 2. Convert to Parquet format
python scripts/convert_to_parquet.py
```

This will create `trade_records.parquet` in the `processed/` folder.

See [../scripts/README.md](../scripts/README.md) for detailed instructions.

## Data Schema

The `trade_records.parquet` file contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `date` | datetime | Trade date (YYYY-MM-DD) |
| `year` | int | Year |
| `month` | int | Month (1-12) |
| `trade_type` | string | "Export" or "Import" |
| `province` | string | Province/territory name |
| `destination` | string | Trading partner country |
| `hs_code` | string | 8-digit HS commodity code |
| `hs_chapter` | string | 2-digit HS chapter |
| `chapter` | string | Chapter description |
| `value` | float | Trade value (CAD) |
| `quantity` | float | Quantity traded |

## Data Source

All data is sourced from Statistics Canada's International Merchandise Trade Database:
- API: https://www150.statcan.gc.ca/t1/cimt/
- License: [Open Government License - Canada](https://open.canada.ca/en/open-government-licence-canada)

## File Sizes

- `trade_records.parquet`: ~350 MB (compressed, 3 years of data)
- `reference/chapters.json`: ~5 KB
- `reference/provinces.json`: ~1 KB

## Troubleshooting

### "Data file not found" error
- The dashboard should auto-download on first run
- If download fails, check your internet connection
- Alternatively, run the processing scripts manually (see above)

### "Download failed" error
- Ensure you have internet connectivity
- Check that GitHub Releases is accessible
- Try running the processing scripts locally as an alternative
