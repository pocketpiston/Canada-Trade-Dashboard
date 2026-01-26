# Canadian Trade Dashboard - Streamlit + DuckDB

Welcome to the Canadian Trade Dashboard! 🇨🇦

## Quick Start

### 1. Run the Dashboard

```bash
cd dashboard_streamlit
streamlit run app.py
```

### 2. Use the Filters

**Sidebar Controls:**
- **Trade Type:** Export, Import, or All
- **Date Range:** Select start and end dates
- **Province:** Filter by Canadian province or view All
- **HS Chapter:** Filter by harmonized system chapter (products)

### 3. Explore the Data

**Main Dashboard Features:**
- **KPI Cards:** Total value, record count, average monthly value
- **Time Series Chart:** Trade value trends over time
- **Top Destinations:** See where Canadian goods are exported
- **Top Provinces:** Which provinces export the most
- **Top HS Chapters:** Most valuable product categories

## Data Summary

- **Records:** 1.5 million trade transactions
- **Date Range:** 2023-2025
- **HS Chapters:** All 98 chapters (complete product coverage)
- **Total Value:** $2.0 trillion CAD

## Performance

- Dashboard load time: < 2 seconds
- Filter updates: < 1 second
- Query time: < 100ms
- All data cached for 10 minutes

## Files

```
dashboard_streamlit/
├── app.py               # Main Streamlit application
├── database.py          # DuckDB wrapper for queries
├── requirements.txt     # Python dependencies
└── README.md           
```


## Support

Questions? Check:
- Streamlit docs: https://docs.streamlit.io
- DuckDB docs: https://duckdb.org/docs/

---
