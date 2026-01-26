# Canadian Trade Dashboard - Streamlit + DuckDB

Welcome to the Canadian Trade Dashboard! 🇨🇦

## Quick Start

### 1. Run the Dashboard

```bash
cd dashboard_streamlit
streamlit run app.py
```

The dashboard will open in your browser at http://localhost:8501

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
└── README.md           # This file
```

## Deployment to Streamlit Cloud

### Option 1: Free Streamlit Cloud Hosting

1. **Push to GitHub:**
   ```bash
   git add dashboard_streamlit/
   git commit -m "Add Streamlit dashboard"
   git push
   ```

2. **Deploy:**
   - Go to https://share.streamlit.io/
   - Click "New app"
   - Select your repository
   - Set main file path: `dashboard_streamlit/app.py`
   - Click "Deploy"

3. **Data Files:**
   - If Parquet files < 100MB: Commit to Git
   - If larger: Use Git LFS or upload via Streamlit Cloud

### Option 2: Local Development

Run locally with:
```bash
streamlit run app.py
```

## Customization

### Change Theme Colors

Edit the CSS in `app.py` (line 19):
```python
st.markdown("""
<style>
    .main-title {
        color: #YOUR_COLOR;
    }
</style>
""", unsafe_allow_html=True)
```

### Add More Filters

Add new filters in the sidebar section (line 76):
```python
new_filter = st.sidebar.selectbox("Filter Name", options)
```

### Modify Charts

All charts use Plotly. Customize in the chart sections:
```python
fig = px.bar(...)
fig.update_layout(height=500, ...)  # Customize here
```

## Troubleshooting

### Dashboard won't start?
```bash
# Install dependencies
pip install -r requirements.txt
```

### Data not loading?
Check that Parquet files exist:
```bash
ls -lh ../data/processed/
```

### Slow queries?
Clear cache and restart:
```python
# In app.py, add refresh button (already included)
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()
```

## Next Steps

1. ✅ Dashboard is running locally
2. → Test all filters and charts
3. → Deploy to Streamlit Cloud
4. → Share URL with stakeholders
5. → (Optional) Extract more years of data for larger dataset

## Support

Questions? Check:
- Streamlit docs: https://docs.streamlit.io
- DuckDB docs: https://duckdb.org/docs/
- Implementation plan: `../.gemini/antigravity/brain/.../implementation_plan.md`

---

**Enjoy your lightning-fast trade dashboard!** ⚡🇨🇦
