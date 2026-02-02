"""
Canadian Trade Dashboard - Streamlit Application

Interactive dashboard for analyzing Canadian export trade data.
Built with Streamlit + DuckDB for fast, responsive analytics.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from database import TradeDatabase
from datetime import datetime
from hs_summaries import HS_CHAPTER_SUMMARIES, get_chapter_summary, get_category

# Page configuration
st.set_page_config(
    page_title="Canadian Trade Dashboard",
    page_icon="🇨🇦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-title {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FF0000;
        padding-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# Initialize database (cached for performance)
@st.cache_resource
def init_database():
    """Initialize and cache database connection."""
    return TradeDatabase()

# Load options (Common)
@st.cache_data(ttl=3600)
def load_common_options():
    db = init_database()
    return db.get_common_options()

# Load dynamic options (Dependent on Trade Type)
@st.cache_data(ttl=600)
def load_dynamic_options(trade_type):
    db = init_database()
    return {
        'provinces': db.get_provinces(trade_type),
        'countries': db.get_countries(trade_type)
    }

# Query data (cached with shorter TTL for updates)
@st.cache_data(ttl=600)
def query_data(start_date, end_date, trade_type, province, destination, hs_chapter, hs_heading, hs_commodity, exclude_usa):
    """Query and cache dashboard data."""
    import time
    start_time = time.time()
    
    db = init_database()
    filters = {
        'start_date': start_date,
        'end_date': end_date,
        'trade_type': trade_type,
        'province': province,
        'destination': destination,
        'hs_chapter': hs_chapter,
        'hs_heading': hs_heading,
        'hs_commodity': hs_commodity,
        'exclude_usa': exclude_usa
    }
    result = db.query_dashboard_stats(filters)
    
    query_time = time.time() - start_time
    return result, query_time

# Initialize database and load common options
try:
    db = init_database()
    common_options = load_common_options()
except Exception as e:
    st.error(f"❌ Error initializing database: {e}")
    st.info("""
    **Possible solutions:**
    1. The data file may still be downloading. Please wait and refresh the page.
    2. Check that the GitHub Release exists: https://github.com/pocketpiston/Canada-Trade-Dashboard/releases/tag/v1.0.0
    3. If the problem persists, check the Streamlit Cloud logs.
    """)
    st.stop()

# ============================================================================
# SIDEBAR FILTERS
# ============================================================================

st.sidebar.markdown("## 🔍 Filters")

# Trade Type
trade_type = st.sidebar.selectbox(
    "Trade Type",
    ['All'] + common_options['trade_types'],
    index=1 if 'Export' in common_options['trade_types'] else 0
)

# Load context-aware options
dynamic_opts = load_dynamic_options(trade_type)

# Date Range - Month and Year dropdowns
st.sidebar.markdown("### 📅 Date Range")
min_year = int(common_options['date_range']['min_year'])
max_year = int(common_options['date_range']['max_year'])

# Start Date
st.sidebar.markdown("**Start Date**")
col1, col2 = st.sidebar.columns(2)
with col1:
    start_month = st.selectbox(
        "Month",
        options=list(range(1, 13)),
        format_func=lambda x: pd.to_datetime(f"2000-{x:02d}-01").strftime("%B"),
        index=0,
        key="start_month"
    )
with col2:
    start_year = st.selectbox(
        "Year",
        options=list(range(min_year, max_year + 1)),
        index=0,
        key="start_year"
    )

# End Date
st.sidebar.markdown("**End Date**")
col3, col4 = st.sidebar.columns(2)
with col4:
    end_year = st.selectbox(
        "Year",
        options=list(range(min_year, max_year + 1)),
        index=len(list(range(min_year, max_year + 1))) - 1,
        key="end_year"
    )

# Determine max month based on selected end year
# 2025 data only available through October
max_end_month = 10 if end_year == 2025 else 12

with col3:
    end_month = st.selectbox(
        "Month",
        options=list(range(1, max_end_month + 1)),
        format_func=lambda x: pd.to_datetime(f"2000-{x:02d}-01").strftime("%B"),
        index=min(11, max_end_month - 1),  # Default to December or October for 2025
        key="end_month_select",
        help="⚠️ 2025 data only available through October" if end_year == 2025 else None
    )

# Convert to date strings
start_date = f"{start_year}-{start_month:02d}-01"
end_date = f"{end_year}-{end_month:02d}-{pd.Period(f'{end_year}-{end_month}').days_in_month}"

# Dynamic Labels based on Trade Type
if trade_type == 'Import':
    prov_label = "📍 Destination Province"
    dest_label = "🌍 Source Country (Origin)"
elif trade_type == 'Export':
    prov_label = "📍 Source Province"
    dest_label = "🌍 Destination Country"
else:
    prov_label = "📍 Province"
    dest_label = "🌍 Trading Partner"

# Province Filter
province = st.sidebar.selectbox(
    prov_label,
    ['All'] + dynamic_opts['provinces']
)

# Destination Country Filter
st.sidebar.markdown(f"### {dest_label.split()[0]} Location") # Header
destination = st.sidebar.selectbox(
    dest_label,
    ['All'] + dynamic_opts['countries']
)

# HS Chapter Filter
st.sidebar.markdown("### 📦 HS Code Hierarchy")

# Chapter (2-digit)
chapter_options = ['All'] + [
    f"{c['hs_chapter']} - {get_chapter_summary(c['hs_chapter'])}"
    for c in common_options['chapters']
]
selected_chapter_display = st.sidebar.selectbox(
    "Chapter (2-digit)",
    chapter_options
)
# Extract chapter code
if selected_chapter_display == 'All':
    hs_chapter = 'All'
else:
    hs_chapter = selected_chapter_display.split(' - ')[0]

# Heading (4-digit) - depends on Chapter
if hs_chapter != 'All':
    headings = db.get_hs_headings(hs_chapter)
    # Show full descriptions without truncation
    heading_options = ['All'] + [
        f"{h['hs_heading']} - {h['heading'].split(' - ')[1] if ' - ' in h['heading'] else h['heading']}"
        for h in headings[:50]  # Limit to 50 for performance
    ]
    selected_heading_display = st.sidebar.selectbox(
        "Heading (4-digit)",
        heading_options
    )
    
    if selected_heading_display == 'All':
        hs_heading = 'All'
    else:
        hs_heading = selected_heading_display.split(' - ')[0]
else:
    hs_heading = 'All'
    st.sidebar.info("Select a Chapter to filter by Heading")

# Commodity (8-digit) - depends on Heading
if hs_heading != 'All':
    commodities = db.get_hs_commodities(hs_chapter, hs_heading)
    commodity_options = ['All'] + [f"{c['hs_code']} - {c['commodity'][:30]}..." 
                                    if len(c['commodity']) > 30 else f"{c['hs_code']} - {c['commodity']}"
                                    for c in commodities[:50]]  # Limit to 50 for performance
    selected_commodity_display = st.sidebar.selectbox(
        "Commodity (8-digit)",
        commodity_options
    )
    
    if selected_commodity_display == 'All':
        hs_commodity = 'All'
    else:
        hs_commodity = selected_commodity_display.split(' - ')[0]
else:
    hs_commodity = 'All'
    if hs_chapter != 'All':
        st.sidebar.info("Select a Heading to filter by Commodity")

# Unit Scale Selector - Moved here to group all HS filters together
st.sidebar.markdown("### 📏 Display Units")
unit_mode = st.sidebar.selectbox(
    "Scale",
    ["Auto (SI)", "Trillions ($T)", "Billions ($B)", "Millions ($M)", "Thousands ($k)", "Raw ($)"],
    index=0,  # Default to Auto
    help="Auto mode automatically selects the best scale based on data magnitude"
)

# Helper to determine scale factor and labels
scale_factor = 1.0
unit_suffix = ""
axis_format = ".2s" # Default SI

if unit_mode == "Auto (SI)":
    # Smart auto-scaling based on data magnitude
    # We'll determine this after querying data
    pass  # Will be set later based on max value
elif unit_mode == "Trillions ($T)":
    scale_factor = 1e12
    unit_suffix = "T"
    axis_format = ",.2f"  # 2 decimal places for trillions
elif unit_mode == "Billions ($B)":
    scale_factor = 1e9
    unit_suffix = "B"
    axis_format = ",.0f"
elif unit_mode == "Millions ($M)":
    scale_factor = 1e6
    unit_suffix = "M"
    axis_format = ",.0f"
elif unit_mode == "Thousands ($k)":
    scale_factor = 1e3
    unit_suffix = "k"
    axis_format = ",.0f"
elif unit_mode == "Raw ($)":
    scale_factor = 1.0
    unit_suffix = ""
    axis_format = ",.2f"

# Axis Label suffix
currency_label = f"CAD ({unit_suffix})" if unit_suffix else "CAD"

# Visualization Theme Selector
st.sidebar.markdown("### 🎨 Visualization Theme")

# Theme configurations
THEMES = {
    'Classic': {
        'destinations': 'Reds',
        'provinces': 'Blues', 
        'breakdown': 'Teal',
        'chapters_treemap': 'RdYlGn',
        'headings_treemap': 'Blues',
        'chapters_bar': 'Greens',
        'treemap_text_color': 'black',
        'treemap_text_size': 14
    },
    'High Contrast': {
        'destinations': 'Oranges',
        'provinces': 'Purples',
        'breakdown': 'YlOrBr',
        'chapters_treemap': [[0, '#f7fbff'], [0.5, '#6baed6'], [1, '#08306b']],
        'headings_treemap': [[0, '#fff5eb'], [0.5, '#fd8d3c'], [1, '#7f2704']],
        'chapters_bar': 'BuGn',
        'treemap_text_color': 'black',
        'treemap_text_size': 15
    },
    'Dark Mode': {
        'destinations': [[0, '#2d1b2e'], [0.5, '#8b4789'], [1, '#e8b4e5']],
        'provinces': [[0, '#1a2332'], [0.5, '#4a7ba7'], [1, '#a8d5ff']],
        'breakdown': [[0, '#1e3a2f'], [0.5, '#4a9b7f'], [1, '#a8e6cf']],
        'chapters_treemap': [[0, '#1a1a2e'], [0.5, '#6a5acd'], [1, '#dda0dd']],
        'headings_treemap': [[0, '#0f2027'], [0.5, '#2c5364'], [1, '#7dd3c0']],
        'chapters_bar': [[0, '#1e3d2f'], [0.5, '#5a9367'], [1, '#b8e6c9']],
        'treemap_text_color': 'white',
        'treemap_text_size': 14
    },
    'Colorblind Friendly': {
        'destinations': 'Viridis',
        'provinces': 'Plasma',
        'breakdown': 'Cividis',
        'chapters_treemap': 'Viridis',
        'headings_treemap': 'Plasma',
        'chapters_bar': 'Cividis',
        'treemap_text_color': 'white',
        'treemap_text_size': 14
    }
}

selected_theme = st.sidebar.selectbox(
    "Color Scheme",
    list(THEMES.keys()),
    index=0,
    help="Choose a color theme for all visualizations. High Contrast and Colorblind Friendly options improve accessibility."
)

theme = THEMES[selected_theme]

# USA Exclusion Filter
st.sidebar.markdown("---")
st.sidebar.markdown("### 🇺🇸 USA Filter")
exclude_usa = st.sidebar.checkbox(
    "Exclude USA from analysis",
    value=False,
    help="When checked, all USA-related trade will be excluded from calculations and visualizations"
)

# Data refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Performance Metrics
st.sidebar.markdown("---")
st.sidebar.markdown("### ⚡ Performance")

# Get database stats
try:
    import duckdb
    from pathlib import Path
    parquet_file = Path('data/processed/trade_records.parquet')
    if parquet_file.exists():
        temp_conn = duckdb.connect(':memory:')
        total_records = temp_conn.execute(f"SELECT COUNT(*) FROM '{parquet_file}'").fetchone()[0]
        temp_conn.close()
        st.sidebar.caption(f"📊 Database: {total_records:,} records")
    else:
        st.sidebar.caption("📊 Database: Loading...")
except:
    st.sidebar.caption("📊 Database: N/A")

# ============================================================================
# MAIN DASHBOARD
# ============================================================================

# Header
st.markdown('<div class="main-title">🇨🇦 Canadian Trade Dashboard</div>', unsafe_allow_html=True)

# Filter summary
trading_partner_label = "Origin" if trade_type == 'Import' else "Destination"

filter_parts = [f"**{trade_type}**", f"{start_date} to {end_date}", f"Province: **{province}**"]
if destination != 'All':
    # Shorten destination name for display
    dest_short = destination.split(' - ')[-1] if ' - ' in destination else destination
    filter_parts.append(f"{trading_partner_label}: **{dest_short}**")
if exclude_usa:
    filter_parts.append("🚫 **USA Excluded**")
if hs_chapter != 'All':
    chapter_summary = get_chapter_summary(hs_chapter)
    filter_parts.append(f"Chapter: **{hs_chapter} - {chapter_summary}**")
if hs_heading != 'All':
    filter_parts.append(f"Heading: **{hs_heading}**")
if hs_commodity != 'All':
    filter_parts.append(f"Commodity: **{hs_commodity}**")
filter_summary = " | ".join(filter_parts)
st.markdown(filter_summary)
st.markdown("---")

# Query data with loading indicator
with st.spinner("Loading data..."):
    data, query_time = query_data(
        start_date,
        end_date,
        trade_type,
        province,
        destination,
        hs_chapter,
        hs_heading,
        hs_commodity,
        exclude_usa
    )

    # --- SMART AUTO-SCALING LOGIC ---
    # If "Auto (SI)" is selected, determine the best scale based on the filtered data.
    # We use the total trade value (KPI) as the heuristic.
    if unit_mode == "Auto (SI)":
        total_val = data['kpi']['total_value']
        # Financial formatting logic (T, B, M, k)
        if total_val >= 1e12:
            scale_factor = 1e12
            unit_suffix = "T"
            axis_format = ",.2f"  # 2 decimals for trillions
        elif total_val >= 1e9:
            scale_factor = 1e9
            unit_suffix = "B"
            axis_format = ",.1f"  # 1 decimal for billions
        elif total_val >= 1e6:
            scale_factor = 1e6
            unit_suffix = "M"
            axis_format = ",.0f"
        elif total_val >= 1e3:
            scale_factor = 1e3
            unit_suffix = "k"
            axis_format = ",.0f"
        else:
            scale_factor = 1.0
            unit_suffix = ""
            axis_format = ",.0f"
            
        # Update label
        currency_label = f"CAD ({unit_suffix})" if unit_suffix else "CAD"

# Display query time in sidebar
st.sidebar.caption(f"⚡ Last query: {query_time:.3f}s")

# Helper function to auto-format KPI values
def format_kpi_value(value):
    """Format large values with appropriate suffix for readability"""
    if value >= 1e12:
        return f"${value/1e12:,.2f}T CAD"
    elif value >= 1e9:
        return f"${value/1e9:,.2f}B CAD"
    elif value >= 1e6:
        return f"${value/1e6:,.1f}M CAD"
    else:
        return f"${value:,.0f} CAD"

# ============================================================================
# KPI CARDS
# ============================================================================

kpi_col1, kpi_col2, kpi_col3 = st.columns(3)

with kpi_col1:
    st.metric(
        label="💰 Total Trade Value",
        value=format_kpi_value(data['kpi']['total_value']),
        help="Total trade value with auto-formatted units"
    )

with kpi_col2:
    total_records = data['kpi']['total_records']
    st.metric(
        label="📊 Total Records",
        value=f"{total_records:,.2f}",
        help="Number of trade transactions"
    )

with kpi_col3:
    st.metric(
        label="📈 Avg Monthly Value",
        value=format_kpi_value(data['kpi']['avg_monthly']),
        help="Average monthly trade value with auto-formatted units"
    )

st.markdown("---")

# ============================================================================
# TIME SERIES CHART
# ============================================================================

st.subheader("📈 Trade Value Over Time")

time_series_df = pd.DataFrame(data['time_series'])
if not time_series_df.empty:
    time_series_df['month'] = pd.to_datetime(time_series_df['month'])
    
    time_series_df['value'] = time_series_df['value'] / scale_factor
    
    fig_time = px.line(
        time_series_df,
        x='month',
        y='value',
        title=f"{trade_type} Value by Month",
        labels={'value': f'Trade Value ({currency_label})', 'month': 'Month'}
    )
    fig_time.update_traces(line_color='#FF0000', line_width=3)
    fig_time.update_layout(
        height=400,
        hovermode='x unified',
        plot_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray', tickformat=axis_format)
    )
    st.plotly_chart(fig_time, use_container_width=True)
else:
    st.info("No time series data available for the selected filters.")

st.markdown("---")

# ============================================================================
# TOP CHARTS (2 COLUMNS)
# ============================================================================

chart_col1, chart_col2 = st.columns(2)

# Top Destinations (or Origins)
with chart_col1:
    partner_label = "Origins" if trade_type == 'Import' else "Destinations"
    single_partner_label = "Origin" if trade_type == 'Import' else "Destination"
    
    st.subheader(f"🌍 Top {trade_type} {partner_label}")
    
    dest_df = pd.DataFrame(data['top_destinations'])
    if not dest_df.empty:
        # Shorten destination names for display
        dest_df['short_name'] = dest_df['destination'].apply(
            lambda x: x.split(' - ')[-1][:30] if ' - ' in x else x[:30]
        )
        
        dest_df['value'] = dest_df['value'] / scale_factor
        
        fig_dest = px.bar(
            dest_df,
            x='value',
            y='short_name',
            orientation='h',
            labels={'value': f'Trade Value ({currency_label})', 'short_name': single_partner_label},
            color='value',
            color_continuous_scale=theme['destinations']
        )
        fig_dest.update_layout(
            height=400,
            showlegend=False,
            plot_bgcolor='white',
            xaxis=dict(showgrid=True, gridcolor='lightgray', tickformat=axis_format),
            yaxis=dict(showgrid=False)
        )
        st.plotly_chart(fig_dest, use_container_width=True)
    else:
        st.info("No destination data available.")

# Top Provinces
with chart_col2:
    st.subheader("🏙️ Top Provinces")
    
    prov_df = pd.DataFrame(data['top_provinces'])
    if not prov_df.empty:
        prov_df['value'] = prov_df['value'] / scale_factor
        fig_prov = px.bar(
            prov_df,
            x='value',
            y='province',
            orientation='h',
            labels={'value': f'Trade Value ({currency_label})', 'province': 'Province'},
            color='value',
            color_continuous_scale=theme['provinces']
        )
        fig_prov.update_layout(
            height=400,
            showlegend=False,
            plot_bgcolor='white',
            xaxis=dict(showgrid=True, gridcolor='lightgray', tickformat=axis_format),
            yaxis=dict(showgrid=False)
        )
        st.plotly_chart(fig_prov, use_container_width=True)
    else:
        st.info("No province data available.")

st.markdown("---")

# ============================================================================
# DYNAMIC BREAKDOWN CHART (Adapts to Filters)
# ============================================================================

st.subheader("📊 Dynamic Breakdown Analysis")

# Determine what to show based on active filters
if province != 'All' and destination == 'All':
    # Province selected: Show top destinations/origins for this province
    action = "Import Origins" if trade_type == 'Import' else "Export Destinations"
    direction = "to" if trade_type == 'Import' else "from" # Semantics: Imports to Prov, Exports from Prov
    # Wait: Imports come TO the province (from Origin). Exports go FROM province (to Dest).
    # Title: "Top Import Origins for {province}" / "Top Export Destinations from {province}"
    
    title_text = f"**Top {action} for {province}**"
    st.markdown(title_text)
    
    breakdown_data = data['top_destinations']
    x_label = 'Origin' if trade_type == 'Import' else 'Destination'
    chart_color = 'Oranges'
    
elif destination != 'All' and province == 'All':
    # Destination/Origin selected: Show top provinces trading
    dest_short = destination.split(' - ')[-1] if ' - ' in destination else destination
    
    if trade_type == 'Import':
        # "Top Provinces Importing from {Origin}"
        st.markdown(f"**Top Provinces Importing from {dest_short}**")
    else:
        st.markdown(f"**Top Provinces Exporting to {dest_short}**")
        
    breakdown_data = data['top_provinces']
    x_label = 'Province'
    chart_color = 'Purples'
    
elif province != 'All' and destination != 'All':
    # Both selected: Show monthly trend
    dest_short = destination.split(' - ')[-1] if ' - ' in destination else destination
    arrow = "←" if trade_type == 'Import' else "→"
    st.markdown(f"**Monthly Trend: {province} {arrow} {dest_short}**")
    
    # Create monthly breakdown
    time_series_df = pd.DataFrame(data['time_series'])
    if not time_series_df.empty:
        time_series_df['month'] = pd.to_datetime(time_series_df['month'])
        time_series_df['month_label'] = time_series_df['month'].dt.strftime('%b %Y')
        
        fig_monthly = px.bar(
            time_series_df,
            x='month_label',
            y='value',
            labels={'value': f'Trade Value ({currency_label})', 'month_label': 'Month'},
            color='value',
            color_continuous_scale=theme['breakdown']
        )
        fig_monthly.update_layout(
            height=400,
            showlegend=False,
            plot_bgcolor='white',
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor='lightgray', tickformat=axis_format)
        )
        st.plotly_chart(fig_monthly, use_container_width=True)
    else:
        st.info("No monthly data available for this filter combination.")
    
    breakdown_data = None  # Skip the standard bar chart below
    
elif hs_chapter != 'All' and destination == 'All' and province == 'All':
    # HS Chapter selected: Show top destinations/origins for this chapter
    partner_label = "Origins" if trade_type == 'Import' else "Destinations"
    chapter_summary = get_chapter_summary(hs_chapter)
    st.markdown(f"**Top {partner_label} for HS Chapter {hs_chapter} - {chapter_summary}**")
    breakdown_data = data['top_destinations']
    x_label = 'Origin' if trade_type == 'Import' else 'Destination'
    
else:
    # No specific filters or HS-only: Show top destinations overall
    partner_label = "Import Origins" if trade_type == 'Import' else "Export Destinations"
    st.markdown(f"**Top {partner_label} (Overall)**")
    breakdown_data = data['top_destinations']
    x_label = 'Origin' if trade_type == 'Import' else 'Destination'

# Show bar chart if we have breakdown data
if breakdown_data and len(breakdown_data) > 0:
    breakdown_df = pd.DataFrame(breakdown_data)
    
    # Scale values according to selected units
    breakdown_df['value'] = breakdown_df['value'] / scale_factor
    
    # Shorten labels if needed
    if 'destination' in breakdown_df.columns:
        breakdown_df['label'] = breakdown_df['destination'].apply(
            lambda x: x.split(' - ')[-1][:25] if ' - ' in x else x[:25]
        )
        y_col = 'label'
    elif 'province' in breakdown_df.columns:
        breakdown_df['label'] = breakdown_df['province']
        y_col = 'label'
    else:
        breakdown_df['label'] = breakdown_df.iloc[:, 0]  # First column
        y_col = 'label'
    
    fig_breakdown = px.bar(
        breakdown_df,
        x='value',
        y=y_col,
        orientation='h',
        labels={'value': f'Trade Value ({currency_label})', y_col: x_label},
        color='value',
        color_continuous_scale=theme['breakdown']
    )
    fig_breakdown.update_layout(
        height=450,
        showlegend=False,
        plot_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray', tickformat=axis_format),
        yaxis=dict(showgrid=False)
    )
    st.plotly_chart(fig_breakdown, use_container_width=True)
elif breakdown_data is not None:
    st.info("No data available for the selected filters.")

st.markdown("---")

# ============================================================================
# HS CHAPTERS TREEMAP
# ============================================================================

# Create title based on province filter
if province != 'All':
    treemap_title = f"📊 HS Chapters Breakdown - {province}"
else:
    treemap_title = "📊 HS Chapters Breakdown - All Provinces"

st.subheader(treemap_title)

# Prepare treemap data
hs_treemap_df = pd.DataFrame(data['top_hs_codes'])

if not hs_treemap_df.empty:
    # Extract chapter code and name separately
    hs_treemap_df['chapter_code'] = hs_treemap_df['code']
    hs_treemap_df['chapter_name'] = hs_treemap_df['description'].apply(
        lambda x: x.split(' - ')[1] if ' - ' in x else x
    )
    
    # Add summary from dictionary
    hs_treemap_df['summary'] = hs_treemap_df['chapter_code'].map(HS_CHAPTER_SUMMARIES)
    
    # Calculate percentage of total
    total_value = hs_treemap_df['value'].sum()
    hs_treemap_df['percentage'] = (hs_treemap_df['value'] / total_value * 100)
    
    # Create display label with code + summary (2 lines)
    hs_treemap_df['display_label'] = hs_treemap_df.apply(
        lambda row: f"{row['chapter_code']}\n{row['summary']}", axis=1
    )
    
    # Scale values for display
    hs_treemap_df['scaled_value'] = hs_treemap_df['value'] / scale_factor
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["📊 Visual", "📋 Table"])
    
    with tab1:
        # Create treemap with code-only labels

        fig_treemap = px.treemap(
            hs_treemap_df,
            path=['display_label'],
            values='value',  # Use raw values for sizing
            color='scaled_value',  # Use scaled values for color
            color_continuous_scale=theme['chapters_treemap'],
            custom_data=['chapter_code', 'summary', 'chapter_name', 'value', 'percentage']
        )
        
        # Update with rich hover tooltips
        fig_treemap.update_traces(
            textposition='middle center',
            textfont_size=theme['treemap_text_size'],
            textfont_color=theme['treemap_text_color'],
            textfont_family='Arial Black',
            marker=dict(
                line=dict(width=2, color='white'),
                pad=dict(t=5, l=5, r=5, b=5)
            ),
            hovertemplate='<b>Chapter %{customdata[0]}: %{customdata[1]}</b><br>' +
                          '<i>%{customdata[2]}</i><br>' +
                          'Value: $%{customdata[3]:,.0f}<br>' +
                          'Share: %{customdata[4]:.1f}%<br>' +
                          '<extra></extra>'
        )
        
        fig_treemap.update_layout(
            height=600,
            margin=dict(t=10, l=10, r=10, b=10),
            coloraxis_colorbar=dict(
                tickformat='.2f',  # Show 2 decimal places in legend
                title=f"Trade Value ({currency_label})"
            )
        )
        
        st.plotly_chart(fig_treemap, use_container_width=True)
        
        # Add helpful caption
        st.caption("💡 Hover over blocks to see full chapter names and values. Larger blocks = higher trade value.")
    
    with tab2:
        # Interactive table view with summary
        display_df = hs_treemap_df[['chapter_code', 'summary', 'chapter_name', 'value', 'percentage']].copy()
        display_df['value'] = display_df['value'].apply(lambda x: f"${x:,.0f}")
        display_df['percentage'] = display_df['percentage'].apply(lambda x: f"{x:.1f}%")
        display_df.columns = ['Code', 'Summary', 'Full Description', 'Trade Value', 'Share']
        
        st.dataframe(
            display_df,
            use_container_width=True,
            height=500,
            hide_index=True
        )

else:
    st.info("No HS chapter data available for the selected filters.")

st.markdown("---")

# ============================================================================
# HS HEADINGS TREEMAP (appears when Chapter is selected)
# ============================================================================

if hs_chapter != 'All' and data.get('top_hs_headings') and len(data['top_hs_headings']) > 0:
    # Create title based on province and chapter
    chapter_summary = get_chapter_summary(hs_chapter)
    if province != 'All':
        heading_title = f"📊 HS Headings Breakdown - Chapter {hs_chapter}: {chapter_summary} - {province}"
    else:
        heading_title = f"📊 HS Headings Breakdown - Chapter {hs_chapter}: {chapter_summary}"
    
    st.subheader(heading_title)
    
    # Prepare treemap data
    hs_headings_df = pd.DataFrame(data['top_hs_headings'])
    
    # Extract heading code and name separately
    hs_headings_df['heading_code'] = hs_headings_df['code']
    hs_headings_df['heading_name'] = hs_headings_df['description'].apply(
        lambda x: x.split(' - ')[1] if ' - ' in x else x
    )
    
    # Calculate percentage of total
    total_value = hs_headings_df['value'].sum()
    hs_headings_df['percentage'] = (hs_headings_df['value'] / total_value * 100)
    
    # Create display labels with code + shortened description for readability
    hs_headings_df['display_label'] = hs_headings_df.apply(
        lambda row: f"{row['heading_code']}\n{row['heading_name'][:40]}..." 
        if len(row['heading_name']) > 40 
        else f"{row['heading_code']}\n{row['heading_name']}",
        axis=1
    )
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["📊 Visual", "📋 Table"])
    
    with tab1:
        # Create treemap with code-only labels

        fig_heading_treemap = px.treemap(
            hs_headings_df,
            path=['display_label'],
            values='value',
            color='value',
            color_continuous_scale=theme['headings_treemap'],
            custom_data=['heading_code', 'heading_name', 'value', 'percentage']
    )
    
    fig_heading_treemap.update_traces(
        textposition='middle center',
        textfont_size=theme['treemap_text_size'],
        textfont_color=theme['treemap_text_color'],
        textfont_family='Arial',
        marker=dict(
            line=dict(width=2, color='white'),
            pad=dict(t=5, l=5, r=5, b=5)
        ),
        hovertemplate='<b>Heading %{customdata[0]}</b><br>' +
                      '<i>%{customdata[1]}</i><br>' +
                      'Value: $%{customdata[2]:,.0f}<br>' +
                      'Share: %{customdata[3]:.1f}%<br>' +
                      '<extra></extra>'
    )
    
    fig_heading_treemap.update_layout(
        height=450,
        margin=dict(t=10, l=10, r=10, b=10)
    )
    
    st.plotly_chart(fig_heading_treemap, use_container_width=True)
    
    # Add explanation
    if province != 'All':
        st.caption(f"💡 Showing relative trade value of HS headings (4-digit) within Chapter {hs_chapter} for **{province}**. Larger blocks = higher trade value.")
    else:
        st.caption(f"💡 Showing relative trade value of HS headings (4-digit) within Chapter {hs_chapter} across all provinces. Larger blocks = higher trade value.")

st.markdown("---")

# ============================================================================
# TOP HS CODES
# ============================================================================

st.subheader("📦 Top HS Chapters by Value")

hs_df = pd.DataFrame(data['top_hs_codes'])
if not hs_df.empty:
    # Create label with code and summary
    hs_df['chapter_code'] = hs_df['code']
    hs_df['summary'] = hs_df['chapter_code'].map(HS_CHAPTER_SUMMARIES)
    hs_df['label'] = hs_df.apply(
        lambda row: f"{row['chapter_code']} - {row['summary']}", axis=1
    )
    
    hs_df['value'] = hs_df['value'] / scale_factor
    fig_hs = px.bar(
        hs_df,
        x='value',
        y='label',
        orientation='h',
        labels={'value': f'Trade Value ({currency_label})', 'label': 'HS Chapter'},
        color='value',
        color_continuous_scale=theme['chapters_bar']
    )
    fig_hs.update_layout(
        height=500,
        showlegend=False,
        plot_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray', tickformat=axis_format),
        yaxis=dict(showgrid=False)
    )
    st.plotly_chart(fig_hs, use_container_width=True)
else:
    st.info("No HS code data available.")

# ============================================================================
# DATA TABLE (EXPANDABLE)
# ============================================================================

with st.expander("📊 View Top HS Codes Data Table"):
    if not hs_df.empty:
        # Format values for display
        display_df = hs_df[['code', 'description', 'value']].copy()
        display_df['value'] = display_df['value'].apply(lambda x: f"${x:,.2f}")
        display_df.columns = ['HS Code', 'Description', 'Trade Value (CAD)']
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("No data to display.")

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.caption("""
**Data Source:** Statistics Canada Table 12-10-0011-01 | 
**Dashboard:** Built with Streamlit + DuckDB | 
**Last Updated:** 31 Jan 2026
""")

# Sidebar info
st.sidebar.markdown("---")
st.sidebar.markdown("### ℹ️ About")
st.sidebar.info(f"""
**Records:** {total_records:,.0f}  
**Date Range:** {start_date} to {end_date}  
**Provinces:** {len(dynamic_opts['provinces'])}  
**HS Chapters:** {len(common_options['chapters'])}  
**Query Time:** < 100ms ⚡
""")
