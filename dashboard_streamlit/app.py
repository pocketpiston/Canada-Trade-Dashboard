"""
Canadian Trade Dashboard - Streamlit Application

Interactive dashboard for analyzing Canadian export trade data.
Built with Streamlit + DuckDB for fast, responsive analytics.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import re
from database import TradeDatabase
from datetime import datetime
from hs_summaries import HS_CHAPTER_SUMMARIES, get_chapter_summary, get_category, get_category_name, CATEGORY_MAP

# Page configuration
st.set_page_config(
    page_title="Canadian Trade Dashboard",
    page_icon="🇨🇦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS – Modern publication-quality styling
st.markdown("""
<style>
    /* Import Inter from Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Global font */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }

    /* Dashboard title */
    .main-title {
        font-size: 2.2rem;
        font-weight: 700;
        letter-spacing: -0.02em;
        color: #1a1a2e;
        padding-bottom: 0.3rem;
    }
    
    /* KPI Cards – refined typography */
    [data-testid="stMetricValue"] {
        font-size: 34px;
        font-weight: 700;
        font-family: 'Inter', sans-serif;
        letter-spacing: -0.01em;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 13px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #666;
    }
    
    /* Section headers – publication style */
    h2 {
        font-weight: 700 !important;
        letter-spacing: -0.01em !important;
        margin-top: 1rem !important;
        margin-bottom: 0.5rem !important;
    }
    
    h3 {
        font-weight: 600 !important;
        letter-spacing: -0.005em !important;
        margin-top: 0.6rem !important;
        margin-bottom: 0.4rem !important;
    }
    
    /* Layout spacing */
    .block-container {
        padding-top: 0.5rem;
        padding-bottom: 0.5rem;
    }
    
    /* Captions – subtle, informational */
    [data-testid="stCaptionContainer"] {
        font-size: 12px;
        color: #888;
        letter-spacing: 0.01em;
    }
    
    /* Compact section spacing */
    .element-container {
        margin-bottom: 0.3rem;
    }
    
    /* Expanders */
    [data-testid="stExpander"] {
        margin-bottom: 0.5rem;
        border: 1px solid #e8e8e8;
        border-radius: 6px;
    }
    
    /* Tab styling – modern underline */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0px;
        border-bottom: 2px solid #e8e8e8;
    }
    
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px;
        font-weight: 500;
        font-size: 14px;
        letter-spacing: 0.01em;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        padding-top: 1rem;
        padding-left: 1rem;
        padding-right: 1rem;
    }
    
    [data-testid="stSidebar"] .block-container {
        padding-top: 0.5rem;
        padding-bottom: 0.5rem;
    }
    
    [data-testid="stSidebar"] [data-testid="stExpander"] {
        margin-bottom: 0.1rem;
        margin-top: 0.1rem;
    }
    
    /* Dataframe */
    [data-testid="stDataFrame"] {
        padding-top: 0.2rem;
        padding-bottom: 0.2rem;
    }
    
    /* Separators – subtle */
    hr {
        margin-top: 0.8rem !important;
        margin-bottom: 0.8rem !important;
        border: none !important;
        border-top: 1px solid #e0e0e0 !important;
    }
    
    /* Paragraph spacing */
    [data-testid="stMarkdownContainer"] p {
        margin-bottom: 0.3rem;
    }
    
    /* Metrics */
    [data-testid="stMetric"] {
        padding: 0.3rem 0;
    }
    
    /* Expander inner padding */
    [data-testid="stExpander"] > div > div {
        padding-top: 0.5rem;
        padding-bottom: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# DATA PROVISIONING - Check data BEFORE initializing database
# This runs in the Streamlit render cycle so the app boots first
# (passing Streamlit Cloud's timeout check), then handles download.
# ============================================================================
@st.cache_resource
def init_database():
    """Initialize and cache database connection."""
    return TradeDatabase()

db = init_database()

if not db.has_data():
    st.title("🇨🇦 Canadian Trade Dashboard")
    st.info("📥 First-time setup: downloading trade data...")
    db._download_trade_data()
    # After download, clear ALL caches so database reinitializes with real data
    st.cache_resource.clear()
    st.cache_data.clear()
    st.rerun()


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
db = init_database()
common_options = load_common_options()

# ============================================================================
# SIDEBAR FILTERS
# ============================================================================

st.sidebar.markdown("## 🔍 Filters")

# ============================================================================
# TIME PERIOD GROUP
# ============================================================================
with st.sidebar.expander("📅 Time Period", expanded=True):
    # Trade Type
    trade_type = st.selectbox(
        "Trade Type",
        ['All'] + common_options['trade_types'],
        index=1 if 'Export' in common_options['trade_types'] else 0,
        key="trade_type_select"
    )
    
    # Date Range - Month and Year dropdowns
    min_year = int(common_options['date_range']['min_year'])
    max_year = int(common_options['date_range']['max_year'])
    
    # Start Date
    st.markdown("**Start Date**")
    col1, col2 = st.columns(2)
    with col1:
        start_month = st.selectbox(
            "Month",
            options=list(range(1, 13)),
            format_func=lambda x: pd.to_datetime(f"2000-{x:02d}-01").strftime("%B"),
            index=0,
            key="start_month",
            label_visibility="collapsed"
        )
    with col2:
        start_year = st.selectbox(
            "Year",
            options=list(range(min_year, max_year + 1)),
            index=0,
            key="start_year",
            label_visibility="collapsed"
        )
    
    # End Date
    st.markdown("**End Date**")
    col3, col4 = st.columns(2)
    with col4:
        end_year = st.selectbox(
            "Year",
            options=list(range(min_year, max_year + 1)),
            index=len(list(range(min_year, max_year + 1))) - 1,
            key="end_year",
            label_visibility="collapsed"
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
            help="⚠️ 2025 data only available through October" if end_year == 2025 else None,
            label_visibility="collapsed"
        )

# Convert to date strings
start_date = f"{start_year}-{start_month:02d}-01"
end_date = f"{end_year}-{end_month:02d}-{pd.Period(f'{end_year}-{end_month}').days_in_month}"

# Load context-aware options
dynamic_opts = load_dynamic_options(trade_type)

# ============================================================================
# GEOGRAPHY GROUP
# ============================================================================
with st.sidebar.expander("🌍 Geography", expanded=True):
    # Dynamic Labels based on Trade Type
    if trade_type == 'Import':
        prov_label = "Destination Province"
        dest_label = "Source Country (Origin)"
    elif trade_type == 'Export':
        prov_label = "Source Province"
        dest_label = "Destination Country"
    else:
        prov_label = "Province"
        dest_label = "Trading Partner"
    
    # Province Filter
    province = st.selectbox(
        prov_label,
        ['All'] + dynamic_opts['provinces'],
        key="province_select"
    )
    
    # Destination Country Filter
    destination = st.selectbox(
        dest_label,
        ['All'] + dynamic_opts['countries'],
        key="destination_select"
    )
    
    # USA Exclusion Filter
    exclude_usa = st.checkbox(
        "Exclude USA from analysis",
        value=False,
        help="When checked, all USA-related trade will be excluded from calculations and visualizations",
        key="exclude_usa_checkbox"
    )

# ============================================================================
# HS PRODUCT CLASSIFICATION GROUP
# ============================================================================
with st.sidebar.expander("📦 HS Product Classification", expanded=True):
    # Chapter (2-digit)
    chapter_options = ['All'] + [
        f"{c['hs_chapter']} - {get_chapter_summary(c['hs_chapter'])}"
        for c in common_options['chapters']
    ]
    selected_chapter_display = st.selectbox(
        "Chapter (2-digit)",
        chapter_options,
        key="chapter_select"
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
        selected_heading_display = st.selectbox(
            "Heading (4-digit)",
            heading_options,
            key="heading_select"
        )
        
        if selected_heading_display == 'All':
            hs_heading = 'All'
        else:
            hs_heading = selected_heading_display.split(' - ')[0]
    else:
        hs_heading = 'All'
        st.info("💡 Select a Chapter to filter by Heading")
    
    # Commodity (8-digit) - depends on Heading
    if hs_heading != 'All':
        commodities = db.get_hs_commodities(hs_chapter, hs_heading)
        commodity_options = ['All'] + [f"{c['hs_code']} - {c['commodity'][:30]}..." 
                                        if len(c['commodity']) > 30 else f"{c['hs_code']} - {c['commodity']}"
                                        for c in commodities[:50]]  # Limit to 50 for performance
        selected_commodity_display = st.selectbox(
            "Commodity (8-digit)",
            commodity_options,
            key="commodity_select"
        )
        
        if selected_commodity_display == 'All':
            hs_commodity = 'All'
        else:
            hs_commodity = selected_commodity_display.split(' - ')[0]
    else:
        hs_commodity = 'All'
        if hs_chapter != 'All':
            st.info("💡 Select a Heading to filter by Commodity")

# ============================================================================
# DISPLAY OPTIONS GROUP
# ============================================================================
with st.sidebar.expander("📊 Display Options", expanded=True):
    # Unit Scale Selector
    unit_mode = st.selectbox(
        "Display Units",
        ["Auto (SI)", "Trillions ($T)", "Billions ($B)", "Millions ($M)", "Thousands ($k)", "Raw ($)"],
        index=0,  # Default to Auto
        help="Auto mode automatically selects the best scale based on data magnitude",
        key="unit_mode_select"
    )
    
    # =========================================================================
    # VISUALIZATION THEMES – Inspired by FT / The Economist
    # =========================================================================
    
    # Custom color sequences for publication-quality charts
    _FT_SEQUENTIAL = ['#fff1e5', '#f2c4a0', '#e69a6e', '#cc6d3d', '#990f3d', '#660a29']
    _FT_CATEGORICAL = ['#0D7680', '#0F5499', '#990F3D', '#CC0000', '#593380', '#B45B2C',
                        '#1D7D74', '#5C4614', '#AD5E9A', '#2E6B8A']
    
    _ECON_SEQUENTIAL = ['#f0f4f7', '#bdd7e7', '#6baed6', '#006BA2', '#004570', '#002940']
    _ECON_CATEGORICAL = ['#006BA2', '#DB444B', '#3EBCD2', '#379A8B', '#EBB434',
                          '#B4BA39', '#9A607F', '#D1B07C', '#758D99', '#5C4A3D']
    
    _DARK_SEQUENTIAL = ['#1a1a2e', '#2d2d5e', '#4a3f8a', '#7b5ea7', '#D4A574', '#F4D9B0']
    _DARK_CATEGORICAL = ['#D4A574', '#2EC4B6', '#E07A5F', '#81B29A', '#F2CC8F',
                          '#6C9BCF', '#C98BBD', '#8ECAE6', '#A8DADC', '#FFB4A2']
    
    THEMES = {
        'Ivory': {
            # Sequential scales (for continuous/heatmap data)
            'sequential': _FT_SEQUENTIAL,
            'sequential_alt': ['#fff1e5', '#fad5b5', '#e8a77e', '#cc6d3d', '#0D7680', '#074D52'],
            'breakdown': ['#fff1e5', '#f2c4a0', '#cc6d3d', '#990F3D', '#660929'],
            'treemap_scale': ['#E8D5C4', '#CC9966', '#B07040', '#8B4513', '#660A29'],
            'headings_scale': ['#D4E8E5', '#80B8B0', '#4D9990', '#0D7680', '#074D52'],
            'heatmap': ['#FFF1E5', '#F2C4A0', '#CC6D3D', '#990F3D', '#660929'],
            # Categorical colors (for discrete/pie data)
            'categorical': _FT_CATEGORICAL,
            # Chart styling
            'line_color': '#0D7680',
            'line_color_alt': '#990F3D',
            'plot_bg': '#FFF8F2',
            'paper_bg': '#FFF8F2',
            'grid_color': '#E8DDD4',
            'font_color': '#33302E',
            'font_family': 'Inter, Georgia, serif',
            'treemap_text_color': '#2D2319',
            'treemap_text_size': 13,
            'template': 'plotly_white',
        },
        'Blue': {
            # Sequential scales
            'sequential': _ECON_SEQUENTIAL,
            'sequential_alt': ['#f0f4f7', '#a8d8a8', '#379A8B', '#225E54', '#143B35'],
            'breakdown': ['#fef2e4', '#f8d5a3', '#EBB434', '#C4891E', '#8B5A0B'],
            'treemap_scale': ['#D4E4F0', '#8BB8D6', '#4A90B8', '#006BA2', '#004570'],
            'headings_scale': ['#D4EDEB', '#7BC8C0', '#3EBCD2', '#1A8A9E', '#0D5E6A'],
            'heatmap': ['#F0F4F7', '#BDD7E7', '#6BAED6', '#DB444B', '#8B1A1A'],
            # Categorical colors
            'categorical': _ECON_CATEGORICAL,
            # Chart styling
            'line_color': '#006BA2',
            'line_color_alt': '#DB444B',
            'plot_bg': '#FFFFFF',
            'paper_bg': '#FFFFFF',
            'grid_color': '#E6E6E6',
            'font_color': '#333333',
            'font_family': 'Inter, -apple-system, sans-serif',
            'treemap_text_color': '#1a1a1a',
            'treemap_text_size': 13,
            'template': 'plotly_white',
        },
        'Dark': {
            # Sequential scales
            'sequential': _DARK_SEQUENTIAL,
            'sequential_alt': ['#1a1a2e', '#2C4A4E', '#3D7A6E', '#2EC4B6', '#7EDDD4'],
            'breakdown': ['#1a1a2e', '#5E3D2E', '#A66845', '#D4A574', '#F4D9B0'],
            'treemap_scale': ['#2A2A4A', '#4A3F8A', '#7B5EA7', '#D4A574', '#F4D9B0'],
            'headings_scale': ['#1A2E2E', '#2A5050', '#2EC4B6', '#7EDDD4', '#C0F0EA'],
            'heatmap': ['#1a1a2e', '#3D2D5E', '#7B5EA7', '#E07A5F', '#FFB4A2'],
            # Categorical colors
            'categorical': _DARK_CATEGORICAL,
            # Chart styling
            'line_color': '#D4A574',
            'line_color_alt': '#2EC4B6',
            'plot_bg': '#16213E',
            'paper_bg': '#1a1a2e',
            'grid_color': '#2A2A5A',
            'font_color': '#C8C8D8',
            'font_family': 'Inter, -apple-system, sans-serif',
            'treemap_text_color': '#F0E6D8',
            'treemap_text_size': 13,
            'template': 'plotly_dark',
        }
    }
    
    selected_theme = st.selectbox(
        "Visualization Theme",
        list(THEMES.keys()),
        index=1,  # Default to Blue
        help="Choose a professional color theme for all visualizations.",
        key="theme_select"
    )
    
    theme = THEMES[selected_theme]

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
# KPI CARDS - Enhanced with larger fonts and better visual prominence
# ============================================================================

kpi_col1, kpi_col2, kpi_col3 = st.columns(3)

with kpi_col1:
    st.metric(
        label="Total Trade Value",
        value=format_kpi_value(data['kpi']['total_value']),
        help="Total trade value with auto-formatted units"
    )

with kpi_col2:
    total_records = data['kpi']['total_records']
    st.metric(
        label="Total Records",
        value=f"{total_records:,.0f}",
        help="Number of trade transactions"
    )

with kpi_col3:
    st.metric(
        label="Avg Monthly Value",
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
    fig_time.update_traces(line_color=theme['line_color'], line_width=2.5)
    fig_time.update_layout(
        height=400,
        hovermode='x unified',
        template=theme['template'],
        plot_bgcolor=theme['plot_bg'],
        paper_bgcolor=theme['paper_bg'],
        font=dict(family=theme['font_family'], color=theme['font_color']),
        xaxis=dict(showgrid=True, gridcolor=theme['grid_color'], zeroline=False),
        yaxis=dict(showgrid=True, gridcolor=theme['grid_color'], tickformat=axis_format, zeroline=False)
    )
    st.plotly_chart(fig_time, width='stretch')
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
            color_continuous_scale=theme['sequential']
        )
        fig_dest.update_layout(
            height=400,
            showlegend=False,
            template=theme['template'],
            plot_bgcolor=theme['plot_bg'],
            paper_bgcolor=theme['paper_bg'],
            font=dict(family=theme['font_family'], color=theme['font_color']),
            xaxis=dict(showgrid=True, gridcolor=theme['grid_color'], tickformat=axis_format),
            yaxis=dict(showgrid=False)
        )
        st.plotly_chart(fig_dest, width='stretch')
    else:
        st.info("No destination data available.")

# Top Provinces
with chart_col2:
    st.subheader("🏙️ Provincial Breakdown")
    
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
            color_continuous_scale=theme['sequential_alt']
        )
        fig_prov.update_layout(
            height=400,
            showlegend=False,
            template=theme['template'],
            plot_bgcolor=theme['plot_bg'],
            paper_bgcolor=theme['paper_bg'],
            font=dict(family=theme['font_family'], color=theme['font_color']),
            xaxis=dict(showgrid=True, gridcolor=theme['grid_color'], tickformat=axis_format),
            yaxis=dict(showgrid=False)
        )
        st.plotly_chart(fig_prov, width='stretch')
    else:
        st.info("No province data available.")

st.markdown("---")

# ============================================================================
# TOP HS CHAPTERS (OVERVIEW - moved up for broad context)
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
        color_continuous_scale=theme['sequential']
    )
    fig_hs.update_layout(
        height=500,
        showlegend=False,
        template=theme['template'],
        plot_bgcolor=theme['plot_bg'],
        paper_bgcolor=theme['paper_bg'],
        font=dict(family=theme['font_family'], color=theme['font_color']),
        xaxis=dict(showgrid=True, gridcolor=theme['grid_color'], tickformat=axis_format),
        yaxis=dict(showgrid=False)
    )
    st.plotly_chart(fig_hs, width='stretch')
else:
    st.info("No HS code data available.")

with st.expander("📊 View Top HS Codes Data Table"):
    if not hs_df.empty:
        # Format values for display
        display_df = hs_df[['code', 'description', 'value']].copy()
        display_df['value'] = display_df['value'].apply(lambda x: f"${x:,.2f}")
        display_df.columns = ['HS Code', 'Description', 'Trade Value (CAD)']
        st.dataframe(display_df, width='stretch', hide_index=True)
    else:
        st.info("No data to display.")

st.markdown("---")

# ============================================================================
# TIER 2: DEEP DIVE (Tabbed Interface)
# ============================================================================

st.subheader("🔍 Detailed Analysis")

deep_tab1, deep_tab2 = st.tabs(["📦 Product Analysis", "⚠️ Concentration Risk"])

# ==========================================================================
# TAB 1: PRODUCT ANALYSIS (Dynamic Breakdown + Product Hierarchy + HS Headings)
# ==========================================================================
with deep_tab1:

    # --- Dynamic Breakdown Analysis ---
    st.markdown("### 📊 Dynamic Breakdown Analysis")
    
    # Determine what to show based on active filters
    if province != 'All' and destination == 'All':
        action = "Import Origins" if trade_type == 'Import' else "Export Destinations"
        title_text = f"**Top {action} for {province}**"
        st.markdown(title_text)
        breakdown_data = data['top_destinations']
        x_label = 'Origin' if trade_type == 'Import' else 'Destination'
        chart_color = 'Oranges'
        
    elif destination != 'All' and province == 'All':
        dest_short = destination.split(' - ')[-1] if ' - ' in destination else destination
        if trade_type == 'Import':
            st.markdown(f"**Top Provinces Importing from {dest_short}**")
        else:
            st.markdown(f"**Top Provinces Exporting to {dest_short}**")
        breakdown_data = data['top_provinces']
        x_label = 'Province'
        chart_color = 'Purples'
        
    elif province != 'All' and destination != 'All':
        dest_short = destination.split(' - ')[-1] if ' - ' in destination else destination
        arrow = "←" if trade_type == 'Import' else "→"
        st.markdown(f"**Monthly Trend: {province} {arrow} {dest_short}**")
        
        time_series_df2 = pd.DataFrame(data['time_series'])
        if not time_series_df2.empty:
            time_series_df2['month'] = pd.to_datetime(time_series_df2['month'])
            time_series_df2['month_label'] = time_series_df2['month'].dt.strftime('%b %Y')
            
            fig_monthly = px.bar(
                time_series_df2,
                x='month_label',
                y='value',
                labels={'value': f'Trade Value ({currency_label})', 'month_label': 'Month'},
                color='value',
                color_continuous_scale=theme['breakdown']
            )
            fig_monthly.update_layout(
                height=400,
                showlegend=False,
                template=theme['template'],
                plot_bgcolor=theme['plot_bg'],
                paper_bgcolor=theme['paper_bg'],
                font=dict(family=theme['font_family'], color=theme['font_color']),
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor=theme['grid_color'], tickformat=axis_format)
            )
            st.plotly_chart(fig_monthly, width='stretch')
        else:
            st.info("No monthly data available for this filter combination.")
        
        breakdown_data = None
        
    elif hs_chapter != 'All' and destination == 'All' and province == 'All':
        partner_label = "Origins" if trade_type == 'Import' else "Destinations"
        chapter_summary = get_chapter_summary(hs_chapter)
        st.markdown(f"**Top {partner_label} for HS Chapter {hs_chapter} - {chapter_summary}**")
        breakdown_data = data['top_destinations']
        x_label = 'Origin' if trade_type == 'Import' else 'Destination'
        
    else:
        partner_label = "Import Origins" if trade_type == 'Import' else "Export Destinations"
        st.markdown(f"**Top {partner_label} (Overall)**")
        breakdown_data = data['top_destinations']
        x_label = 'Origin' if trade_type == 'Import' else 'Destination'
    
    # Show bar chart if we have breakdown data
    if breakdown_data and len(breakdown_data) > 0:
        breakdown_df = pd.DataFrame(breakdown_data)
        breakdown_df['value'] = breakdown_df['value'] / scale_factor
        
        if 'destination' in breakdown_df.columns:
            breakdown_df['label'] = breakdown_df['destination'].apply(
                lambda x: x.split(' - ')[-1][:25] if ' - ' in x else x[:25]
            )
            y_col = 'label'
        elif 'province' in breakdown_df.columns:
            breakdown_df['label'] = breakdown_df['province']
            y_col = 'label'
        else:
            breakdown_df['label'] = breakdown_df.iloc[:, 0]
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
            template=theme['template'],
            plot_bgcolor=theme['plot_bg'],
            paper_bgcolor=theme['paper_bg'],
            font=dict(family=theme['font_family'], color=theme['font_color']),
            xaxis=dict(showgrid=True, gridcolor=theme['grid_color'], tickformat=axis_format),
            yaxis=dict(showgrid=False)
        )
        st.plotly_chart(fig_breakdown, width='stretch')
    elif breakdown_data is not None:
        st.info("No data available for the selected filters.")
    
    st.markdown("---")
    
    # --- Product Hierarchy Analysis ---
    # NAVIGATION & DRILL-DOWN HELPERS
    
    def get_clean_category(code):
        """Helper to get category name without emojis"""
        name = get_category_name(code)
        return re.sub(r'[^\x00-\x7F]+', '', name).strip()
    
    if hs_chapter != 'All':
        col1, col2 = st.columns([4, 1])
        with col1:
            st.info(f"📍 **Drilled down to:** Category: {get_clean_category(hs_chapter)} > Chapter {hs_chapter}")
        with col2:
            if st.button("⬅️ Back to All", help="Clear HS Chapter filter"):
                st.warning("Please select 'All' in the sidebar filter to go back.")
    
    # Create title based on province filter
    if province != 'All':
        hierarchy_title = f"Product Hierarchy Analysis - {province}"
    else:
        hierarchy_title = "Product Hierarchy Analysis - All Provinces"

    st.markdown(f"### 📦 {hierarchy_title}")


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
        
        # Map to categories (without emojis)
        hs_treemap_df['category'] = hs_treemap_df['chapter_code'].apply(get_clean_category)
        
        # Create discrete color map for categories
        clean_cat_map = {}
        for r, (name, color) in CATEGORY_MAP.items():
            clean_name = re.sub(r'[^\x00-\x7F]+', '', name).strip()
            clean_cat_map[clean_name] = color
        
        # Create display label with code + summary (2 lines)
        hs_treemap_df['display_label'] = hs_treemap_df.apply(
            lambda row: f"{row['chapter_code']}\n{row['summary']}", axis=1
        )
        
        # Scale values for display
        hs_treemap_df['scaled_value'] = hs_treemap_df['value'] / scale_factor
        
        # ============================================================================
        # TABBED INTERFACE: Treemap vs Sunburst (Phase 2)
        # ============================================================================
        viz_tab1, viz_tab2, viz_tab3 = st.tabs(["🗺️ Treemap View", "🌲 Sunburst View", "📋 Data Table"])
        
        # TAB 1: TREEMAP VIEW
        with viz_tab1:
            st.caption("**Industry perspective** with discrete colors by category")
            
            # Create treemap with code-only labels
            fig_treemap = px.treemap(
                hs_treemap_df,
                path=['category', 'display_label'],  # Two-level hierarchy
                values='value',  # Use raw values for sizing
                color='category',  # Category-based coloring
                color_discrete_map=clean_cat_map,
                custom_data=['chapter_code', 'summary', 'chapter_name', 'value', 'percentage']
            )
            
            fig_treemap.update_traces(
                textposition='middle center',
                textfont_size=theme['treemap_text_size'],
                textfont_color=theme['treemap_text_color'],
                textfont_family=theme['font_family'],
                marker=dict(
                    line=dict(width=1, color=theme['paper_bg']),
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
                showlegend=True,
                legend=dict(title="Industry Category", orientation="h", y=-0.2),
                template=theme['template'],
                paper_bgcolor=theme['paper_bg'],
                font=dict(family=theme['font_family'], color=theme['font_color'])
            )
            
            st.plotly_chart(fig_treemap, width='stretch')
            st.caption("💡 Hover over blocks to see full chapter names and values. Larger blocks = higher trade value.")
        
        # TAB 2: SUNBURST VIEW
        with viz_tab2:
            st.caption("**Radial hierarchy** with value-based continuous colors")

            # Need to define hs_headings_df here if hs_chapter != 'All' for the sunburst tab
            hs_headings_df = pd.DataFrame() # Initialize empty
            if hs_chapter != 'All' and data.get('top_hs_headings') and len(data['top_hs_headings']) > 0:
                hs_headings_df = pd.DataFrame(data['top_hs_headings'])
                # Extract heading code and name separately
                hs_headings_df['heading_code'] = hs_headings_df['code']
                hs_headings_df['heading_name'] = hs_headings_df['description'].apply(
                    lambda x: x.split(' - ')[1] if ' - ' in x else x
                )
                # Calculate percentage of total
                total_value_headings = hs_headings_df['value'].sum()
                hs_headings_df['percentage'] = (hs_headings_df['value'] / total_value_headings * 100)
                # Create display labels
                hs_headings_df['display_label'] = hs_headings_df.apply(
                    lambda row: f"{row['heading_code']}\n{row['heading_name'][:40]}..." 
                    if len(row['heading_name']) > 40 
                    else f"{row['heading_code']}\n{row['heading_name']}",
                    axis=1
                )
            
            if hs_chapter == 'All' or hs_headings_df.empty:
                # Category -> Chapter hierarchy
                fig_sunburst = px.sunburst(
                    hs_treemap_df,
                    path=['category', 'display_label'],
                    values='value',
                    color='scaled_value',  # Value-based coloring
                    color_continuous_scale=theme['treemap_scale'],
                    custom_data=['chapter_code', 'summary', 'chapter_name', 'value', 'percentage']
                )
                hovertemplate = (
                    '<b>Chapter %{customdata[0]}</b><br>' +
                    '<b>Summary:</b> %{customdata[1]}<br>' +
                    '<i>%{customdata[2]}</i><br><br>' +
                    '<b>Value:</b> $%{customdata[3]:,.0f}<br>' +
                    '<b>Share:</b> %{customdata[4]:.1f}%<br>' +
                    '<extra></extra>'
                )
            else:
                # Category -> Chapter -> Heading hierarchy
                sun_headings_df = hs_headings_df.copy()
                clean_cat = re.sub(r'[^\x00-\x7F]+', '', get_category_name(hs_chapter)).strip()
                sun_headings_df['category'] = clean_cat
                sun_headings_df['chapter_label'] = f"{hs_chapter}\n{chapter_summary}"
                
                fig_sunburst = px.sunburst(
                    sun_headings_df,
                    path=['category', 'chapter_label', 'display_label'],
                    values='value',
                    color='value',  # Value-based coloring
                    color_continuous_scale=theme['headings_scale'],
                    custom_data=['heading_code', 'heading_name', 'value', 'percentage']
                )
                hovertemplate = (
                    '<b>Heading %{customdata[0]}</b><br>' +
                    '<i>%{customdata[1]}</i><br><br>' +
                    '<b>Value:</b> $%{customdata[2]:,.0f}<br>' +
                    '<b>Share:</b> %{customdata[3]:.1f}%<br>' +
                    '<extra></extra>'
                )
        
            fig_sunburst.update_traces(
                textfont_size=theme['treemap_text_size'],
                textfont_color=theme['treemap_text_color'],
                textfont_family=theme['font_family'],
                hovertemplate=hovertemplate
            )
        
            fig_sunburst.update_layout(
                height=650,
                margin=dict(t=10, l=10, r=10, b=10),
                template=theme['template'],
                paper_bgcolor=theme['paper_bg'],
                font=dict(family=theme['font_family'], color=theme['font_color'])
            )
        
            st.plotly_chart(fig_sunburst, width='stretch')
            st.caption("💡 This radial chart shows the hierarchical proportions of trade. Click on categories or chapters to drill down/up.")
        
        # TAB 3: DATA TABLE
        with viz_tab3:
            # Interactive table view with summary
            display_df = hs_treemap_df[['chapter_code', 'summary', 'chapter_name', 'value', 'percentage']].copy()
            display_df['value'] = display_df['value'].apply(lambda x: f"${x:,.0f}")
            display_df['percentage'] = display_df['percentage'].apply(lambda x: f"{x:.1f}%")
            display_df.columns = ['Code', 'Summary', 'Full Description', 'Trade Value', 'Share']
            
            st.dataframe(
                display_df,
                width='stretch',
                hide_index=True,
                height=400
            )
    else:
        st.info("No HS chapter data available for the selected filters.")

    st.markdown("---")

    # ======================================================================
    # HS HEADINGS TREEMAP (appears when Chapter is selected)
    # ======================================================================

    if hs_chapter != 'All' and data.get('top_hs_headings') and len(data['top_hs_headings']) > 0:
        # Create title based on province and chapter
        chapter_summary = get_chapter_summary(hs_chapter)
        if province != 'All':
            heading_title = f"📊 HS Headings Breakdown - Chapter {hs_chapter}: {chapter_summary} - {province}"
        else:
            heading_title = f"📊 HS Headings Breakdown - Chapter {hs_chapter}: {chapter_summary}"
        
        st.markdown(f"### {heading_title}")
        
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
        
        # Apply scaling for consistent display across all visualizations
        hs_headings_df['scaled_value'] = hs_headings_df['value'] / scale_factor
        
        # Create tabs for different views
        heading_tab1, heading_tab2 = st.tabs(["📊 Visual", "📋 Table"])
        
        with heading_tab1:
            # Create treemap with code-only labels

            fig_heading_treemap = px.treemap(
                hs_headings_df,
                path=['display_label'],
                values='value',
                color='scaled_value',  # Use scaled values for color
                color_continuous_scale=theme['headings_scale'],
                custom_data=['heading_code', 'heading_name', 'scaled_value', 'percentage']
            )
        
            fig_heading_treemap.update_traces(
                textposition='middle center',
                textfont_size=theme['treemap_text_size'],
                textfont_color=theme['treemap_text_color'],
                textfont_family=theme['font_family'],
                marker=dict(
                    line=dict(width=1, color=theme['paper_bg']),
                    pad=dict(t=5, l=5, r=5, b=5)
                ),
                hovertemplate='<b>Heading %{customdata[0]}</b><br>' +
                              '<i>%{customdata[1]}</i><br>' +
                              f'Value: ${currency_label} %{{customdata[2]:,.2f}}<br>' +
                              'Share: %{customdata[3]:.1f}%<br>' +
                              '<extra></extra>'
            )
            
            fig_heading_treemap.update_layout(
                height=450,
                margin=dict(t=10, l=10, r=10, b=10),
                template=theme['template'],
                paper_bgcolor=theme['paper_bg'],
                font=dict(family=theme['font_family'], color=theme['font_color']),
                coloraxis_colorbar=dict(
                    title=f"Value CAD ({unit_suffix})" if unit_suffix else "Value CAD",
                    tickformat=',.0f'  # Show whole numbers, decimals only when appropriate
                )
            )
            
            st.plotly_chart(fig_heading_treemap, width='stretch')
        
        # Add explanation
        if province != 'All':
            st.caption(f"💡 Showing relative trade value of HS headings (4-digit) within Chapter {hs_chapter} for **{province}**. Larger blocks = higher trade value.")
        else:
            st.caption(f"💡 Showing relative trade value of HS headings (4-digit) within Chapter {hs_chapter} across all provinces. Larger blocks = higher trade value.")

# ==========================================================================
# TAB 2: CONCENTRATION RISK
# ==========================================================================
with deep_tab2:

    # Create title based on filters
    if province != 'All':
        risk_title = f"Concentration Risk Analysis - {province}"
    else:
        risk_title = "Concentration Risk Analysis - All Provinces"

    st.markdown(f"### ⚠️ {risk_title}")
    st.markdown("**Identify economic vulnerabilities through market and product concentration metrics**")
    
    # Query concentration metrics
    with st.spinner("Calculating concentration metrics..."):
        concentration_data = db.query_concentration_metrics({
            'start_date': start_date,
            'end_date': end_date,
            'trade_type': trade_type,
            'province': province,
            'destination': destination,
            'hs_chapter': hs_chapter,
            'hs_heading': hs_heading,
            'hs_commodity': hs_commodity,
            'exclude_usa': exclude_usa
        })
    
    market_conc = concentration_data['market_concentration']
    product_conc = concentration_data['product_concentration']
    
    # Risk Level Helper Function
    def get_risk_level(pct):
        """Return risk level and color based on concentration percentage"""
        if pct >= 50:
            return "🔴 High Risk", "red"
        elif pct >= 30:
            return "🟡 Moderate Risk", "orange"
        else:
            return "🟢 Diversified", "green"
    
    # ======================================================================
    # RISK INDICATORS - Market and Product Concentration
    # ======================================================================
    
    st.markdown("#### 📊 Concentration Indicators")
    
    col1, col2 = st.columns(2)
    
    # Market Concentration Indicator
    with col1:
        st.markdown("**Market Concentration**")
        market_risk, market_color = get_risk_level(market_conc['top1_pct'])
        
        # Progress bar for top country
        st.metric(
            label="Top Market Share",
            value=f"{market_conc['top1_pct']:.1f}%",
            help=f"Percentage of trade with the largest trading partner"
        )
        st.progress(min(market_conc['top1_pct'] / 100, 1.0))
        st.markdown(f"**Status:** {market_risk}")
        
        # Top-N metrics
        st.caption(f"Top 3 markets: {market_conc['top3_pct']:.1f}% | Top 5 markets: {market_conc['top5_pct']:.1f}%")
    
    # Product Concentration Indicator
    with col2:
        st.markdown("**Product Concentration**")
        product_risk, product_color = get_risk_level(product_conc['top1_pct'])
        
        # Progress bar for top product
        st.metric(
            label="Top Product Share",
            value=f"{product_conc['top1_pct']:.1f}%",
            help=f"Percentage of trade from the largest HS chapter"
        )
        st.progress(min(product_conc['top1_pct'] / 100, 1.0))
        st.markdown(f"**Status:** {product_risk}")
        
        # Top-N metrics
        st.caption(f"Top 3 products: {product_conc['top3_pct']:.1f}% | Top 5 products: {product_conc['top5_pct']:.1f}%")
    
    st.markdown("---")
    
    # ======================================================================
    # CONCENTRATION BREAKDOWN - Donut Charts
    # ======================================================================
    
    st.markdown("#### 📈 Concentration Breakdown")
    
    col3, col4 = st.columns(2)
    
    # Market Concentration Donut Chart
    with col3:
        st.markdown("**Top Markets**")
        if market_conc['top_countries']:
            market_df = pd.DataFrame(market_conc['top_countries'][:5])
            
            # Apply scaling to values for hover display
            market_df['scaled_value'] = market_df['value'] / scale_factor
            
            fig_market_donut = px.pie(
                market_df,
                values='pct',
                names='destination',
                hole=0.4,
                color_discrete_sequence=theme['categorical']
            )
            fig_market_donut.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>' +
                              f'Value: ${currency_label} %{{customdata[0]:,.0f}}<br>' +
                              'Share: %{value:.1f}%<br>' +
                              '<extra></extra>',
                customdata=market_df[['scaled_value']].values,
                textfont_color=theme['paper_bg']
            )
            fig_market_donut.update_layout(
                height=350,
                margin=dict(t=10, l=10, r=10, b=10),
                showlegend=False,
                template=theme['template'],
                paper_bgcolor=theme['paper_bg'],
                font=dict(family=theme['font_family'], color=theme['font_color'])
            )
            st.plotly_chart(fig_market_donut, width='stretch')
        else:
            st.info("No market data available")
    
    # Product Concentration Donut Chart
    with col4:
        st.markdown("**Top Products (HS Chapters)**")
        if product_conc['top_chapters']:
            product_df = pd.DataFrame(product_conc['top_chapters'][:5])
            
            # Add summary category and create display label
            product_df['summary'] = product_df['hs_chapter'].map(HS_CHAPTER_SUMMARIES)
            product_df['label'] = product_df['hs_chapter'] + ' - ' + product_df['summary']
            
            # Apply scaling to values for hover display
            product_df['scaled_value'] = product_df['value'] / scale_factor
            
            fig_product_donut = px.pie(
                product_df,
                values='pct',
                names='label',
                hole=0.4,
                color_discrete_sequence=theme['categorical']
            )
            fig_product_donut.update_traces(
                textposition='inside',
                textinfo='label',  # Show HS Code + Summary instead of percentages
                hovertemplate='<b>%{label}</b><br>' +
                              f'Value: ${currency_label} %{{customdata[0]:,.0f}}<br>' +
                              'Share: %{value:.1f}%<br>' +
                              '<extra></extra>',
                customdata=product_df[['scaled_value']].values,
                textfont_color=theme['paper_bg']
            )
            fig_product_donut.update_layout(
                height=350,
                margin=dict(t=10, l=10, r=10, b=10),
                showlegend=False,
                template=theme['template'],
                paper_bgcolor=theme['paper_bg'],
                font=dict(family=theme['font_family'], color=theme['font_color'])
            )
            st.plotly_chart(fig_product_donut, width='stretch')
        else:
            st.info("No product data available")
    
    # ======================================================================
    # DEPENDENCY MATRIX - Province × Country Heatmap
    # ======================================================================
    
    if province == 'All' and concentration_data['dependency_matrix']:
        st.markdown("---")
        st.markdown("#### 🗺️ Dependency Matrix: Province × Country Concentration")
        st.caption("Shows what percentage of each province's trade goes to each country. Darker colors = higher dependency.")
        
        # Convert to pivot table for heatmap
        matrix_df = pd.DataFrame(concentration_data['dependency_matrix'])
        
        # Filter to show only top countries and provinces for readability
        top_countries = matrix_df.groupby('destination')['value'].sum().nlargest(10).index
        matrix_filtered = matrix_df[matrix_df['destination'].isin(top_countries)]
        
        # Create pivot table
        pivot = matrix_filtered.pivot_table(
            index='province',
            columns='destination',
            values='pct_of_province_total',
            fill_value=0
        )
        
        # Shorten country names for display
        pivot.columns = [col.split(' - ')[-1][:20] if ' - ' in col else col[:20] for col in pivot.columns]
        
        # Create heatmap
        fig_heatmap = px.imshow(
            pivot,
            labels=dict(x="Trading Partner", y="Province", color="% of Trade"),
            color_continuous_scale=theme['heatmap'],
            aspect='auto'
        )
        fig_heatmap.update_layout(
            height=500,
            xaxis_title="Trading Partner",
            yaxis_title="Province",
            coloraxis_colorbar=dict(
                title="% of<br>Province<br>Trade"
            ),
            template=theme['template'],
            plot_bgcolor=theme['plot_bg'],
            paper_bgcolor=theme['paper_bg'],
            font=dict(family=theme['font_family'], color=theme['font_color'])
        )
        fig_heatmap.update_xaxes(side='bottom')
        
        st.plotly_chart(fig_heatmap, width='stretch')
        
        # Risk alerts
        high_risk_deps = matrix_df[matrix_df['pct_of_province_total'] >= 50]
        if not high_risk_deps.empty:
            st.warning(f"⚠️ **High Risk Dependencies Detected:** {len(high_risk_deps)} province-country pairs with >50% concentration")
            with st.expander("View High Risk Dependencies"):
                risk_display = high_risk_deps[['province', 'destination', 'pct_of_province_total']].copy()
                risk_display.columns = ['Province', 'Trading Partner', 'Concentration (%)']
                risk_display = risk_display.sort_values('Concentration (%)', ascending=False)
                st.dataframe(risk_display, width='stretch', hide_index=True)

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.caption("""
**Data Source:** Statistics Canada Table 12-10-0011-01 | 
**Dashboard:** Built with Streamlit + DuckDB | 
**Last Updated:** 2026-02-03
""")

# Sidebar info
st.sidebar.markdown("---")
st.sidebar.markdown("### ℹ️ About")
st.sidebar.info(f"""
**Records:** {total_records:,.0f}  
**Date Range:** {start_date} to {end_date}  
**Provinces:** {len(dynamic_opts['provinces'])}  
**HS Chapters:** {len(common_options['chapters'])}  
""")

