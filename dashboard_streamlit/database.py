"""
DuckDB Database Wrapper for Canadian Trade Dashboard

This module provides a clean interface to query trade data stored in Parquet files
using DuckDB for fast, analytical queries.
"""

import duckdb
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any
import streamlit as st


class TradeDatabase:
    """
    Interface to Canadian trade data stored in Parquet files.
    
    Uses DuckDB to query Parquet files directly without loading into memory.
    All queries are optimized for dashboard use cases.
    """
    
    # GitHub Release URL for data download
    DATA_RELEASE_URL = "https://github.com/pocketpiston/Canada-Trade-Dashboard/releases/download/v1.0.0/trade_records.parquet"
    
    def __init__(self, data_dir: str = None):
        """
        Initialize database connection.
        
        Args:
            data_dir: Directory containing Parquet files (default: resolved relative to this file)
        """
        if data_dir is None:
            # Resolve relative to this file: ../data/processed (assuming this file is in dashboard_streamlit/)
            base_dir = Path(__file__).resolve().parent.parent
            self.data_dir = base_dir / "data" / "processed"
        else:
            self.data_dir = Path(data_dir)
        
        # Ensure directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.trade_parquet_pattern = str(self.data_dir / "trade_records*.parquet")
        self.trade_parquet_file = self.data_dir / "trade_records.parquet"
        self.hs_lookup_parquet = self.data_dir / "hs_lookup.parquet"
        
        # Auto-download data if missing
        if not self.trade_parquet_file.exists():
            self._download_trade_data()
        
        # Create in-memory DuckDB connection
        self.conn = duckdb.connect(':memory:')
        
        # Set query timeout to prevent hanging (30 seconds)
        self.conn.execute("SET max_query_timeout = 30000")
        
        # Initialize views
        self._initialize_views()
    
    def _download_trade_data(self):
        """Download trade data from GitHub Releases if not present."""
        import requests
        
        st.info("📥 Downloading trade data (350 MB)... This will take a few minutes on first run.")
        
        try:
            # Stream download with progress bar
            response = requests.get(self.DATA_RELEASE_URL, stream=True, timeout=300)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            if total_size > 0:
                progress_bar = st.progress(0, text="Downloading trade data...")
                downloaded = 0
                
                with open(self.trade_parquet_file, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            progress = min(downloaded / total_size, 1.0)
                            progress_bar.progress(progress, text=f"Downloading: {downloaded / 1024 / 1024:.1f} MB / {total_size / 1024 / 1024:.1f} MB")
                
                progress_bar.empty()
                st.success("✅ Data downloaded successfully! Refresh the page to load the dashboard.")
                st.stop()
            else:
                # Fallback without progress
                with open(self.trade_parquet_file, 'wb') as f:
                    f.write(response.content)
                st.success("✅ Data downloaded successfully! Refresh the page to load the dashboard.")
                st.stop()
                
        except requests.exceptions.RequestException as e:
            st.error(f"❌ Failed to download data: {e}")
            st.info("""
            **Alternative options:**
            1. Run the data processing scripts locally:
               ```bash
               python scripts/extract_all_trade.py
               python scripts/convert_to_parquet.py
               ```
            2. Download manually from: {self.DATA_RELEASE_URL}
               Place in: `data/processed/trade_records.parquet`
            """)
            st.stop()
        except Exception as e:
            st.error(f"❌ Unexpected error: {e}")
            st.stop()
    
    def _initialize_views(self):
        """Create DuckDB views from Parquet files."""
        
        # Main trade data view
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW trade_records AS
            SELECT * FROM read_parquet('{self.trade_parquet_pattern}')
        """)
        
        # HS code lookup view
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW hs_lookup AS
            SELECT * FROM read_parquet('{self.hs_lookup_parquet}')
        """)
    
    def get_common_options(self) -> Dict[str, Any]:
        """
        Get common filter options (static chunks like Dates, Trade Types).
        Provinces and Destinations are fetched dynamically now.
        """
        # Get HS chapters with descriptions (Filtered by existence? Keep global for now or filter?)
        # Let's keep Chapters global for performance, or filter if requested.
        chapters = self.conn.execute("""
            SELECT DISTINCT hs_chapter, chapter
            FROM trade_records
            WHERE hs_chapter IS NOT NULL
            ORDER BY hs_chapter
        """).df().to_dict('records')
        
        # Get date range
        date_range = self.conn.execute("""
            SELECT 
                MIN(date) as min_date,
                MAX(date) as max_date,
                MIN(year) as min_year,
                MAX(year) as max_year
            FROM trade_records
        """).df().iloc[0].to_dict()
        
        # Get trade types
        trade_types = self.conn.execute("""
            SELECT DISTINCT trade_type
            FROM trade_records
            ORDER BY trade_type
        """).df()['trade_type'].tolist()
        
        return {
            'chapters': chapters,
            'date_range': date_range,
            'trade_types': trade_types
        }

    def get_provinces(self, trade_type: str = 'All') -> List[str]:
        """Get provinces, optionally filtered by trade type."""
        where_clause = "province != 'Canada (Total)'"
        if trade_type != 'All':
            where_clause += f" AND trade_type = '{trade_type}'"
            
        return self.conn.execute(f"""
            SELECT DISTINCT province
            FROM trade_records
            WHERE {where_clause}
            ORDER BY province
        """).df()['province'].tolist()

    def get_countries(self, trade_type: str = 'All') -> List[str]:
        """Get partner countries/destinations/origins filtered by trade type."""
        where_clause = "destination IS NOT NULL"
        if trade_type != 'All':
            where_clause += f" AND trade_type = '{trade_type}'"
            
        return self.conn.execute(f"""
            SELECT DISTINCT destination
            FROM trade_records
            WHERE {where_clause}
            ORDER BY destination
        """).df()['destination'].tolist()
    
    def get_filter_options(self) -> Dict[str, Any]:
        """Legacy support / Get all options (Defaults to All)"""
        common = self.get_common_options()
        common['provinces'] = self.get_provinces('All')
        common['destinations'] = self.get_countries('All')
        return common
    
    def get_hs_headings(self, chapter: str = None) -> List[Dict[str, str]]:
        """
        Get HS headings, optionally filtered by chapter.
        
        Args:
            chapter: HS chapter code (2-digit) or None for all
        
        Returns:
            List of dictionaries with hs_heading and heading description
        """
        if chapter and chapter != 'All':
            query = f"""
                SELECT DISTINCT hs_heading, heading
                FROM trade_records
                WHERE hs_chapter = '{chapter}' AND hs_heading IS NOT NULL
                ORDER BY hs_heading
            """
        else:
            query = """
                SELECT DISTINCT hs_heading, heading
                FROM trade_records
                WHERE hs_heading IS NOT NULL
                ORDER BY hs_heading
            """
        
        return self.conn.execute(query).df().to_dict('records')
    
    def get_hs_commodities(self, chapter: str = None, heading: str = None) -> List[Dict[str, str]]:
        """
        Get HS commodities, optionally filtered by chapter and/or heading.
        
        Args:
            chapter: HS chapter code (2-digit) or None
            heading: HS heading code (4-digit) or None
        
        Returns:
            List of dictionaries with hs_code and commodity description
        """
        where_parts = []
        if chapter and chapter != 'All':
            where_parts.append(f"hs_chapter = '{chapter}'")
        if heading and heading != 'All':
            where_parts.append(f"hs_heading = '{heading}'")
        
        where_clause = ' AND '.join(where_parts) if where_parts else '1=1'
        
        query = f"""
            SELECT DISTINCT hs_code, commodity
            FROM trade_records
            WHERE {where_clause} AND hs_code IS NOT NULL
            ORDER BY hs_code
            LIMIT 1000
        """
        
        return self.conn.execute(query).df().to_dict('records')
    
    def query_dashboard_stats(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Query main dashboard statistics with filters applied.
        
        Args:
            filters: Dictionary with filter criteria:
                - start_date: str (YYYY-MM-DD)
                - end_date: str (YYYY-MM-DD)
                - trade_type: str (Export/Import/All)
                - province: str or 'All'
                - hs_chapter: str or 'All'
        
        Returns:
            Dictionary with:
                - kpi: {total_value, total_records, avg_monthly}
                - time_series: [{month, value}, ...]
                - top_destinations: [{destination, value}, ...]
                - top_provinces: [{province, value}, ...]
                - top_hs_codes: [{code, description, value}, ...]
        """
        
        # Build WHERE clause
        where_parts = self._build_where_clause(filters)
        where_clause = ' AND '.join(where_parts)
        
        # KPI metrics
        kpi_query = f"""
        SELECT
            SUM(value) as total_value,
            COUNT(*) as total_records,
            SUM(value) / COUNT(DISTINCT strftime(date, '%Y-%m')) as avg_monthly
        FROM trade_records
        WHERE {where_clause}
        """
        kpi = self.conn.execute(kpi_query).df().iloc[0].to_dict()
        
        # Time series (monthly aggregates)
        time_series_query = f"""
        SELECT 
            strftime(date, '%Y-%m-01') as month,
            SUM(value) as value
        FROM trade_records
        WHERE {where_clause}
        GROUP BY strftime(date, '%Y-%m-01')
        ORDER BY month
        """
        time_series = self.conn.execute(time_series_query).df().to_dict('records')
        
        # Top destinations
        destinations_query = f"""
        SELECT 
            destination,
            SUM(value) as value
        FROM trade_records
        WHERE {where_clause}
        GROUP BY destination
        ORDER BY value DESC
        LIMIT 10
        """
        destinations = self.conn.execute(destinations_query).df().to_dict('records')
        
        # Top provinces
        provinces_query = f"""
        SELECT 
            province,
            SUM(value) as value
        FROM trade_records
        WHERE {where_clause} AND province != 'Canada (Total)'
        GROUP BY province
        ORDER BY value DESC
        LIMIT 10
        """
        provinces = self.conn.execute(provinces_query).df().to_dict('records')
        
        # Top HS chapters
        hs_query = f"""
        SELECT 
            hs_chapter as code,
            chapter as description,
            SUM(value) as value
        FROM trade_records
        WHERE {where_clause} AND hs_chapter IS NOT NULL
        GROUP BY hs_chapter, chapter
        ORDER BY value DESC
        LIMIT 10
        """
        hs_codes = self.conn.execute(hs_query).df().to_dict('records')
        
        # Top HS headings (when chapter is selected)
        hs_chapter_filter = filters.get('hs_chapter', 'All')
        
        if hs_chapter_filter and hs_chapter_filter != 'All':
            hs_headings_query = f"""
            SELECT 
                hs_heading as code,
                heading as description,
                SUM(value) as value
            FROM trade_records
            WHERE {where_clause} AND hs_heading IS NOT NULL
            GROUP BY hs_heading, heading
            ORDER BY value DESC
            LIMIT 20
            """
            hs_headings = self.conn.execute(hs_headings_query).df().to_dict('records')
        else:
            hs_headings = []
        
        return {
            'kpi': kpi,
            'time_series': time_series,
            'top_destinations': destinations,
            'top_provinces': provinces,
            'top_hs_codes': hs_codes,
            'top_hs_headings': hs_headings
        }
    
    def _build_where_clause(self, filters: Dict[str, Any]) -> List[str]:
        """
        Build WHERE clause components from filters.
        
        Args:
            filters: Dictionary of filter values
        
        Returns:
            List of WHERE clause strings
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
        
        # Exclude USA filter
        if filters.get('exclude_usa') == True:
            # Exclude all destinations that start with "USA -"
            where_parts.append("destination NOT LIKE 'USA - %'")
        
        # Default to TRUE if no filters (return all data)
        if not where_parts:
            where_parts.append('1=1')
        
        return where_parts
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


# Test the database wrapper
if __name__ == "__main__":
    import time
    
    print("=" * 70)
    print("TESTING DUCKDB DATABASE WRAPPER")
    print("=" * 70 + "\n")
    
    # Initialize database
    print("🔌 Connecting to database...")
    db = TradeDatabase()
    print("✅ Connected!\n")
    
    # Test 1: Get filter options
    print("📋 Test 1: Loading filter options...")
    start = time.time()
    options = db.get_filter_options()
    elapsed = time.time() - start
    
    print(f"   ✓ Provinces: {len(options['provinces'])}")
    print(f"   ✓ HS Chapters: {len(options['chapters'])}")
    print(f"   ✓ Date range: {options['date_range']['min_date']} to {options['date_range']['max_date']}")
    print(f"   ⏱️  Time: {elapsed:.3f}s\n")
    
    # Test 2: Query dashboard stats (all data)
    print("📊 Test 2: Query all data...")
    start = time.time()
    filters = {
        'start_date': '2023-01-01',
        'end_date': '2025-12-31',
        'trade_type': 'All',
        'province': 'All',
        'hs_chapter': 'All'
    }
    results = db.query_dashboard_stats(filters)
    elapsed = time.time() - start
    
    print(f"   ✓ Total records: {results['kpi']['total_records']:,}")
    print(f"   ✓ Total value: ${results['kpi']['total_value']:,.0f} CAD")
    print(f"   ✓ Avg monthly: ${results['kpi']['avg_monthly']:,.0f} CAD")
    print(f"   ✓ Time series points: {len(results['time_series'])}")
    print(f"   ✓ Top destination: {results['top_destinations'][0]['destination']}")
    print(f"   ⏱️  Time: {elapsed:.3f}s\n")
    
    # Test 3: Query with filters (Ontario, 2023)
    print("📊 Test 3: Query with filters (Ontario, 2023)...")
    start = time.time()
    filters = {
        'start_date': '2023-01-01',
        'end_date': '2023-12-31',
        'trade_type': 'Export',
        'province': 'Ontario',
        'hs_chapter': 'All'
    }
    results = db.query_dashboard_stats(filters)
    elapsed = time.time() - start
    
    print(f"   ✓ Filtered records: {results['kpi']['total_records']:,}")
    print(f"   ✓ Filtered value: ${results['kpi']['total_value']:,.0f} CAD")
    print(f"   ✓ Top Chapter: {results['top_hs_codes'][0]['description']}")
    print(f"   ⏱️  Time: {elapsed:.3f}s\n")
    
    # Close connection
    db.close()
    
    print("=" * 70)
    print("✅ ALL TESTS PASSED!")
    print("=" * 70)
    print("\n🚀 Database wrapper ready for Streamlit dashboard!")
