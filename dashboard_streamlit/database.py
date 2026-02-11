"""
DuckDB Database Wrapper for Canadian Trade Dashboard

This module provides a clean interface to query trade data stored in Parquet files
using DuckDB for fast, analytical queries.
"""

import duckdb
import pandas as pd
import requests
import os
from pathlib import Path
from typing import Dict, List, Any


class TradeDatabase:
    """
    Interface to Canadian trade data stored in Parquet files.
    
    Uses DuckDB to query Parquet files directly without loading into memory.
    All queries are optimized for dashboard use cases.
    """
    
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
            
        self.trade_parquet_pattern = str(self.data_dir / "trade_records*.parquet")
        self.hs_lookup_parquet = self.data_dir / "hs_lookup.parquet"
        
        # Create in-memory DuckDB connection
        self.conn = duckdb.connect(':memory:')
        
        # Provision data if missing
        self._provision_data()
        
        # Initialize views
        self._initialize_views()
    
    def _provision_data(self):
        """
        Check if data exists locally, and if not, attempt to download from GitHub Releases.
        Provides visual feedback via Streamlit status if running in a Streamlit context.
        """
        # Ensure directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Target files (Using the 'latest' release alias for robustness)
        files_to_check = [
            ("trade_records.parquet", "https://github.com/pocketpiston/Canada-Trade-Dashboard/releases/latest/download/trade_records.parquet"),
            ("hs_lookup.parquet", "https://github.com/pocketpiston/Canada-Trade-Dashboard/releases/latest/download/hs_lookup.parquet")
        ]
        
        missing_files = [f for f, u in files_to_check if not (self.data_dir / f).exists()]
        
        if not missing_files:
            return

        # Check if running in Streamlit context
        try:
            import streamlit as st
            from streamlit.runtime.scriptrunner import get_script_run_ctx
            has_streamlit = get_script_run_ctx() is not None
        except (ImportError, AttributeError):
            has_streamlit = False

        # Use st.status for better UI visibility during large downloads
        if has_streamlit:
            try:
                with st.status("📥 Initializing data provisioning...", expanded=True) as status:
                    for filename, url in files_to_check:
                        dest_path = self.data_dir / filename
                        if not dest_path.exists():
                            status.update(label=f"📥 Downloading {filename}...", state="running")
                            try:
                                # 120 second timeout for large files
                                response = requests.get(url, stream=True, timeout=120)
                                response.raise_for_status()
                                
                                total_size = int(response.headers.get('content-length', 0))
                                progress_bar = st.progress(0, text=f"Downloading {filename}...")
                                
                                downloaded = 0
                                with open(dest_path, 'wb') as f:
                                    for chunk in response.iter_content(chunk_size=8192):
                                        if chunk:
                                            f.write(chunk)
                                            downloaded += len(chunk)
                                            if total_size > 0:
                                                progress = min(downloaded / total_size, 1.0)
                                                progress_bar.progress(progress, text=f"Downloading {filename}: {progress*100:.1f}%")
                                
                                progress_bar.empty()
                                st.success(f"✅ {filename} downloaded.")
                            except Exception as e:
                                st.error(f"❌ Failed to download {filename}: {e}")
                                if dest_path.exists():
                                    dest_path.unlink() # Cleanup partial file
                    
                    status.update(label="✅ Data provisioning complete!", state="complete", expanded=False)
            except Exception as e:
                # If streamlit UI fails, fall back to print
                has_streamlit = False
        
        # Fallback for non-streamlit context (tests) or if st.status fails
        if not has_streamlit:
            for filename, url in files_to_check:
                dest_path = self.data_dir / filename
                if not dest_path.exists():
                    try:
                        print(f"📥 Provisioning {filename} from GitHub...")
                        response = requests.get(url, stream=True, timeout=120)
                        response.raise_for_status()
                        with open(dest_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        print(f"✅ {filename} downloaded successfully.")
                    except Exception as e2:
                        print(f"❌ Failed to download {filename}: {e2}")
    
    def _initialize_views(self):
        """Create DuckDB views from Parquet files."""
        
        # Check if files exist to avoid DuckDB errors
        import glob
        trade_files = glob.glob(self.trade_parquet_pattern)
        
        if not trade_files:
            # Create empty view with correct schema if possible, or just a dummy
            # For now, we'll let it fail or create a dummy to allow app to load
            self.conn.execute("""
                CREATE OR REPLACE VIEW trade_records AS 
                SELECT 
                    CAST(NULL AS DATE) as date,
                    CAST(NULL AS INTEGER) as year,
                    CAST(NULL AS VARCHAR) as province,
                    CAST(NULL AS VARCHAR) as destination,
                    CAST(NULL AS VARCHAR) as trade_type,
                    CAST(NULL AS VARCHAR) as hs_chapter,
                    CAST(NULL AS VARCHAR) as chapter,
                    CAST(NULL AS VARCHAR) as hs_heading,
                    CAST(NULL AS VARCHAR) as heading,
                    CAST(NULL AS VARCHAR) as hs_code,
                    CAST(NULL AS VARCHAR) as commodity,
                    CAST(0 AS DOUBLE) as value
                WHERE 1=0
            """)
        else:
            # Main trade data view
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW trade_records AS
                SELECT * FROM read_parquet('{self.trade_parquet_pattern}')
            """)
        
        if not self.hs_lookup_parquet.exists():
             self.conn.execute("""
                CREATE OR REPLACE VIEW hs_lookup AS 
                SELECT CAST(NULL AS VARCHAR) as hs_chapter, CAST(NULL AS VARCHAR) as summary
                WHERE 1=0
            """)
        else:
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
        try:
            # Get HS chapters with descriptions
            chapters = self.conn.execute("""
                SELECT DISTINCT hs_chapter, chapter
                FROM trade_records
                WHERE hs_chapter IS NOT NULL
                ORDER BY hs_chapter
            """).df().to_dict('records')
            
            # Get date range
            date_res = self.conn.execute("""
                SELECT 
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    MIN(year) as min_year,
                    MAX(year) as max_year
                FROM trade_records
            """).df()
            
            if date_res.empty or date_res.iloc[0]['min_date'] is None:
                # Fallback for empty data
                date_range = {
                    'min_date': '2023-01-01',
                    'max_date': '2025-12-31',
                    'min_year': 2023,
                    'max_year': 2025
                }
            else:
                date_range = date_res.iloc[0].to_dict()
            
            # Get trade types
            trade_types = self.conn.execute("""
                SELECT DISTINCT trade_type
                FROM trade_records
                WHERE trade_type IS NOT NULL
                ORDER BY trade_type
            """).df()['trade_type'].tolist()
            
            if not trade_types:
                trade_types = ['Export', 'Import']
                
            return {
                'chapters': chapters,
                'date_range': date_range,
                'trade_types': trade_types
            }
        except Exception as e:
            # absolute fallback
            return {
                'chapters': [],
                'date_range': {'min_date': '2023-01-01', 'max_date': '2025-12-31', 'min_year': 2023, 'max_year': 2025},
                'trade_types': ['Export', 'Import']
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
    
    def query_concentration_metrics(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate concentration risk metrics for market and product dependencies.
        
        Args:
            filters: Standard filter dictionary
            
        Returns:
            Dictionary with:
                - market_concentration: {top1_pct, top3_pct, top5_pct, top_countries: [{country, value, pct}, ...]}
                - product_concentration: {top1_pct, top3_pct, top5_pct, top_chapters: [{chapter, value, pct}, ...]}
                - dependency_matrix: [{province, country, value, pct_of_province_total}, ...]
        """
        where_parts = self._build_where_clause(filters)
        where_clause = ' AND '.join(where_parts)
        
        # Market Concentration (by country)
        market_query = f"""
        WITH country_totals AS (
            SELECT 
                destination,
                SUM(value) as value
            FROM trade_records
            WHERE {where_clause}
            GROUP BY destination
        ),
        total AS (
            SELECT SUM(value) as total_value FROM country_totals
        )
        SELECT 
            destination,
            value,
            ROUND(100.0 * value / total.total_value, 2) as pct
        FROM country_totals, total
        ORDER BY value DESC
        LIMIT 10
        """
        market_data = self.conn.execute(market_query).df().to_dict('records')
        
        # Calculate top-N percentages
        top1_market = market_data[0]['pct'] if len(market_data) > 0 else 0
        top3_market = sum([d['pct'] for d in market_data[:3]]) if len(market_data) >= 3 else 0
        top5_market = sum([d['pct'] for d in market_data[:5]]) if len(market_data) >= 5 else 0
        
        # Product Concentration (by HS chapter)
        product_query = f"""
        WITH chapter_totals AS (
            SELECT 
                hs_chapter,
                chapter,
                SUM(value) as value
            FROM trade_records
            WHERE {where_clause} AND hs_chapter IS NOT NULL
            GROUP BY hs_chapter, chapter
        ),
        total AS (
            SELECT SUM(value) as total_value FROM chapter_totals
        )
        SELECT 
            hs_chapter,
            chapter,
            value,
            ROUND(100.0 * value / total.total_value, 2) as pct
        FROM chapter_totals, total
        ORDER BY value DESC
        LIMIT 10
        """
        product_data = self.conn.execute(product_query).df().to_dict('records')
        
        top1_product = product_data[0]['pct'] if len(product_data) > 0 else 0
        top3_product = sum([d['pct'] for d in product_data[:3]]) if len(product_data) >= 3 else 0
        top5_product = sum([d['pct'] for d in product_data[:5]]) if len(product_data) >= 5 else 0
        
        # Dependency Matrix (Province × Country)
        # Only calculate if no province filter is applied
        dependency_matrix = []
        if filters.get('province') == 'All' or 'province' not in filters:
            matrix_query = f"""
            WITH province_country AS (
                SELECT 
                    province,
                    destination,
                    SUM(value) as value
                FROM trade_records
                WHERE {where_clause} AND province != 'Canada (Total)'
                GROUP BY province, destination
            ),
            province_totals AS (
                SELECT 
                    province,
                    SUM(value) as total
                FROM province_country
                GROUP BY province
            )
            SELECT 
                pc.province,
                pc.destination,
                pc.value,
                ROUND(100.0 * pc.value / pt.total, 2) as pct_of_province_total
            FROM province_country pc
            JOIN province_totals pt ON pc.province = pt.province
            WHERE pc.value > 0
            ORDER BY pc.province, pc.value DESC
            """
            dependency_matrix = self.conn.execute(matrix_query).df().to_dict('records')
        
        return {
            'market_concentration': {
                'top1_pct': top1_market,
                'top3_pct': top3_market,
                'top5_pct': top5_market,
                'top_countries': market_data
            },
            'product_concentration': {
                'top1_pct': top1_product,
                'top3_pct': top3_product,
                'top5_pct': top5_product,
                'top_chapters': product_data
            },
            'dependency_matrix': dependency_matrix
        }
    
    def query_sankey_data(self, filters: Dict[str, Any], flow_type: str = 'export') -> Dict[str, Any]:
        """
        Get data for Sankey diagram showing trade flows.
        
        Args:
            filters: Standard filter dictionary
            flow_type: 'export' (Province → Country → Chapter) or 'import' (Country → Province → Chapter)
            
        Returns:
            Dictionary with nodes and links for Sankey diagram
        """
        where_parts = self._build_where_clause(filters)
        where_clause = ' AND '.join(where_parts)
        
        # Get flow data: Province → Country → Chapter
        flow_query = f"""
        SELECT 
            province,
            destination,
            hs_chapter,
            chapter,
            SUM(value) as value
        FROM trade_records
        WHERE {where_clause} 
            AND province != 'Canada (Total)'
            AND hs_chapter IS NOT NULL
        GROUP BY province, destination, hs_chapter, chapter
        HAVING SUM(value) > 0
        ORDER BY value DESC
        LIMIT 200
        """
        flows = self.conn.execute(flow_query).df()
        
        return {
            'flows': flows.to_dict('records')
        }
    
    def query_province_comparison_metrics(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get comparison metrics for all provinces.
        
        Args:
            filters: Standard filter dictionary (province filter will be ignored)
            
        Returns:
            List of dictionaries with metrics for each province:
                - province: str
                - total_value: float
                - num_countries: int (market diversification)
                - num_chapters: int (product diversification)
                - top_destination: str
                - top_chapter: str
                - growth_rate: float (YoY if applicable)
        """
        # Build where clause without province filter
        filters_copy = filters.copy()
        filters_copy['province'] = 'All'
        where_parts = self._build_where_clause(filters_copy)
        where_clause = ' AND '.join(where_parts)
        
        query = f"""
        WITH province_stats AS (
            SELECT 
                province,
                SUM(value) as total_value,
                COUNT(DISTINCT destination) as num_countries,
                COUNT(DISTINCT hs_chapter) as num_chapters
            FROM trade_records
            WHERE {where_clause} AND province != 'Canada (Total)'
            GROUP BY province
        ),
        top_destinations AS (
            SELECT 
                province,
                destination as top_destination,
                ROW_NUMBER() OVER (PARTITION BY province ORDER BY SUM(value) DESC) as rn
            FROM trade_records
            WHERE {where_clause} AND province != 'Canada (Total)'
            GROUP BY province, destination
        ),
        top_chapters AS (
            SELECT 
                province,
                hs_chapter || ' - ' || chapter as top_chapter,
                ROW_NUMBER() OVER (PARTITION BY province ORDER BY SUM(value) DESC) as rn
            FROM trade_records
            WHERE {where_clause} AND province != 'Canada (Total)' AND hs_chapter IS NOT NULL
            GROUP BY province, hs_chapter, chapter
        )
        SELECT 
            ps.province,
            ps.total_value,
            ps.num_countries,
            ps.num_chapters,
            td.top_destination,
            tc.top_chapter
        FROM province_stats ps
        LEFT JOIN top_destinations td ON ps.province = td.province AND td.rn = 1
        LEFT JOIN top_chapters tc ON ps.province = tc.province AND tc.rn = 1
        ORDER BY ps.total_value DESC
        """
        
        return self.conn.execute(query).df().to_dict('records')
    
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
