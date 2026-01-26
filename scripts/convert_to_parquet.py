#!/usr/bin/env python3
"""
Convert Raw Trade Data to Parquet Format

This script processes all raw JSON files from the Statistics Canada API
and converts them into optimized Parquet files for use with DuckDB.

Output:
- data/processed/trade_records.parquet - Main trade data
- data/processed/hs_lookup.parquet - HS code hierarchy lookup
- data/processed/metadata.json - Processing metadata
"""

import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# Add parent directory to path to import from extract_trade_data
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from extract_trade_data import process_data, CHAPTER_MAP, HEADING_MAP, COMMODITY_MAP

# Configuration
RAW_DATA_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
OUTPUT_FILE = os.path.join(PROCESSED_DIR, "trade_records.parquet")
HS_LOOKUP_FILE = os.path.join(PROCESSED_DIR, "hs_lookup.parquet")
METADATA_FILE = os.path.join(PROCESSED_DIR, "metadata.json")


def process_all_raw_files():
    """
    Process all raw JSON files from data/raw/ directory.
    
    Returns:
        List of processed records
    """
    print(f"🔍 Scanning raw data in {RAW_DATA_DIR} and data/raw_imports...")
    all_rows = []
    file_count = 0
    error_count = 0
    
    # Process both exports and imports directories
    dirs_to_process = [RAW_DATA_DIR, "data/raw_imports"]
    
    for process_dir in dirs_to_process:
        if not os.path.exists(process_dir):
            print(f"   ⚠️  Directory {process_dir} does not exist, skipping...")
            continue
            
        # Walk through Year/Chapter directories
        for root, dirs, files in os.walk(process_dir):
            for file in files:
                if file.endswith(".json"):
                    try:
                        # Determine Trade Type based on directory
                        is_import = "raw_imports" in process_dir
                        current_trade_type = "Import" if is_import else "Export"
                        
                        # Parse filename for province ID (format: "{month:02d}_{prov_id}.json")
                        parts = file.replace('.json', '').split('_')
                        if len(parts) >= 2:
                            prov_id = int(parts[1])
                        else:
                            prov_id = None
                        
                        # Load and process raw data
                        file_path = os.path.join(root, file)
                        with open(file_path, 'r', encoding='utf-8') as f:
                            raw_data = json.load(f)
                    
                        # Process data using existing enrichment logic
                        rows = process_data(raw_data, current_prov_id=prov_id)
                        
                        # Post-process: Override TradeType if Import
                        # (Because process_data defaults to Export)
                        if is_import:
                            for row in rows:
                                row['TradeType'] = 'Import'
                        
                        # Apply filter logic (same as extract script)
                        valid_rows = []
                        for row in rows:
                            is_us = (str(row.get('CountryCode')) == '9')
                            if prov_id == 0:
                                # Canada Total: exclude US (handled separately by provinces)
                                if not is_us:
                                    valid_rows.append(row)
                            else:
                                # Provincial data: include all
                                valid_rows.append(row)
                        
                        all_rows.extend(valid_rows)
                        file_count += 1
                        
                        if file_count % 50 == 0:
                            print(f"   ✓ Processed {file_count} files, {len(all_rows):,} records...", flush=True)
                    
                    except Exception as e:
                        error_count += 1
                        print(f"   ⚠️  Error processing {file}: {e}")
    
    print(f"\n✅ Processed {file_count} files successfully")
    if error_count > 0:
        print(f"⚠️  {error_count} files had errors")
    print(f"📊 Total records: {len(all_rows):,}")
    
    return all_rows


def normalize_records(records):
    """
    Normalize record structure for Parquet conversion.
    
    Args:
        records: List of raw processed records
    
    Returns:
        pandas DataFrame with normalized schema
    """
    print("\n🔄 Normalizing record structure...")
    
    # Convert to DataFrame
    df = pd.DataFrame(records)
    
    # Rename and normalize columns
    column_mapping = {
        'Date': 'date',
        'Value': 'value',
        'Quantity': 'quantity',
        'Province': 'province',
        'Destination': 'destination',
        'DestinationISO': 'destination_iso',
        'DestinationState': 'destination_state',
        'HSCode': 'hs_code',
        'Chapter': 'chapter',
        'Heading': 'heading',
        'Commodity': 'commodity',
        'UOM': 'uom',
        'TradeType': 'trade_type',
        'CountryCode': 'country_code',
        'ProvinceCode': 'province_code'
    }
    
    # Rename columns that exist
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    # Extract HS code components (chapter, heading)
    if 'hs_code' in df.columns:
        df['hs_chapter'] = df['hs_code'].str[:2]
        df['hs_heading'] = df['hs_code'].str[:4]
    
    # Parse date to datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Convert numeric columns
    numeric_columns = ['value', 'quantity']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Add year and month columns for easier filtering
    if 'date' in df.columns:
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
    
    # Select and order final columns
    final_columns = [
        'date', 'year', 'month',
        'trade_type',
        'province', 'province_code',
        'destination', 'destination_iso', 'destination_state',
        'hs_code', 'hs_chapter', 'hs_heading',
        'chapter', 'heading', 'commodity',
        'value', 'quantity', 'uom'
    ]
    
    # Include only columns that exist
    df = df[[col for col in final_columns if col in df.columns]]
    
    print(f"   ✓ Normalized {len(df):,} records")
    print(f"   ✓ Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"   ✓ Columns: {', '.join(df.columns)}")
    
    return df


def create_hs_lookup():
    """
    Create HS code hierarchy lookup table.
    
    Returns:
        pandas DataFrame with HS code hierarchy
    """
    print("\n🏗️  Creating HS code lookup table...")
    
    hs_data = []
    
    # Chapters (2-digit)
    for code, desc in CHAPTER_MAP.items():
        hs_data.append({
            'hs_level': 'chapter',
            'hs_code': code,
            'hs_chapter': code,
            'description': desc
        })
    
    # Headings (4-digit)
    for code, desc in HEADING_MAP.items():
        hs_data.append({
            'hs_level': 'heading',
            'hs_code': code,
            'hs_chapter': code[:2] if len(code) >= 2 else code,
            'hs_heading': code,
            'description': desc
        })
    
    # Commodities (8-digit)
    for code, info in COMMODITY_MAP.items():
        hs_data.append({
            'hs_level': 'commodity',
            'hs_code': code,
            'hs_chapter': code[:2] if len(code) >= 2 else code,
            'hs_heading': code[:4] if len(code) >= 4 else code,
            'description': info.get('EN', code),
            'uom': info.get('UOM')
        })
    
    df = pd.DataFrame(hs_data)
    
    print(f"   ✓ Created lookup with {len(df):,} HS codes")
    print(f"   ✓ Chapters: {len(CHAPTER_MAP)}, Headings: {len(HEADING_MAP)}, Commodities: {len(COMMODITY_MAP)}")
    
    return df


def save_to_parquet(df, output_path, description):
    """
    Save DataFrame to Parquet with optimal settings for DuckDB.
    
    Args:
        df: pandas DataFrame
        output_path: Output file path
        description: Description for logging
    """
    print(f"\n💾 Saving {description}...")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save with Snappy compression (good balance of speed and size)
    df.to_parquet(
        output_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )
    
    # Get file size
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    
    print(f"   ✓ Saved to: {output_path}")
    print(f"   ✓ Records: {len(df):,}")
    print(f"   ✓ File size: {file_size_mb:.2f} MB")


def save_metadata(total_records, trade_df):
    """
    Save processing metadata.
    
    Args:
        total_records: Total number of records processed
        trade_df: Trade data DataFrame for stats
    """
    print(f"\n📝 Saving metadata...")
    
    metadata = {
        'created_at': datetime.now().isoformat(),
        'total_records': total_records,
        'date_range': {
            'start': str(trade_df['date'].min()),
            'end': str(trade_df['date'].max())
        },
        'years': sorted(trade_df['year'].unique().tolist()),
        'provinces': sorted(trade_df['province'].unique().tolist()),
        'trade_types': sorted(trade_df['trade_type'].unique().tolist()) if 'trade_type' in trade_df.columns else [],
        'hs_chapters': sorted(trade_df['hs_chapter'].unique().tolist()),
        'total_value_cad': float(trade_df['value'].sum()),
        'files': {
            'trade_records': OUTPUT_FILE,
            'hs_lookup': HS_LOOKUP_FILE
        }
    }
    
    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)
    with open(METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"   ✓ Saved to: {METADATA_FILE}")


def main():
    """Main processing pipeline."""
    print("=" * 70)
    print("CANADIAN TRADE DATA - PARQUET CONVERSION")
    print("=" * 70)
    
    # Step 1: Process all raw JSON files
    records = process_all_raw_files()
    
    if not records:
        print("❌ No records found to process!")
        return
    
    # Step 2: Normalize records to DataFrame
    trade_df = normalize_records(records)
    
    # Step 3: Create HS code lookup table
    hs_lookup_df = create_hs_lookup()
    
    # Step 4: Save to Parquet
    save_to_parquet(trade_df, OUTPUT_FILE, "trade records")
    save_to_parquet(hs_lookup_df, HS_LOOKUP_FILE, "HS code lookup")
    
    # Step 5: Save metadata
    save_metadata(len(records), trade_df)
    
    # Summary
    print("\n" + "=" * 70)
    print("✅ CONVERSION COMPLETE!")
    print("=" * 70)
    print(f"\n📁 Output files:")
    print(f"   • {OUTPUT_FILE}")
    print(f"   • {HS_LOOKUP_FILE}")
    print(f"   • {METADATA_FILE}")
    print(f"\n📊 Summary:")
    print(f"   • Records: {len(records):,}")
    print(f"   • Date range: {trade_df['date'].min()} to {trade_df['date'].max()}")
    print(f"   • Total value: ${trade_df['value'].sum():,.0f} CAD")
    print(f"\n🚀 Ready for DuckDB and Streamlit dashboard!")
    print("=" * 70)


if __name__ == "__main__":
    main()
