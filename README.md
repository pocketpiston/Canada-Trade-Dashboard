# 🇨🇦 Canada Trade Analysis Dashboard

An interactive Streamlit dashboard for analyzing Canadian international trade data. Explore exports and imports by province, destination, HS commodity codes, and time periods with beautiful visualizations and multiple theme options.

![Dashboard Preview](https://via.placeholder.com/800x400?text=Dashboard+Screenshot+Coming+Soon)

## ✨ Features

- **📊 Interactive Visualizations** - Powered by Plotly for smooth, responsive charts
- **🎨 4 Theme Options** - Classic, High Contrast, Dark Mode, and Colorblind Friendly
- **📈 Auto-Scaling Units** - Automatically displays values in Trillions, Billions, Millions, or Thousands
- **🔍 Advanced Filtering** - Filter by trade type, province, destination, HS codes, and date range
- **⚡ Lightning Fast** - DuckDB queries on Parquet files for sub-second response times
- **🌐 Full Data Pipeline** - Includes scripts to extract and process data from Statistics Canada

## 🚀 Quick Start

### Option 1: Run Locally (Recommended)

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/Canada-Trade-Dashboard.git
cd Canada-Trade-Dashboard

# Install dependencies
pip install -r requirements.txt

# Run the dashboard
streamlit run dashboard_streamlit/app.py
```

**Note:** On first run, the dashboard will automatically download the trade data (350 MB) from GitHub Releases. This may take a few minutes depending on your internet connection.

### Option 2: Deploy to Streamlit Cloud

1. Fork this repository
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Deploy from your forked repo
4. Set main file path: `dashboard_streamlit/app.py`

The data will be automatically downloaded on first deployment.

## 📊 Data

### Data Source

All data is sourced from **Statistics Canada's International Merchandise Trade Database**:
- API: https://www150.statcan.gc.ca/t1/cimt/
- License: [Open Government License - Canada](https://open.canada.ca/en/open-government-licence-canada)

### Data Coverage

- **Time Period:** 2008-2025 (configurable)
- **Trade Types:** Exports and Imports
- **Provinces:** All Canadian provinces and territories
- **Destinations:** 200+ countries and territories
- **HS Codes:** Full 8-digit Harmonized System classification

### Data Processing

The dashboard uses pre-processed Parquet files for optimal performance. To generate your own data or update with new years:

```bash
# 1. Extract raw data from Statistics Canada API
python scripts/extract_all_trade.py

# 2. Convert to optimized Parquet format
python scripts/convert_to_parquet.py
```

See [scripts/README.md](scripts/README.md) for detailed instructions.

## 🎨 Visualization Themes

The dashboard includes 4 professionally designed themes:

### Classic
Clean, professional color scheme with high readability

### High Contrast
Bold, saturated colors for maximum visual distinction

### Dark Mode
Dark palettes optimized for low-light environments

### Colorblind Friendly
Scientifically-designed palettes (Viridis, Plasma, Cividis) accessible to colorblind users

See [VISUALIZATION_THEMES_REFERENCE.md](VISUALIZATION_THEMES_REFERENCE.md) for theme details and customization options.

## 📁 Project Structure

```
Canada-Trade-Dashboard/
├── dashboard_streamlit/          # Main dashboard application
│   ├── app.py                   # Streamlit dashboard
│   ├── database.py              # DuckDB query interface
│   ├── hs_summaries.py         # HS code descriptions
│   └── requirements.txt         # Python dependencies
├── scripts/                      # Data processing scripts
│   ├── extract_all_trade.py    # Download data from Stats Canada API
│   ├── convert_to_parquet.py   # Convert to Parquet format
│   └── README.md                # Processing pipeline documentation
├── data/                         # Data directory
│   ├── reference/               # Reference data (HS codes, provinces)
│   └── processed/               # Processed Parquet files
├── README.md                     # This file
├── VISUALIZATION_THEMES_REFERENCE.md  # Theme documentation
└── requirements.txt              # Root dependencies
```

## 🛠️ Technology Stack

- **Frontend:** [Streamlit](https://streamlit.io/) - Interactive web applications
- **Visualization:** [Plotly](https://plotly.com/python/) - Interactive charts
- **Database:** [DuckDB](https://duckdb.org/) - Fast analytical queries
- **Data Format:** [Parquet](https://parquet.apache.org/) - Columnar storage
- **Data Processing:** [Pandas](https://pandas.pydata.org/) - Data manipulation

## 📖 Usage Examples

### Filter by Province and Trade Type
1. Select "Export" from Trade Type dropdown
2. Choose "Ontario" from Province filter
3. View Ontario's export destinations and top commodities

### Analyze Specific HS Code
1. Select an HS Chapter (e.g., "87 - Vehicles")
2. Drill down to specific headings or commodities
3. View trade patterns over time

### Compare Time Periods
1. Use the date range slider to select specific years
2. Toggle between Exports and Imports
3. Compare top destinations across periods

### Change Display Units
1. Select scale from dropdown (Auto, Trillions, Billions, Millions, Thousands)
2. All charts and KPIs update automatically
3. Auto mode intelligently selects the best scale

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/Canada-Trade-Dashboard.git

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run locally
streamlit run dashboard_streamlit/app.py
```

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

Data is sourced from Statistics Canada and subject to the [Open Government License - Canada](https://open.canada.ca/en/open-government-licence-canada).

## 🙏 Acknowledgments

- **Statistics Canada** for providing comprehensive trade data through their open API
- **Streamlit** for the excellent dashboard framework
- **DuckDB** for blazing-fast analytical queries
- **Plotly** for beautiful, interactive visualizations

## 📧 Contact

For questions or feedback, please open an issue on GitHub.

---

**Built with ❤️ for data transparency and trade analysis**