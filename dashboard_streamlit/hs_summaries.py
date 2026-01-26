"""
HS Code Summaries and Category Mappings

This module contains reference data for Harmonized System (HS) codes:
- 4-word summaries for all 99 HS chapters
- Category groupings for visualization
- Helper functions for category assignment
"""

# HS Chapter Summaries (4-word descriptions for cleaner display)
HS_CHAPTER_SUMMARIES = {
    '01': 'Live Animals', '02': 'Meat & Offal Products', '03': 'Fish & Seafood Products',
    '04': 'Dairy Eggs & Honey', '05': 'Other Animal Products', '06': 'Plants Flowers & Trees',
    '07': 'Vegetables Roots & Tubers', '08': 'Fruits Nuts & Citrus', '09': 'Coffee Tea & Spices',
    '10': 'Grains & Cereals', '11': 'Milled Grain Products', '12': 'Seeds Oils & Plants',
    '13': 'Gums Resins & Extracts', '14': 'Other Vegetable Products', '15': 'Fats Oils & Waxes',
    '16': 'Prepared Meat & Seafood', '17': 'Sugar & Confectionery', '18': 'Cocoa & Chocolate',
    '19': 'Baked Goods & Cereals', '20': 'Preserved Fruits & Vegetables', '21': 'Misc Food Preparations',
    '22': 'Beverages & Spirits', '23': 'Animal Feed & Waste', '24': 'Tobacco & Substitutes',
    '25': 'Minerals Salt & Cement', '26': 'Ores Slag & Ash', '27': 'Fuels Oils & Gas',
    '28': 'Inorganic Chemical Compounds', '29': 'Organic Chemicals', '30': 'Pharmaceuticals & Medicines',
    '31': 'Fertilizers', '32': 'Dyes, Paints & Inks', '33': 'Perfumes & Cosmetics',
    '34': 'Soaps Cleaners & Waxes', '35': 'Proteins Starches & Glues', '36': 'Explosives & Pyrotechnics',
    '37': 'Photography & Film Supplies', '38': 'Misc Chemical Products', '39': 'Plastics & Polymers',
    '40': 'Rubber & Products', '41': 'Hides Skins & Leather', '42': 'Leather Goods & Bags',
    '43': 'Furs & Fur Products', '44': 'Wood & Wood Products', '45': 'Cork & Cork Products',
    '46': 'Baskets & Woven Goods', '47': 'Wood Pulp & Waste', '48': 'Paper & Paperboard',
    '49': 'Books Prints & Publications', '50': 'Silk & Silk Fabrics', '51': 'Wool & Animal Hair',
    '52': 'Cotton & Cotton Fabrics', '53': 'Vegetable Textile Fibers', '54': 'Synthetic Filament Fabrics',
    '55': 'Synthetic Staple Fibers', '56': 'Wadding Felt & Ropes', '57': 'Carpets & Floor Coverings',
    '58': 'Special Woven Fabrics', '59': 'Industrial Textile Fabrics', '60': 'Knitted & Crocheted Fabrics',
    '61': 'Knitted Clothing & Accessories', '62': 'Woven Clothing & Accessories', '63': 'Textile Articles & Rags',
    '64': 'Footwear & Parts', '65': 'Hats Caps & Headgear', '66': 'Umbrellas Canes & Accessories',
    '67': 'Feathers Flowers & Hair', '68': 'Stone Cement & Plaster', '69': 'Ceramics & Pottery',
    '70': 'Glass & Glassware', '71': 'Gems Jewelry & Coins', '72': 'Iron & Steel Products',
    '73': 'Iron Steel Articles', '74': 'Copper & Copper Products', '75': 'Nickel & Nickel Products',
    '76': 'Aluminum & Aluminum Products', '77': 'Reserved Chapter', '78': 'Lead & Lead Products',
    '79': 'Zinc & Zinc Products', '80': 'Tin & Tin Products', '81': 'Other Base Metals',
    '82': 'Tools Cutlery & Implements', '83': 'Misc Metal Articles', '84': 'Machinery & Mechanical Equipment',
    '85': 'Electrical Equipment & Electronics', '86': 'Railway Equipment & Parts', '87': 'Vehicles & Auto Parts',
    '88': 'Aircraft & Spacecraft', '89': 'Ships Boats & Vessels', '90': 'Optical Medical & Precision',
    '91': 'Clocks Watches & Parts', '92': 'Musical Instruments & Parts', '93': 'Arms Ammunition & Parts',
    '94': 'Furniture Lighting & Fixtures', '95': 'Toys Games & Sports', '96': 'Misc Manufactured Articles',
    '97': 'Art Antiques & Collectibles', '98': 'Special Trade Classifications', '99': 'Special Import Export'
}

# Category Groupings (for optional hierarchical visualization)
CATEGORY_MAP = {
    '01-24': ('🌱 Agriculture & Food', '#90EE90'),
    '25-27': ('⛏️ Minerals & Fuels', '#D2691E'),
    '28-38': ('🧪 Chemicals & Pharma', '#87CEEB'),
    '39-40': ('🏭 Plastics & Rubber', '#DDA0DD'),
    '41-43': ('🐄 Leather & Furs', '#F4A460'),
    '44-49': ('🌲 Wood & Paper', '#8B4513'),
    '50-63': ('🧵 Textiles & Clothing', '#FFB6C1'),
    '64-67': ('👞 Accessories', '#FFA07A'),
    '68-71': ('🏺 Stone & Glass', '#B0C4DE'),
    '72-83': ('⚙️ Metals', '#C0C0C0'),
    '84-85': ('🔧 Machinery', '#4682B4'),
    '86-89': ('🚗 Transportation', '#FF6347'),
    '90-92': ('🔬 Precision Instruments', '#9370DB'),
    '93-97': ('🏠 Consumer Goods', '#FFD700'),
    '98-99': ('📋 Special Provisions', '#808080')
}


def get_chapter_summary(chapter_code):
    """
    Get the 4-word summary for a chapter code.
    
    Args:
        chapter_code (str): 2-digit HS chapter code (e.g., '01', '84')
    
    Returns:
        str: 4-word summary, or the code itself if not found
    """
    return HS_CHAPTER_SUMMARIES.get(str(chapter_code), str(chapter_code))


def get_category(chapter_code):
    """
    Get the category grouping for a chapter code.
    
    Args:
        chapter_code (str): 2-digit HS chapter code
    
    Returns:
        tuple: (category_name, color_hex) or ('Other', '#CCCCCC') if not found
    """
    try:
        code_num = int(chapter_code)
        for range_key, (category, color) in CATEGORY_MAP.items():
            start, end = map(int, range_key.split('-'))
            if start <= code_num <= end:
                return category, color
    except (ValueError, AttributeError):
        pass
    return 'Other', '#CCCCCC'


def get_category_name(chapter_code):
    """
    Get just the category name for a chapter code.
    
    Args:
        chapter_code (str): 2-digit HS chapter code
    
    Returns:
        str: Category name
    """
    return get_category(chapter_code)[0]


def get_category_color(chapter_code):
    """
    Get just the category color for a chapter code.
    
    Args:
        chapter_code (str): 2-digit HS chapter code
    
    Returns:
        str: Hex color code
    """
    return get_category(chapter_code)[1]
