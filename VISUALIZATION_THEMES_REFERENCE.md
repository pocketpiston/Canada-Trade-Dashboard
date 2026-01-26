# Visualization Themes & Packages Reference

## Plotly Theme & Color Resources

### 1. Plotly Built-in Themes
- **Reference:** [Plotly Templates Documentation](https://plotly.com/python/templates/)
- **Built-in themes:** `plotly`, `plotly_white`, `plotly_dark`, `ggplot2`, `seaborn`, `simple_white`, `none`
- **Usage:**
  ```python
  fig.update_layout(template="plotly_dark")
  ```

### 2. Plotly Color Scales
- **Reference:** [Plotly Colorscales](https://plotly.com/python/builtin-colorscales/)
- **Categories:**
  - **Sequential:** `Blues`, `Greens`, `Reds`, `Viridis`, `Plasma`, `Inferno`, `Magma`
  - **Diverging:** `RdBu`, `RdYlGn`, `Spectral`, `Picnic`
  - **Cyclical:** `IceFire`, `Edge`, `Phase`
  - **Qualitative:** `Plotly`, `D3`, `G10`, `T10`, `Alphabet`

### 3. ColorBrewer (Scientific Color Palettes)
- **Reference:** [ColorBrewer2.org](https://colorbrewer2.org/)
- **Plotly Integration:** All ColorBrewer palettes available in Plotly
- **Categories:** Sequential, Diverging, Qualitative
- **Colorblind-safe options** clearly marked
- **Usage:** Palettes like `YlOrRd`, `PuBuGn`, `RdYlBu` are available directly in Plotly

### 4. Seaborn Color Palettes
- **Reference:** [Seaborn Color Palettes](https://seaborn.pydata.org/tutorial/color_palettes.html)
- **Can be used with Plotly:**
  ```python
  import seaborn as sns
  colors = sns.color_palette("husl", 10).as_hex()
  fig.update_traces(marker=dict(color=colors))
  ```

---

## Alternative Visualization Libraries

### 5. Altair (Declarative Visualization)
- **Reference:** [Altair Documentation](https://altair-viz.github.io/)
- **GitHub:** [altair-viz/altair](https://github.com/altair-viz/altair)
- **Pros:** Declarative syntax, excellent for exploratory analysis, grammar of graphics
- **Built-in Themes:** `dark`, `latimes`, `fivethirtyeight`, `ggplot2`, `googlecharts`, `powerbi`, `quartz`, `vox`
- **Installation:** `pip install altair`
- **Streamlit Support:** Native support via `st.altair_chart()`

### 6. Bokeh (Interactive Visualizations)
- **Reference:** [Bokeh Documentation](https://docs.bokeh.org/)
- **GitHub:** [bokeh/bokeh](https://github.com/bokeh/bokeh)
- **Pros:** High-performance interactivity, server-side rendering, real-time streaming
- **Themes:** Customizable via CSS and built-in themes
- **Installation:** `pip install bokeh`
- **Streamlit Support:** Via `st.bokeh_chart()`

### 7. Streamlit Theme System
- **Reference:** [Streamlit Theming](https://docs.streamlit.io/develop/concepts/configuration/theming)
- **Config file:** `.streamlit/config.toml`
- **Example:**
  ```toml
  [theme]
  primaryColor = "#FF4B4B"
  backgroundColor = "#0E1117"
  secondaryBackgroundColor = "#262730"
  textColor = "#FAFAFA"
  font = "sans serif"
  ```

---

## Color Palette Generators & Tools

### 8. Coolors.co
- **Reference:** [Coolors Palette Generator](https://coolors.co/)
- **Features:** 
  - Generate color palettes with spacebar
  - Explore trending palettes
  - Export as arrays, CSS, SCSS, SVG
  - Accessibility checker
- **Use Case:** Quick palette generation for custom themes

### 9. Adobe Color
- **Reference:** [Adobe Color](https://color.adobe.com/)
- **Features:**
  - Color wheel with harmony rules (complementary, triadic, etc.)
  - Accessibility tools (contrast checker)
  - Explore trending color themes
  - Extract palettes from images
- **Use Case:** Professional color scheme design

### 10. Viz Palette
- **Reference:** [Viz Palette](https://projects.susielu.com/viz-palette)
- **Features:**
  - Test palettes for colorblind accessibility
  - Simulators: Protanopia, Deuteranopia, Tritanopia, Grayscale
  - Compare multiple palettes side-by-side
- **Use Case:** Ensuring accessibility compliance

### 11. Chroma.js Color Palette Helper
- **Reference:** [Chroma.js Palette Helper](https://gka.github.io/palettes/)
- **Features:**
  - Generate color scales with different interpolation modes
  - Bezier interpolation for smooth gradients
  - Export as hex arrays
- **Use Case:** Creating custom sequential color scales

### 12. ColorSpace
- **Reference:** [ColorSpace](https://mycolor.space/)
- **Features:**
  - Generate gradients and palettes from a single color
  - Multiple palette types (gradient, classy, matching, etc.)
  - CSS code export
- **Use Case:** Building cohesive color schemes from brand colors

---

## Current Dashboard Themes

### Classic (Default)
```python
'Classic': {
    'destinations': 'Reds',
    'provinces': 'Blues', 
    'breakdown': 'Teal',
    'chapters_treemap': 'RdYlGn',
    'headings_treemap': 'Blues',
    'chapters_bar': 'Greens',
    'treemap_text_color': 'black',
    'treemap_text_size': 14
}
```

### High Contrast
```python
'High Contrast': {
    'destinations': 'Oranges',
    'provinces': 'Purples',
    'breakdown': 'YlOrBr',
    'chapters_treemap': [[0, '#f7fbff'], [0.5, '#6baed6'], [1, '#08306b']],
    'headings_treemap': [[0, '#fff5eb'], [0.5, '#fd8d3c'], [1, '#7f2704']],
    'chapters_bar': 'BuGn',
    'treemap_text_color': 'black',
    'treemap_text_size': 15
}
```

### Dark Mode
```python
'Dark Mode': {
    'destinations': [[0, '#2d1b2e'], [0.5, '#8b4789'], [1, '#e8b4e5']],
    'provinces': [[0, '#1a2332'], [0.5, '#4a7ba7'], [1, '#a8d5ff']],
    'breakdown': [[0, '#1e3a2f'], [0.5, '#4a9b7f'], [1, '#a8e6cf']],
    'chapters_treemap': [[0, '#1a1a2e'], [0.5, '#6a5acd'], [1, '#dda0dd']],
    'headings_treemap': [[0, '#0f2027'], [0.5, '#2c5364'], [1, '#7dd3c0']],
    'chapters_bar': [[0, '#1e3d2f'], [0.5, '#5a9367'], [1, '#b8e6c9']],
    'treemap_text_color': 'white',
    'treemap_text_size': 14
}
```

### Colorblind Friendly
```python
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
```

---

## Recommended Reading

### Color Theory for Data Visualization
- [Datawrapper Color Guide](https://blog.datawrapper.de/colorguide/)
- [NASA's Guide to Color Usage](https://earthobservatory.nasa.gov/blogs/elegantfigures/2013/08/05/subtleties-of-color-part-1-of-6/)
- [Eager Eyes: Rainbow Color Map](https://eagereyes.org/basics/rainbow-color-map)

### Accessibility
- [WCAG Color Contrast Guidelines](https://www.w3.org/WAI/WCAG21/Understanding/contrast-minimum.html)
- [Colorblind Web Page Filter](https://www.toptal.com/designers/colorfilter)
- [A11y Color Palette](https://a11y-101.com/design/color-palette)

### Plotly-Specific Resources
- [Plotly Community Forum](https://community.plotly.com/)
- [Plotly Figure Reference](https://plotly.com/python/reference/)
- [Plotly Dash Design Kit](https://plotly.com/dash/design-kit/)
