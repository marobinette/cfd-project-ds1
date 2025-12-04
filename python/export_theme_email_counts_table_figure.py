# -------------------------------------------------------------------
# Export a table of email counts with percent shading
# -------------------------------------------------------------------

import pandas as pd
import dataframe_image as dfi
from matplotlib.colors import LinearSegmentedColormap
import os

# -------------------------------
# Load CSV
# -------------------------------
theme_table_df = pd.read_csv("../data/dcInbox/data_for_emails_by_theme_table_figure.csv")

# -------------------------------
# Specify sorting
# -------------------------------
pt_party_order = ["Democrat", "Republican"]
theme_table_df["party"] = pd.Categorical(theme_table_df["party"], categories=pt_party_order, ordered=True)

# -------------------------------
# Pivot table
# -------------------------------
theme_pt = pd.pivot_table(
    theme_table_df,
    index="topic",
    columns="party",
    values=["emails", "percent"],
    aggfunc={"emails": "sum", "percent": "mean"},
    fill_value=0,
    observed=True
)

# -------------------------------
# Format cell strings: "emails (percent%)"
# -------------------------------
theme_formatted = pd.DataFrame(index=theme_pt.index)
for party in theme_pt.columns.levels[1]:
    emails = theme_pt["emails", party].astype(int)
    percent = theme_pt["percent", party]
    # theme_formatted[party] = emails.map(str) + " (" + percent.map(lambda x: f"{x:.1f}") + "%)"
    theme_formatted[party] = emails.map(lambda x: f"{x:,.0f}") + " (" + percent.map(lambda x: f"{x:.1f}") + "%)"

theme_formatted.index.name = ""

# -------------------------------
# Set cell shading
# -------------------------------
vmin, vmax = 0, 40
cmap = LinearSegmentedColormap.from_list("white_gray", ["#ffffff", "#b0b0b0"])

def percent_to_hex(val, vmin, vmax):
    val_clipped = max(min(val, vmax), vmin)
    rgba = cmap((val_clipped - vmin) / (vmax - vmin))
    return '#%02x%02x%02x' % tuple(int(255*x) for x in rgba[:3])

# Build a DataFrame of colors
colors_df = pd.DataFrame(index=theme_formatted.index, columns=theme_formatted.columns)
for col in theme_formatted.columns:
    for idx in theme_formatted.index:
        colors_df.at[idx, col] = f'background-color: {percent_to_hex(theme_pt["percent", col][idx], vmin, vmax)}'

# Apply the colors using Styler
theme_pt_styled = theme_formatted.style.apply(lambda _: colors_df, axis=None)

# Apply coloring per column
# for col in theme_formatted.columns:
#     print("vmin:", vmin)
#     print("vmax:", vmax)
#     theme_pt_styled = theme_pt_styled.apply(color_cells, axis=1, col_name=col, vmin=vmin, vmax=vmax)

# Add table styles and caption
caption_size = "8pt"
theme_pt_styled = theme_pt_styled.set_table_styles([
    {'selector': 'th.row_heading', 'props': [
        ('text-align', 'left'),
        ('border', '1px solid gray'),
        ('background-color', 'white'),
        ('font-size', '10pt')
    ]},
    {'selector': 'th.col_heading', 'props': [
        ('text-align', 'center'),
        ('white-space', 'pre-wrap'),
        ('border', '1px solid gray'),
        ('background-color', 'white'),
        ('font-size', '10pt')
    ]},
    {'selector': 'td', 'props': [
        ('text-align', 'center'),
        ('border', '1px solid gray'),
        ('font-size', '10pt')
    ]},
    {'selector': 'th.blank', 'props': [
        ('background-color', 'white'),
        ('border', '1px solid gray')
    ]},
    {'selector': 'caption', 'props': [
        ('caption-side', 'bottom'),
        ('font-size', caption_size),
        ('text-align', 'left'),
        ('margin-top', '8pt')  
    ]}
], overwrite=False).set_caption("The more emails, the darker the shading.")

# -------------------------------
# Export to PNG
# -------------------------------
os.makedirs("../figures", exist_ok=True)
dfi.export(theme_pt_styled, "../figures/theme_email_count_table_paper.png")
