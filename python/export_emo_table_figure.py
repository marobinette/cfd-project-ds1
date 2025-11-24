# -------------------------------------------------------------------
# Process to export a figure that is a table of all the fundraising
# scores for all the emotions we analyzed.
# -------------------------------------------------------------------

import pandas as pd
import dataframe_image as dfi
from matplotlib.colors import LinearSegmentedColormap
import os

# -------------------------------
# Load CSV
# -------------------------------
emo_table_df = pd.read_csv("../data/matched/data_for_emotion_table_figure.csv")

# -------------------------------
# Create pivot table for EMOTION
# -------------------------------
emo_pt_df = emo_table_df.copy()

# Map Category to different labels (with line breaks)
mapping = {
    "Low": "Low\n(Less than 0.5)",
    "High": "High\n(0.5 or More)"
}
emo_pt_df["Category"] = (
    emo_pt_df["Category"].map(mapping)
)

# Specify sorting
pt_cat_order = ["Low\n(Less than 0.5)", "High\n(0.5 or More)"]
pt_party_order = ["Overall", "Democrats", "Republicans"]
# Map Category sorting
emo_pt_df["Category"] = pd.Categorical(
    emo_pt_df["Category"],
    categories=pt_cat_order,
    ordered=True,
)
emo_pt_df["Party"] = pd.Categorical(
    emo_pt_df["Party"],
    categories=pt_party_order,
    ordered=True,
)

# Pivot table
emo_pt = pd.pivot_table(
    emo_pt_df,
    index="Breakdown",
    columns=["Party", "Category"],
    values="Score",
    aggfunc="mean",
    fill_value=0,
    observed=True # only include combos present in the data
)

# Wrap headers (keep line breaks)
emo_pt_wrapped = emo_pt.copy()
emo_pt_wrapped.columns = pd.MultiIndex.from_tuples([
    (party, cat)
    for party, cat in emo_pt_wrapped.columns
])

# Remove index and column names
emo_pt_wrapped.index.name = None
emo_pt_wrapped.columns.names = [None, None]

# -------------------------------
# Style the table
# -------------------------------
cmap = LinearSegmentedColormap.from_list("white_gray", ["#ffffff", "#b0b0b0"])  

# Clip values for gradient
gradient_df = emo_pt_wrapped.copy()
gradient_df_clipped = gradient_df.clip(upper=2.1, lower=1.7)
# gradient_df_clipped = gradient_df.clip(lower=.5)

# Find the min/max across the entire table for setting the gradient
vmin = gradient_df_clipped.min().min()
vmax = gradient_df_clipped.max().max()

caption_size = "8pt"

emo_pt_styled = (
    emo_pt_wrapped.style
        .format("{:.3f}")
        .background_gradient(cmap=cmap, vmin=vmin, vmax=vmax)
        .set_table_styles([
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
        ], overwrite=False)
        .set_caption("The greater the fundraising score, the darker the shading.")
)

os.makedirs("../figures", exist_ok=True)

# Export to PNG 
dfi.export(emo_pt_styled, "../figures/emotion_table_paper.png")
