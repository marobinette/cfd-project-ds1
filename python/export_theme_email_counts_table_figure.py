# -------------------------------------------------------------------
# Process to export a figure that is a table of email counts
# for all the themes we analyzed.
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
# Create pivot table
# -------------------------------
theme_pt_df = theme_table_df.copy()
# print(theme_pt_df)

# Map Category to different labels (with line breaks)
# mapping = {
#     "Topic\nused": "Topic\nused in\nnewsletter",
#     "Not\nused": "Topic\nnot used in\nnewsletter"
# }
# theme_pt_df["Category"] = theme_pt_df["Category"].map(mapping)

# Specify sorting
# pt_cat_order = ["Topic\nused in\nnewsletter", "Topic\nnot used in\nnewsletter"]
pt_party_order = ["Democrat", "Republican"]

# theme_pt_df["Category"] = pd.Categorical(theme_pt_df["Category"], categories=pt_cat_order, ordered=True)
theme_pt_df["party"] = pd.Categorical(theme_pt_df["party"], categories=pt_party_order, ordered=True)

# Pivot table
theme_pt = pd.pivot_table(
    theme_pt_df,
    index="topic",
    columns="party",
    values=["emails", "percent"],
    aggfunc={"emails": "sum", "percent": "mean"},
    fill_value=0,
    observed=True
)

# Set clip values for gradient
gradient_df_clipped = theme_pt.clip(upper=1, lower=0)
vmin = gradient_df_clipped['percent'].min()
vmax = gradient_df_clipped['percent'].max()

# Format cell contents to have both number and percent of emails
theme_formatted = pd.DataFrame(index=theme_pt.index)
for party in theme_pt.columns.levels[1]:
    emails = theme_pt["emails", party].astype(int)
    percent = theme_pt["percent",party]
    theme_formatted[party] = emails.astype(str) + " (" + percent.map(lambda x: f"{x:.1f}") + "%)"

print(theme_formatted)

# Wrap headers (keep line breaks)
# theme_pt_wrapped = theme_pt.copy()
# theme_pt_wrapped.columns = pd.MultiIndex.from_tuples([
#     (party, cat) for party, cat in theme_pt_wrapped.columns
# ])

# Remove index and column names
# theme_pt_wrapped.index.name = None
# theme_pt_wrapped.columns.names = [None]

# -------------------------------
# Style 
# -------------------------------
cmap = LinearSegmentedColormap.from_list("white_gray", ["#ffffff", "#b0b0b0"])

caption_size = "8pt"

theme_pt_styled = (
    theme_formatted.style
        # .format("{:.3f}")
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
        .set_caption("The more emails, the darker the shading.")
)

os.makedirs("../figures", exist_ok=True)

# Export to PNG 
dfi.export(theme_pt_styled, "../figures/theme_email_count_table_paper.png")
