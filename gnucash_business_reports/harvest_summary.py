from gnucash_business_reports.builder import GnuCash_Data_Analysis
import numpy as np
from great_tables import GT, md
import polars as pl
import polars.selectors as cs
import pandas as pd
from pathlib import Path


gda = GnuCash_Data_Analysis()
gda.year = 2024


latest_tx = gda.get_all_transactions().groupby(["account_desc"])["post_date"].max() #.reset_index().set_index("account_desc")
latest = latest_tx.loc[latest_tx.index.str.match("Delivered")]
last_delivery = latest.max()

grain = (
            gda.get_commodity_stock_values(["account_name", "account_desc", "commodity_guid"])
        ).reset_index().set_index(["account_desc"])
grain["abs_qty"] = abs(grain["qty"])
grain = grain.join(latest_tx)


df = pd.pivot_table(grain, values="abs_qty", index="account_name", columns="account_desc").fillna(0)
df["Contracted"] = df["Contracted Corn"] + df["Contracted Soybeans"]
df["Delivered"] = df["Delivered Corn"] + df["Delivered Soybeans"]
df["Harvested"] = df["Harvested Corn"] + df["Harvested Soybeans"]
df = df[["Contracted", "Delivered", "Harvested"]]
df["Total"] = df["Delivered"] + df["Harvested"]
df["pct"] = df["Delivered"] / df["Contracted"]

def create_bar(prop_fill: float, max_width: int, height: int) -> str:
    """Create divs to represent prop_fill as a bar."""
    if prop_fill > 1:
        prop_fill = 1
    width = round(max_width * prop_fill, 2)
    px_width = f"{width}px"
    return f"""\
    <div style="width: {max_width}px; background-color: lightgrey;">\
        <div style="height:{height}px;width:{px_width};background-color:green;"></div>\
    </div>\
    """


polar_df = pl.from_pandas(df.reset_index())
zoom_level = 400
res = (
    polar_df.with_columns(
        (pl.col("Delivered") / pl.col("Contracted")).alias("raw_perc"),
        (pl.col("account_name").str.to_lowercase() + ".png").alias("icon"),
    )
    .head(9)
    .with_columns(
        pl.col("raw_perc")
          .map_elements(lambda x: create_bar(x, max_width=75*(zoom_level/100), height=20*(zoom_level/100)))
          .alias("Progress")
    )
    .select("icon", "Contracted", "Delivered", "Harvested", "Total", "Progress")
)
table = (
    GT(res, rowname_col="icon")
    .tab_header(title=f"{gda.year} Harvest",
                subtitle="Progress towards filling contracts"
                )
    .tab_stubhead(label="Crop")
    # .tab_spanner("Earnings", cs.contains("Earnings"))
    .fmt_number(["Contracted", "Delivered", "Harvested", "Total"], decimals=0)
    .tab_options(table_font_size=f"{zoom_level / 10}px",# f"{zoom_level}%",
                 column_labels_padding_horizontal=f"{(zoom_level / 10) / 2}px",
                 data_row_padding_horizontal=f"{(zoom_level / 10) / 2}px",
                 )
    .fmt_image("icon", path="./img/")
    .tab_source_note(
        md(
            '<br><div style="text-align: center;">'
            "GNUCash Accounting"
            f" | Last Recorded Delivery: {last_delivery.strftime('%Y-%m-%d')}"
            "</div>"
            "<br>"
        )
    )
)

html = table.as_raw_html()
path = Path(f"{gda.get_config()["Paths"]["html"]}/grain_table.html")
path.write_text(html, encoding="utf-8")
