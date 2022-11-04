from datetime import datetime

from jinja2 import Environment, FileSystemLoader

from .builder import GnuCash_Data_Analysis
from .config import get_config
from .helpers import column_filler, column_type_changer

gda = GnuCash_Data_Analysis()
gda.year = 2022
tx = gda.get_farm_cash_transactions(include_depreciation=True)


account_codes = tx["account_code"].unique()
tx["display"] = tx["account_name"] + " (" + tx["account_code"] + ")"
account_totals = tx.groupby(["account_code"]).sum()
account_names_df = tx.reset_index()
account_names_df.set_index(["account_code"], inplace=True)
account_names_df = account_names_df["display"]
account_names_df = account_names_df[~account_names_df.reset_index().duplicated().values]
account_totals_series = account_totals["amt"].squeeze()
account_totals_dict = account_totals_series.to_dict()
account_names_dict = account_names_df.to_dict()

latex = []
for code in account_codes:
    latex_df = tx[tx["account_code"] == code]  # .set_index("post_date")
    latex_df = latex_df[
        [
            "src_code",
            "post_date",
            "description",
            "memo",
            "amt",
        ]
    ]
    total_row = ["Total"]
    if len(latex_df.columns) > 2:
        total_row.extend(column_filler(latex_df))
    total_row.append(account_totals_dict[code])
    total_row[1] = datetime(gda.year, 12, 31)
    latex_df.loc[len(latex_df)] = total_row
    latex_df["post_date"] = latex_df["post_date"].astype("datetime64[ns]")
    latex_df = latex_df.rename(
        columns={
            "src_code": "Src",
            "post_date": "Date",
            "description": "Description",
            "memo": "Memo",
            "amt": "Amount",
        },
    ).sort_values(by=["Date", "Src"])
    # latex_df.style.format()
    latex_str = column_type_changer(
        latex_df,
        caption=account_names_dict[code],
    )
    latex.append(latex_str)

latex_dict = dict(zip(account_codes, latex))

file_loader = FileSystemLoader("templates")
env = Environment(
    block_start_string="\BLOCK{",
    block_end_string="}",
    variable_start_string="\VAR{",
    variable_end_string="}",
    comment_start_string="\#{",
    comment_end_string="}",
    line_statement_prefix="%%",
    line_comment_prefix="%#",
    trim_blocks=True,
    autoescape=False,
    loader=file_loader,
)
report_details = {
    "report_name": f"{gda.year} Transaction Detail Report",
    "organization_name": f"""{get_config()["Organization"]["business_name"]}""",
}
template = env.get_template("tabularray.tex")

exec_summary = column_type_changer(
    gda.get_executive_summary(),
    caption="Executive Summary",
)

acct_summary = column_type_changer(
    gda.get_summary_by_account(),
    caption="Account Totals",
)

output = template.render(
    exec_summary=exec_summary,
    acct_summary=acct_summary,
    data=report_details,
    data1=latex_dict,
    names=account_names_dict,
)
with open(f"export/{gda.year}-rendered.tex", "w") as f:
    f.write(output)
