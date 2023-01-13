from datetime import datetime

from jinja2 import Environment, FileSystemLoader

from .builder import GnuCash_Data_Analysis, pd
from .config import get_config
from .helpers import column_filler, column_type_changer


def build_report(gda):
    tx = gda.get_farm_cash_transactions(include_depreciation=False)
    account_codes = tx["account_code"].unique()
    tx["display"] = tx["account_name"] + " (" + tx["account_code"] + ")"
    account_totals = tx.groupby(["account_code"]).sum(numeric_only=True)
    account_names_df = tx.reset_index()
    account_names_df.set_index(["account_code"], inplace=True)
    account_names_df = account_names_df["display"]
    account_names_df = account_names_df[
        ~account_names_df.reset_index().duplicated().values
    ]
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
        gda.get_executive_summary(include_depreciation=False),
        caption="Executive Summary",
    )

    acct_summary = column_type_changer(
        gda.get_summary_by_account(include_depreciation=False),
        caption="Account Totals",
    )

    output = template.render(
        exec_summary=exec_summary,
        acct_summary=acct_summary,
        data=report_details,
        data1=latex_dict,
        names=account_names_dict,
    )
    with open(f"export/{gda.year}-Detail_Report.tex", "w") as f:
        f.write(output)


def production_data(gda):
    # Get Production Data and export it into a spreadsheet
    production = gda.get_production(include_operation_id=False).reset_index()
    writer = pd.ExcelWriter(
        f"export/analysis/{gda.year}-Production.xlsx", engine="xlsxwriter"
    )
    production.to_excel(writer, index=False, sheet_name="KFF")
    workbook = writer.book
    sheet = writer.sheets["KFF"]
    fmt_header = workbook.add_format(
        {
            "bold": True,
            "text_wrap": True,
            "valign": "top",
            "fg_color": "#5DADE2",
            "font_color": "#FFFFFF",
            "border": 1,
        }
    )

    for col, value in enumerate(production.columns.values):
        sheet.write(0, col, value, fmt_header)
    writer.close()


def grain_invoices(gda):
    # Get Grain Invoices
    grain_invoices = gda.get_grain_invoices()
    crops = ["Corn", "Soybeans"]
    writer = pd.ExcelWriter(
        f"export/analysis/{gda.year}-Grain.xlsx", engine="xlsxwriter"
    )
    for crop in crops:
        df = grain_invoices[grain_invoices["Crop"].str.match(crop)]
        df.to_excel(writer, index=False, sheet_name=crop)
        workbook = writer.book
        sheet = writer.sheets[crop]
        fmt_header = workbook.add_format(
            {
                "bold": True,
                "text_wrap": True,
                "valign": "top",
                "fg_color": "#5DADE2",
                "font_color": "#FFFFFF",
                "border": 1,
            }
        )
        for col, value in enumerate(df.columns.values):
            sheet.write(0, col, value, fmt_header)
        fmt_currency = workbook.add_format({"num_format": "$#,##0.00", "bold": False})
        sheet.set_column("F:G", 10, fmt_currency)

    writer.close()


gda = GnuCash_Data_Analysis()
gda.year = 2022

build_report(gda)
production_data(gda)
grain_invoices(gda)
gda.sanity_checker()
