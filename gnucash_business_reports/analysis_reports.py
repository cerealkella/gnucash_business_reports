from .builder import GnuCash_Data_Analysis, pd

gda = GnuCash_Data_Analysis()
gda.year = 2022

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


# Get Grain Invoices
grain_invoices = gda.get_grain_invoices()

crops = ["Corn", "Soybeans"]

writer = pd.ExcelWriter(f"export/analysis/{gda.year}-Grain.xlsx", engine="xlsxwriter")
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
