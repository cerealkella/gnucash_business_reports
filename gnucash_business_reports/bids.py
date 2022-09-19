from .builder import GnuCash_Data_Analysis

gda = GnuCash_Data_Analysis()


prices = gda.get_latest_commodity_bids()
# print(prices.loc[prices.index.max()][["fullname", "cash"]])

# prices = prices.groupby("fullname")["date", "cash"].max()

print(prices)

gda.year = 2021
balance_sheet = gda.get_balance_sheet()
# balance_sheet = balance_sheet[balance_sheet["post_date"].dt.year <= year]
print(balance_sheet)
# balance_sheet_totals = balance_sheet.groupby("balance_sheet_category").sum()
# balance_sheet_totals.loc["Grain"] = (balance_sheet_totals.loc["Stock"]["qty"] * 2, 0, 0)

"""
total_row.extend(column_filler(latex_df))
total_row.append(account_totals_dict[code])
latex_df.loc["2021-12-31"] = total_row
"""

# print(balance_sheet_totals)

# stock = gda.get_stock()
# print(stock.groupby("commodity_guid").sum())


# print(gda.get_commodity_prices())

# print(gda.get_depreciation_schedule())

# print(gda.get_farm_cash_transactions())

# print(gda.get_invoices())
