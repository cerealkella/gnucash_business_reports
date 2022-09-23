from .builder import GnuCash_Data_Analysis


gda = GnuCash_Data_Analysis()
gda.year = 2022

"""
tx_w_depr = gda.get_farm_cash_transactions(include_depreciation=True)
tx_w_depr.reset_index(inplace=True)
tx_w_depr.sort_values(by=["account_code", "post_date"], inplace=True)

tx_sum = tx_w_depr.groupby(
    [
        "account_type",
    ]
).sum()
print(tx_sum)
"""
# balance_sheet = gda.get_balance_sheet()
# balance_sheet = balance_sheet[balance_sheet["post_date"].dt.year <= year]
# print(balance_sheet)
# print(balance_sheet.groupby("balance_sheet_category").sum())

# prices = gda.get_commodity_prices()
# print(prices)

# all_accounts = gda.get_all_accounts()
# commodities = all_accounts["commodity_guid"].unique()
# print(commodities.tolist())
# gda.year = 2021
balance_sheet = gda.get_corporation_value()

print(balance_sheet)

gda.sanity_checker()
