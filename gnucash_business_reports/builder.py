import pandas as pd
import pd_db_wrangler
from tabulate import tabulate
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from dateutil.relativedelta import relativedelta
from uuid import uuid4
from .config import get_datadir, get_gnucash_file_path
from .helpers import get_keys, parse_toml


class GnuCash_Data_Analysis:
    def __init__(self, cached_mode=False):
        self.CACHED_MODE = cached_mode
        self.data_directory = get_datadir()
        # Set Reporting year constant
        self.year = 2022
        self.all_accounts = None
        self.cash_accounts = ["RECEIVABLE", "PAYABLE", "BANK", "CREDIT", "CASH"]
        # Suppress warnings, format numbers
        pd.options.mode.chained_assignment = None  # default='warn'
        pd.set_option("display.float_format", lambda x: "%.2f" % x)
        self.pdw = pd_db_wrangler.Pandas_DB_Wrangler()
        self.pdw.set_connection_string(get_gnucash_file_path(), db_type="sqlite")

    def get_all_accounts(self) -> pd.DataFrame:
        """
        Optionally Override cached mode

        True = use CSV files
        False = use live data from database
        """
        if self.all_accounts is None:
            if self.CACHED_MODE:
                all_accounts = pd.read_csv(
                    f"{self.data_directory}/ALL_ACCOUNTS.csv", dtype={"code": object}
                )
            else:
                all_accounts = self.pdw.df_fetch(
                    self.pdw.read_sql_file("sql/all_accounts.sql")
                )
                all_accounts.to_csv(
                    f"{self.data_directory}/ALL_ACCOUNTS.csv", index=False
                )

            all_account_types = all_accounts["account_type"].unique().tolist()
            id_name_dict = dict(zip(all_accounts["guid"], all_accounts["name"]))
            parent_dict = dict(zip(all_accounts["guid"], all_accounts["parent_guid"]))

            def find_parent(x) -> str:
                """Function to recursively determine parents for accounts"""
                value = parent_dict.get(x, None)
                if value is None:
                    return ""
                else:
                    # In case there is a id without name.
                    if id_name_dict.get(value, None) is None:
                        return "" + find_parent(value)
                    return str(id_name_dict.get(value)) + ">" + find_parent(value)

            def get_third_level(x) -> str:
                """For the Finpack report, we want the 3rd Level of acct categories"""
                parents = x.split(">")
                level_count = len(parents)
                if level_count > 2:
                    return str(parents[level_count - 3])
                else:
                    return ""

            all_accounts["parent_accounts"] = (
                all_accounts["guid"].apply(lambda x: find_parent(x)).str.rstrip(">")
            )
            all_accounts["finpack_account"] = all_accounts["parent_accounts"].apply(
                lambda x: get_third_level(x)
            )
            all_accounts.loc[
                all_accounts["finpack_account"] == "", "finpack_account"
            ] = all_accounts["name"]

            all_accounts.set_index("guid", drop=True, inplace=True)

            if not self.CACHED_MODE:
                # Create CSV with Parental Tree for reporting
                all_accounts.to_csv(f"{self.data_directory}/ALL_ACCOUNTS_W_PARENTS.csv")
                # Create CSV with Prices which is used for valuation
            self.all_accounts = all_accounts
        return self.all_accounts

    def get_commodity_prices(self) -> pd.DataFrame:
        if self.CACHED_MODE:
            query = "SELECT * FROM prices"
            prices = self.pdw.df_fetch(query, parse_dates=["date"])
            prices.to_csv(f"{self.data_directory}/PRICES.csv")
        else:
            prices = pd.read_csv(
                f"{self.data_directory}/PRICES.csv", parse_dates=["date"]
            )
        return prices

    def build_depreciation_dataframe(self) -> pd.DataFrame:
        """Builds depreciation schedule

        Returns:
            pd.DataFrame: dataframe containing depreciation entries
        """
        self.get_all_accounts()

        def get_depreciation_accounts() -> pd.DataFrame:
            depreciation_accounts = self.all_accounts[
                self.all_accounts["account_notes"]
                .str.startswith("[Depreciation]")
                .replace("\\n", "\n", regex=True)
                == True
            ]
            depr_keys = get_keys(
                depreciation_accounts["account_notes"][0], "Depreciation"
            )  # this assumes the first account will have all the keys we need
            # populate depreciation columns into dataframe using keys from get_keys
            for key in depr_keys:
                depreciation_accounts[key] = depreciation_accounts[
                    "account_notes"
                ].apply(lambda x: parse_toml(x, "Depreciation", key))
            return depreciation_accounts

        def build_depreciation_schedule(
            account, amount, sec_179, method, term, date_in_service
        ):
            basis = amount - sec_179
            depreciation_codes = []
            accounts = []
            dates = []
            amounts = []
            descriptions = []
            if method == "MO S/L":
                deduction_month_frequency = 6
                amount_per_term = basis / (term * 2)
            else:
                deduction_month_frequency = 12
                amount_per_term = round(basis / term, 2)
            amount_left = basis
            if sec_179 > 0:
                depreciation_codes.append("801")
                accounts.append(account)
                dates.append(date_in_service)
                amounts.append(sec_179 * -1)
                descriptions.append("Section 179")
            while amount_left > 0.5:
                if amount_left == basis:
                    dates.append(date_in_service)
                    new_date = date_in_service
                else:
                    new_date = new_date + relativedelta(
                        months=deduction_month_frequency
                    )
                    dates.append(new_date)
                depreciation_codes.append("800")
                accounts.append(account)
                descriptions.append(f"Regular Depreciation")
                amounts.append(amount_per_term * -1)
                amount_left -= amount_per_term
            depreciation_dict = {
                "depreciation_codes": depreciation_codes,
                "account_guid": accounts,
                "post_date": dates,
                "amt": amounts,
                "description": descriptions,
            }
            df = pd.DataFrame.from_dict(depreciation_dict)
            df["post_date"] = df["post_date"].astype("datetime64[ns]")
            return df

        def build_dataframe(df: pd.DataFrame()) -> pd.DataFrame:
            i = 0
            depreciation_schedule = pd.DataFrame()
            while i < len(df):
                depreciation_schedule = pd.concat(
                    [
                        depreciation_schedule,
                        build_depreciation_schedule(
                            df.index[i],
                            df["Cost"][i],
                            df["Sec_179"][i],
                            df["Method"][i],
                            df["Years"][i],
                            df["Date_in_Service"][i],
                        ),
                    ]
                )
                i += 1

            # depreciation_schedule.reset_index(drop=True, inplace=True)
            depreciation_schedule = depreciation_schedule.join(
                df, on="account_guid", rsuffix="_acct"
            )
            depreciation_schedule["account_code"] = "800"
            depreciation_schedule["account_name"] = "Depreciation"
            depreciation_schedule["account_type"] = "DEPRECIATION"
            depreciation_schedule["src_code"] = depreciation_schedule["code"]
            # create dummy data for guid values
            depreciation_schedule["src_guid"] = depreciation_schedule["account_guid"]
            depreciation_schedule["account_guid"] = uuid4().hex
            depreciation_schedule["src_type"] = depreciation_schedule["account_type"]
            depreciation_schedule["src_name"] = depreciation_schedule["name"]
            depreciation_schedule["currency_guid"] = ""
            depreciation_schedule["currency_guid"] = uuid4().hex
            depreciation_schedule["tx_num"] = str(depreciation_schedule.index)
            depreciation_schedule["tx_guid"] = ""
            depreciation_schedule["tx_guid"] = depreciation_schedule["tx_guid"].apply(
                lambda v: uuid4().hex
            )
            depreciation_schedule["split_action"] = "DEPR"
            depreciation_schedule["split_guid"] = ""
            depreciation_schedule["split_guid"] = depreciation_schedule[
                "split_guid"
            ].apply(lambda v: uuid4().hex)
            depreciation_schedule["enter_date"] = datetime.now().date
            depreciation_schedule["reconcile_date"] = datetime.now().date
            depreciation_schedule["reconcile_state"] = ""
            depreciation_schedule["memo"] = "Generated Dynamically"
            # depreciation_schedule["qty"] = 1.0
            depreciation_schedule["quantity"] = 1.0
            depreciation_schedule.rename(
                columns={
                    "description_acct": "account_desc",
                    # "name": "src_name",
                    # "code": "account_code",
                },
                inplace=True,
            )
            depreciation_df = depreciation_schedule[
                [
                    "account_guid",
                    "tx_guid",
                    "account_type",
                    "account_desc",
                    "parent_guid",
                    "commodity_guid",
                    "account_name",
                    "account_code",
                    "src_guid",
                    "src_code",
                    "src_type",
                    "src_name",
                    "currency_guid",
                    "tx_num",
                    "enter_date",
                    "description",
                    "post_date",
                    "split_action",
                    "split_guid",
                    "amt",
                    # "qty",
                    "reconcile_date",
                    "reconcile_state",
                    "memo",
                    "finpack_account",
                    "parent_accounts",
                    "quantity",
                ]
            ]
            depreciation_df["post_date"] = depreciation_df["post_date"].astype(
                "datetime64[ns]"
            )
            return depreciation_df.reset_index(
                drop=True
            )  # .set_index(["tx_guid", "account_guid"])

        return build_dataframe(get_depreciation_accounts())

    def get_depreciation_schedule(self, year: int = 0) -> pd.DataFrame:
        """returns depreciation schedule transactions

        Args:
            year (int, optional): if a year is specified, the function
            will filter on it. Defaults to 0.

        Returns:
            pd.DataFrame: dataframe containing depreciation transactions
        """
        df = self.build_depreciation_dataframe()
        if year > 0:
            self.year = year
            year_mask = df["post_date"].dt.year == self.year
            return df[year_mask]
        else:
            return df

    def get_guid_list(self, acct_types=[]):
        """2020-10-25 Refactored function to make it more generic"""
        self.get_all_accounts()  # ensure all_accounts df has been created
        filtered_accounts = self.all_accounts[
            self.all_accounts["account_type"].isin(acct_types)
        ]
        return filtered_accounts.index.tolist()

    def fetch_transactions(
        self, acct_types: list = [], inverse_multiplier: bool = True
    ) -> pd.DataFrame:
        def get_transactions_from_db(guids=[], inverse_multiplier=True) -> pd.DataFrame:
            """
            Pass a list of Account GUIDs and whether to pull by Source Accounts for cash accounting
            or main account for accrual
            When passing True, it will need to multiply the account totals by -1 to invert
            """
            if inverse_multiplier == True:
                multiplier = "* -1"
                inner_where = """
                where
                    accounts.guid in ('{}')
                """
                main_where = """ where a.guid not in ('{}')"""
            else:
                multiplier = "* 1"
                inner_where = """
                where
                    accounts.guid not in ('{}')
                """
                main_where = """ where a.guid in ('{}')"""

            """
            This gives us everything for each acct, not filtered by
            year, allowing us to get current balances for sanity checking
            """
            sql = self.pdw.read_sql_file("sql/transactions_master.sql")

            for guid in guids:
                query = sql.format(
                    inner_where.format(guid), main_where.format(guid), multiplier
                )
                tx = self.pdw.df_fetch(
                    query, parse_dates=["post_date", "enter_date", "reconcile_date"]
                )
                if guid == guids[0]:
                    all_tx = tx
                else:
                    all_tx = pd.concat([all_tx, tx])
            return all_tx

        if self.CACHED_MODE:
            for account in acct_types:
                csv_import = pd.read_csv(
                    f"{self.data_directory}/{account}.csv",
                    dtype={
                        "account_code": object,
                        "src_code": object,
                        "tx_num": object,
                    },
                    parse_dates=["post_date", "enter_date", "reconcile_date"],
                )
                if account == acct_types[0]:
                    tx = csv_import
                else:
                    tx = pd.concat([tx, csv_import])
        else:
            for account in acct_types:
                guids = self.get_guid_list([account])
                csv_export = get_transactions_from_db(guids, inverse_multiplier)
                csv_export.to_csv(f"{self.data_directory}/{account}.csv", index=False)
                if account == acct_types[0]:
                    tx = csv_export
                else:
                    tx = pd.concat([tx, csv_export])
            tx = tx.join(
                self.all_accounts[["finpack_account", "parent_accounts"]],
                on="account_guid",
            )
        invoices = self.get_invoices().groupby(["tx_guid", "account_guid"]).sum()
        tx.set_index(["tx_guid", "account_guid"], inplace=True)
        tx = tx.join(invoices[["quantity"]])
        tx["post_date"] = pd.to_datetime(tx["post_date"], yearfirst=True)
        return tx.fillna(0)

    def get_assets(self) -> pd.DataFrame:
        """calls fetch transactions with ASSETS as parameter

        Returns:
            pd.Dataframe: dataframe containing asset transactions
        """
        return self.fetch_transactions(["ASSET"], True)

    def get_cash(self) -> pd.DataFrame:
        """calls fetch transactions with BANK, CASH as parameter

        Returns:
            pd.Dataframe: dataframe containing cash transactions
        """
        return self.fetch_transactions(["BANK", "CASH"], True)

    def get_liabilities(self) -> pd.DataFrame:
        """calls fetch transactions with liability types as parameters

        Returns:
            pd.Dataframe: dataframe containing liability transactions
        """
        return self.fetch_transactions(["LIABILITY", "CREDIT", "PAYABLE"], True)

    def get_stock(self) -> pd.DataFrame:
        """calls fetch transactions with STOCK, False as parameter
        for farming operations this dataframe contains grain inventory
        qty (quantity) column is important for this calcualtion, as
        it is needed when calculating different commodity values

        Returns:
            pd.Dataframe: dataframe containing stock transactions
        """
        return self.fetch_transactions(["STOCK"], False)

    def get_balance_sheet(self) -> pd.DataFrame:
        assets = self.get_assets()
        assets["balance_sheet_category"] = "Assets"
        cash = self.get_cash()
        cash["balance_sheet_category"] = "Cash"
        liabilities = self.get_liabilities()
        liabilities["balance_sheet_category"] = "Liabilities"
        stock = self.get_stock()
        stock["balance_sheet_category"] = "Stock"

        return pd.concat(
            [
                assets,
                cash,
                liabilities,
                stock,
            ]
        )

    def get_all_cash_transactions(self, year: int = 0) -> pd.DataFrame:
        """calls fetch transactions passing the necessary account types
        to retrieve actual cash transactions throughout the accounting period

        Returns:
            pd.Dataframe: dataframe containing desired transactions
            sorted by post dates
        """
        return (
            self.fetch_transactions(self.cash_accounts, True)
            .reset_index()
            .sort_values(by=["post_date"])
        )

    def get_cleaned_cash_transactions(self, year: int = 0) -> pd.DataFrame:
        """calls fetch transactions passing the necessary account types
        to retrieve actual cash transactions throughout the accounting period
        removes account to account entries (transfers) and payments on
        payables and receivables

        Returns:
            pd.Dataframe: dataframe containing desired transactions
        """
        tx = self.get_all_cash_transactions(year).drop(columns="qty")

        # Remove the acct-to-acct entries (e.g. AP to Checking, etc)
        guid_mask = (
            tx["account_guid"].isin(self.get_guid_list(self.cash_accounts)) == False
        )
        # Remove Payment entries for Bills/Invoices.
        action_mask = tx["split_action"].isin(["Payment"]) == False

        # Filter to transactions from a given year if provided
        if year > 0:
            self.year = year
            year_mask = tx["post_date"].dt.year == self.year
            return tx[year_mask & guid_mask & action_mask]
        else:
            return tx[guid_mask & action_mask]

    def get_farm_cash_transactions(self, year: int = 0) -> pd.DataFrame:
        """_summary_

        Args:
            year (int, optional): User-provided year filter. Defaults to 0.

        Returns:
            pd.DataFrame: DataFrame containing farm expenses
        """

        def filter_and_reclassify_farm_transactions(tx: pd.DataFrame) -> pd.DataFrame:
            """Postprocesses the Transactions dataframe for farm operations
            Args:
                tx (pd.DataFrame): dataframe containing unfiltered transactions

            Returns:
                pd.DataFrame: dataframe containing filtered transactions
            """

            # Remove Harvest Income, which gets posted to inventory from AR
            # These accounts aren't actually cash entries
            # Order matters here! Need to do this before reclassification
            account_mask = tx["account_code"].isin(["301c", "303b"]) == False
            # Apply Mask
            tx = tx[account_mask]

            # Income accounts filtered out, now re-classify inventory accounts
            # as income
            tx.loc[tx["account_code"].str.match("133|134"), "account_type"] = "INCOME"
            tx.loc[tx["account_code"].str.startswith("133"), "account_code"] = "301c"
            tx.loc[tx["account_code"].str.startswith("133"), "account_name"] = "Corn"
            tx.loc[tx["account_code"].str.startswith("134"), "account_code"] = "303b"

            # Classify Prepaids as Expense
            tx.loc[tx["account_code"].str.match("146"), "account_type"] = "EXPENSE"

            # Classify Non-Taxable Expenses
            tx.loc[
                tx["account_code"].str.startswith("9"), "account_type"
            ] = "NF EXPENSE"

            # Enterprise Column
            tx["crop"] = "General"
            tx.loc[tx["parent_accounts"].str.contains("Soybeans"), "crop"] = "Soybeans"
            tx.loc[tx["parent_accounts"].str.contains("Corn"), "crop"] = "Corn"
            tx.loc[tx["account_name"].str.contains("Soybeans"), "crop"] = "Soybeans"
            tx.loc[tx["account_name"].str.contains("Corn"), "crop"] = "Corn"

            return tx.sort_values(by=["account_code", "post_date"])

        return filter_and_reclassify_farm_transactions(
            self.get_cleaned_cash_transactions(year)
        )

    def get_invoices(self):
        # bring in invoices for quantities
        invoices_sql = self.pdw.read_sql_file("sql/invoices_master.sql")
        invoices = self.pdw.df_fetch(invoices_sql)
        return invoices.rename(columns={"post_txn": "tx_guid"})


year = 2022
gda = GnuCash_Data_Analysis()
depr = gda.get_depreciation_schedule(year)
# acct_types = ["RECEIVABLE", "PAYABLE", "BANK", "CREDIT", "CASH"]
# cash_accounts = gda.fetch_transactions(acct_types)
# print(cash_accounts)
# print(gda.get_stock())
all_tx = gda.get_all_cash_transactions(year)
tx = gda.get_farm_cash_transactions(year)
tx_w_depr = pd.concat([tx, depr])
# tx_w_depr.reset_index(inplace=True)
# tx_w_depr.sort_values(by=["account_code", "post_date"], inplace=True)
tx_sum = tx_w_depr.groupby(
    [
        "account_type",
    ]
).sum()
print(tx_sum)
"""
print(depr)
print(tx)
"""
balance_sheet = gda.get_balance_sheet()
balance_sheet = balance_sheet[balance_sheet["post_date"].dt.year <= year]
print(balance_sheet)
print(balance_sheet.groupby("balance_sheet_category").sum())
