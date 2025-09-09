from datetime import datetime
from pathlib import Path
from typing import Callable
from uuid import uuid4

import pandas as pd
import pd_db_wrangler
from dateutil.relativedelta import relativedelta

from .config import (
    get_config,
    get_datadir,
    get_gnucash_file_path,
    get_excel_formatting,
)
from .helpers import get_keys, nearest, parse_toml
from .logger import log


class GnuCash_Data_Analysis:
    def __init__(self):
        self.data_directory = get_datadir()
        # Set Reporting year constant
        self.year = datetime.now().year  # defaults to current year
        self.all_accounts = None
        self.cash_accounts = ["RECEIVABLE", "PAYABLE", "BANK", "CREDIT", "CASH"]
        self.excel_formatting = get_excel_formatting()
        # Suppress warnings, format numbers
        pd.options.mode.chained_assignment = None  # default='warn'
        pd.set_option("display.float_format", lambda x: "%.2f" % x)
        self.date_format = "%Y-%m-%d %H:%M:%S"
        self.pdw = pd_db_wrangler.Pandas_DB_Wrangler(get_gnucash_file_path())
        # Unix/Mac - 4 initial slashes in total
        self.engine = self.pdw.engine

    def filter_by_year(
        self, df: pd.DataFrame, column: str, all_years_plus_specified: bool = False
    ) -> pd.DataFrame:
        """Helper function for passing in dataframes and filtering by year

        Args:
            df (pd.DataFrame): dataframe to filter
            column (str): year column in dataframe
            all_years_plus_specified (bool, optional): whether to be all inclusive
            leading up to the specified year or just filter entries on the
            year itself. Defaults to False.

        Returns:
            pd.DataFrame: dataframe filtered by year. If year = 0, it will pass
            back the original dataframe
        """
        if self.year > 0:
            if all_years_plus_specified:
                return df[df[column].dt.year <= self.year]
            else:
                return df[df[column].dt.year == self.year]
        else:
            return df

    def db_locked(self) -> bool:
        """Checks to see if database is in use prior to performing
            any write operations
        Returns:
            bool: True if locked, False if not locked.
        """
        df_lock = self.pdw.df_fetch("SELECT * FROM gnclock")
        if len(df_lock) > 0:
            log.warning("DATABASE IS LOCKED BY %s", df_lock["Hostname"])
            return True
        else:
            return False

    def add_descriptor_column(
        self,
        df: pd.DataFrame,
        column_to_match: str,
        initial_value: str = None,
        strings_to_match: list = ["Corn", "Soybeans"],
        new_column: str = "crop",
    ) -> pd.DataFrame:
        """Adds new column to dataframe
        the primary purpose this function was created is to add
        a crop column to a dataframe using an account heirarchy
        column
        """

        if initial_value is not None:
            df[new_column] = initial_value
        for match in strings_to_match:
            df.loc[df[column_to_match].str.contains(match), new_column] = match
        return df

    def get_all_accounts(self) -> pd.DataFrame:
        """Get all accounts from the database"""
        if self.all_accounts is None:
            df = self.pdw.df_fetch(
                self.pdw.read_sql_file("sql/all_accounts.sql"),
            )
            log.info(df.dtypes)
            df.to_csv(f"{self.data_directory}/ALL_ACCOUNTS.csv", index=False)

            # all_account_types = df["account_type"].unique().tolist()
            id_name_dict = dict(zip(df["guid"], df["name"]))
            parent_dict = dict(zip(df["guid"], df["parent_guid"]))

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

            def get_level(x: str, level: int) -> str:
                """
                Pass in the string to split (acct heirarchy) and the level
                we're looking for.
                For the Finpack report, we want the 4th Level of acct categories
                """
                parents = x.split(">")
                level_count = len(parents)
                if level_count > level - 1:
                    return str(parents[level_count - level])
                else:
                    return ""

            df["parent_accounts"] = (
                df["guid"].apply(lambda x: find_parent(x)).str.rstrip(">")
            )
            df["finpack_account"] = df["parent_accounts"].apply(
                lambda x: get_level(x, 4)
            )
            df.loc[df["finpack_account"] == "", "finpack_account"] = df["name"]

            df.set_index("guid", drop=True, inplace=True)

            df = self.add_descriptor_column(
                df, "parent_accounts", initial_value="General"
            )
            df = self.add_descriptor_column(df, "name")

            # Create CSV with Parental Tree for reporting
            df.to_csv(f"{self.data_directory}/ALL_ACCOUNTS_W_PARENTS.csv")
            self.all_accounts = df
        return self.all_accounts

    def toml_to_df(self, toml_series: pd.Series, str_to_match: str) -> pd.DataFrame:
        """Get accounts matching a string to be passed in. Usually contains
        some toml to parse.

        Make sure to pass a unique and meaningful index for the toml series

        Returns:
            pd.DataFrame: a dataframe with the matched accounts
        """
        toml_series = toml_series.dropna()
        column_name = toml_series.name
        matched_series = toml_series[toml_series.str.contains(str_to_match)]
        first_toml_str = matched_series.iloc[0]
        keys = get_keys(
            first_toml_str, str_to_match
        )  # this assumes the first matched row will have all the keys
        df = matched_series.to_frame()
        # populate columns into dataframe using keys from get_keys
        for key in keys:
            df[key] = df[column_name].apply(lambda x: parse_toml(x, str_to_match, key))
        return df

    def get_commodity_prices(self) -> pd.DataFrame:
        dates = {"date": self.date_format}

        prices = self.pdw.df_fetch(
            self.pdw.read_sql_file("sql/prices.sql"), parse_dates=dates
        )
        prices.to_csv(f"{self.data_directory}/PRICES.csv")
        return self.filter_by_year(prices, "date")

    def get_nearest_commodity_bid(self, commodity: str, date: datetime) -> float:
        """Get the nearest bid for a given commodity (e.g. Corn, Soybeans)

        Args:
            commodity (str): Commodity by which to filter, may pass a guid or name
            date (datetime): date to search by

        Returns:
            float: price of the commodity around that date
        """
        # not using the get_commodity_prices function because we don't want to
        # filter on date by year
        dates = {"date": self.date_format}
        prices = self.pdw.df_fetch(
            self.pdw.read_sql_file("sql/prices.sql"), parse_dates=dates
        ).set_index("date")
        bids_mask = prices["type"].str.match("bid")
        # guid_list = type(prices["commodity_guid"].drop_duplicates().tolist())
        if commodity in (prices["commodity_guid"].drop_duplicates().tolist()):
            commodity_mask = prices["commodity_guid"].str.match(commodity)
        else:
            commodity_mask = prices["fullname"].str.match(commodity.title())
        prices = prices[bids_mask & commodity_mask]
        nearest_bid_date = nearest(prices.index.tolist(), date)
        log.info(f"found a nearby date: {nearest_bid_date}")
        return round(
            prices.loc[nearest_bid_date]["value_num"]
            / prices.loc[nearest_bid_date]["value_denom"],
            2,
        )

    def get_commodity_bids(self, how: str = "mean") -> pd.DataFrame:
        """gets the commodity bids from the price database in GnuCash

        Args:
            how (str, optional): Aggregation method - how to aggregate
            the dataframe. last or mean are common. Defaults to "mean".

        Returns:
            pd.DataFrame: small df with the grouped and aggregated bids
        """
        prices = self.get_commodity_prices()  # .set_index("date").sort_index()
        # commodity_list = prices["commodity_guid"].unique().tolist()
        prices["cash"] = prices["value_num"] / prices["value_denom"]
        prices.dropna(inplace=True)
        bids = prices[prices["type"].str.match("bid")].drop(
            columns=[
                "guid",
                "source",
                "source",
                "type",
                "value_num",
                "value_denom",
                "namespace",
                "cusip",
                "fraction",
                "quote_flag",
                "quote_source",
                "quote_tz",
            ]
        )
        return (
            bids.sort_values("date")
            .groupby(["commodity_guid", "fullname", "currency_guid"])
            .agg(how, numeric_only=True)
            .reset_index()
            .rename(columns={"fullname": "crop"})
        )

    def build_depreciation_dataframe(self) -> pd.DataFrame:
        """Builds depreciation schedule

        Returns:
            pd.DataFrame: dataframe containing depreciation entries
        """
        self.get_all_accounts()

        def get_depreciation_accounts() -> pd.DataFrame:
            depreciation_accounts = self.toml_to_df(
                self.all_accounts["account_notes"],
                "Depreciation",
            ).join(self.all_accounts, rsuffix="_acct")
            return depreciation_accounts

        def build_depreciation_schedule(
            account, amount, sec_179, method, term, date_in_service
        ):
            def get_deduction_month_frequency(method):
                if method in ("MO S/L",):  # "HY 200DB"):
                    return 6
                else:
                    return 12

            def get_amount_per_term(method: str):
                if method == "MO S/L":
                    depreciation = basis / (term * 2)
                elif method == "HY 200DB":
                    if loop_counter == (term):
                        depreciation = amount_left
                    elif loop_counter == 1:
                        depreciation_rate = ((basis / (term * 2)) / basis) * 2
                        depreciation = depreciation_rate * amount_left
                    else:
                        depreciation_rate = ((basis / (term)) / basis) * 2
                        depreciation = depreciation_rate * amount_left
                    # log.info(f"Depreciation= {depreciation}")
                else:
                    depreciation = round(basis / term, 2)
                return depreciation

            basis = amount - sec_179
            depreciation_codes = []
            accounts = []
            dates = []
            amounts = []
            descriptions = []
            date_in_service = datetime(date_in_service.year, 12, 31)
            amount_left = basis
            deduction_month_frequency = get_deduction_month_frequency(method)
            loop_counter = 0
            if sec_179 > 0:
                depreciation_codes.append("801")
                accounts.append(account)
                dates.append(date_in_service)
                amounts.append(sec_179 * -1)
                descriptions.append("Section 179")
            while amount_left > 0.5:
                loop_counter += 1
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
                descriptions.append("Regular Depreciation")
                amount_per_term = get_amount_per_term(method)
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
                            df["Cost"].iloc[i],
                            df["Sec_179"].iloc[i],
                            df["Method"].iloc[i],
                            df["Years"].iloc[i],
                            df["Date_in_Service"].iloc[i],
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
            depreciation_schedule["tx_num"] = depreciation_schedule.index
            depreciation_schedule["tx_guid"] = ""
            depreciation_schedule["tx_guid"] = depreciation_schedule["tx_guid"].apply(
                lambda v: uuid4().hex
            )
            depreciation_schedule["split_action"] = "DEPR"
            depreciation_schedule["split_guid"] = ""
            depreciation_schedule["split_guid"] = depreciation_schedule[
                "split_guid"
            ].apply(lambda v: uuid4().hex)
            depreciation_schedule["enter_date"] = datetime.now()
            depreciation_schedule["reconcile_date"] = datetime.now()
            depreciation_schedule["reconcile_state"] = ""
            depreciation_schedule["memo"] = depreciation_schedule["name"]
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
            return depreciation_df.reset_index(drop=True).sort_values(
                by=["account_code", "post_date"]
            )

        return build_dataframe(get_depreciation_accounts())

    def get_depreciation_schedule(self) -> pd.DataFrame:
        """returns depreciation schedule transactions

        Returns:
            pd.DataFrame: dataframe containing depreciation transactions
            for the specified year
        """
        return self.filter_by_year(self.build_depreciation_dataframe(), "post_date")

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
            if inverse_multiplier is True:
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
            2023-04-16 - explicitly attempting to coerce errors for post_date
            there seems to be som normalization issues in GNUCash so it'd be
            good to do some database fixing to normalize the precision of the
            data in SQLite
            """
            sql = self.pdw.read_sql_file("sql/transactions_master.sql")
            dates = {
                # 2023-04-17 I don't think I need to be this specific
                # after running the post_date_fixer.sql
                "post_date": {
                    "format": self.date_format,
                    "errors": "coerce",
                    "exact": False,
                },
                "enter_date": self.date_format,
                "reconcile_date": self.date_format,
            }
            for guid in guids:
                query = sql.format(
                    inner_where.format(guid), main_where.format(guid), multiplier
                )
                tx = self.pdw.df_fetch(query, parse_dates=dates)
                if guid == guids[0]:
                    all_tx = tx
                else:
                    if not tx.empty:
                        all_tx = pd.concat([all_tx, tx])
            return all_tx

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
        invoices = (
            self.get_invoices()
            .groupby(["tx_guid", "account_guid"])
            .sum(numeric_only=True)
        )
        tx.set_index(["tx_guid", "account_guid"], inplace=True)
        tx = tx.join(invoices[["quantity"]])
        return tx.fillna(0)

    def get_assets(self) -> pd.DataFrame:
        """calls fetch transactions with ASSETS as parameter

        Returns:
            pd.Dataframe: dataframe containing asset transactions
        """
        return self.filter_by_year(
            self.fetch_transactions(["ASSET"], True), "post_date", True
        )

    def get_cash(self) -> pd.DataFrame:
        """calls fetch transactions with BANK, CASH as parameter

        Returns:
            pd.Dataframe: dataframe containing cash transactions
        """
        return self.filter_by_year(
            self.fetch_transactions(["BANK", "CASH"], True), "post_date", True
        )

    def get_liabilities(self) -> pd.DataFrame:
        """calls fetch transactions with liability types as parameters

        Returns:
            pd.Dataframe: dataframe containing liability transactions
        """
        return self.filter_by_year(
            self.fetch_transactions(["LIABILITY", "CREDIT", "PAYABLE"], True),
            "post_date",
            True,
        )

    def get_stock(self) -> pd.DataFrame:
        """calls fetch transactions with STOCK, False as parameter
        for farming operations this dataframe contains grain inventory
        qty (quantity) column is important for this calcualtion, as
        it is needed when calculating different commodity values

        Returns:
            pd.Dataframe: dataframe containing stock transactions
        """
        return self.filter_by_year(
            self.fetch_transactions(["STOCK"], False), "post_date", True
        )

    def get_commodity_stock_values(
        self, groupby: list = ["commodity_guid"]
    ) -> pd.DataFrame:
        """Leverages get_stock to build a dataframe containing grain
        values.

        Returns:
            pd.DataFrame: dataframe with grain values
        """
        df = (
            self.add_descriptor_column(self.get_stock(), "parent_accounts")
            .groupby(groupby)
            .sum(numeric_only=True)
            .join(self.get_commodity_bids(how="last").set_index("commodity_guid"))
        )
        df["balance_sheet_category"] = "Grain"
        df["amt"] = df["qty"] * df["cash"]
        return df.drop(columns=["quantity", "cash"])

    def get_balance_sheet_details(self) -> pd.DataFrame:
        assets = self.get_assets()
        assets["balance_sheet_category"] = "Assets"
        cash = self.get_cash()
        cash["balance_sheet_category"] = "Cash"
        liabilities = self.get_liabilities()
        liabilities["balance_sheet_category"] = "Liabilities"
        return pd.concat(
            [
                assets,
                cash,
                liabilities,
            ]
        )

    def get_balance_sheet(self) -> pd.DataFrame:
        balance_sheet = (
            self.get_balance_sheet_details()
            .groupby("balance_sheet_category")
            .sum(numeric_only=True)
            .drop(columns=["quantity"])
        )

        grain = (
            self.get_commodity_stock_values()
            .groupby("balance_sheet_category")
            .sum(numeric_only=True)
        )
        # balance_sheet.loc["Grain"] = (grain["value"][0], grain["qty"][0])
        return pd.concat([balance_sheet, grain])

    def get_all_transactions(self) -> pd.DataFrame:
        """2024-09-16 JRK
        don't use this for much! it's just a mess of transactions
        initial creation for the purposes of running aggregate functions
        against the transactions (max date for latest transaction)

        Returns:
            pd.Dataframe: dataframe containing desired transactions
            sorted by post dates
        """
        sql = self.pdw.read_sql_file("sql/all_transactions.sql")
        return self.pdw.df_fetch(sql).reset_index().sort_values(by=["post_date"])

    def get_all_cash_transactions(self) -> pd.DataFrame:
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

    def get_cleaned_cash_transactions(self) -> pd.DataFrame:
        """calls fetch transactions passing the necessary account types
        to retrieve actual cash transactions throughout the accounting period
        removes account to account entries (transfers) and payments on
        payables and receivables

        Returns:
            pd.Dataframe: dataframe containing desired transactions
        """
        tx = self.get_all_cash_transactions().drop(columns="qty")

        # Remove the acct-to-acct entries (e.g. AP to Checking, etc)
        guid_mask = (
            tx["account_guid"].isin(self.get_guid_list(self.cash_accounts)) == False
        )
        # Remove Payment entries for Bills/Invoices.
        action_mask = tx["split_action"].isin(["Payment"]) == False

        # Filter to transactions from a given year if provided
        return self.filter_by_year(tx[guid_mask & action_mask], "post_date")

    def get_farm_cash_transactions(
        self, include_depreciation: bool = False
    ) -> pd.DataFrame:
        """_summary_

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
            tx.loc[tx["account_code"].str.startswith("133"), "account_name"] = "Corn"
            tx.loc[tx["account_code"].str.startswith("134"), "account_name"] = (
                "Soybeans"
            )
            tx.loc[tx["account_code"].str.match("133|134"), "account_type"] = "INCOME"
            tx.loc[tx["account_code"].str.startswith("133"), "account_code"] = "301c"
            tx.loc[tx["account_code"].str.startswith("134"), "account_code"] = "303b"

            # Classify Prepaids as Expense
            tx.loc[tx["account_code"].str.match("146|147"), "account_type"] = "EXPENSE"

            # Classify Non-Taxable Expenses
            tx.loc[tx["account_code"].str.startswith("9"), "account_type"] = (
                "NF EXPENSE"
            )

            tx.to_csv(f"{self.data_directory}/FARM_TRANSACTIONS.csv")

            return tx.sort_values(by=["account_code", "post_date"])

        df = filter_and_reclassify_farm_transactions(
            self.get_cleaned_cash_transactions()
        )
        if include_depreciation:
            return pd.concat([df, self.get_depreciation_schedule()])
        else:
            return df

    def get_invoices(self):
        # bring in invoices for quantities
        dates = {
            "date_posted": self.date_format,
            "date_opened": self.date_format,
            "entry_date": self.date_format,
        }
        invoices_sql = self.pdw.read_sql_file("sql/invoices_master.sql")
        invoices = self.pdw.df_fetch(invoices_sql, parse_dates=dates)
        invoices.to_csv("export/invoices.csv")
        return self.filter_by_year(
            invoices.rename(columns={"post_txn": "tx_guid"}), "date_posted"
        )

    def get_corporation_value(self):
        balance_sheet = self.get_balance_sheet()
        corp_config = get_config()
        total_shares = corp_config["Valuation"]["Shares"]
        discounts = corp_config["Discounts"]
        for key in discounts:
            balance_sheet.loc[key, "share_val"] = (
                (discounts[key] / 100) * balance_sheet.loc[key]["amt"]
            ) / total_shares

        balance_sheet.loc["Total"] = balance_sheet.sum(numeric_only=True).to_list()
        return balance_sheet.drop(columns=["qty"])

    def get_trend_data(
        self, func: Callable, years_to_go_back: int, index_cols: list, trend_col: str
    ) -> pd.DataFrame:
        """Attempts to build trend analysis when provided a function.
        User defines how many years to go back, and on which columns
        to index and analyze trends.

        Args:
            func (function): function to pass
            years_to_go_back (int): how many years to go back
            index_col (str): column on which to index
            trend_col (str): numerical column to trend

        Returns:
            pd.DataFrame: a pivoted dataframe where columns are years
        """
        original_year = self.year
        for x in range(years_to_go_back):
            self.year = original_year - years_to_go_back + x + 1
            df = func()
            df["year"] = self.year
            if x == 0:
                trend_df = df.set_index(["year"] + index_cols)
            else:
                trend_df = pd.concat([trend_df, df.set_index(["year"] + index_cols)])
        self.year = original_year

        return pd.pivot_table(
            trend_df, values=trend_col, index=index_cols, columns="year"
        ).fillna(0)

    def trendsetter(self, df: pd.DataFrame) -> pd.DataFrame:
        """Takes a dataframe (generated by get_trend_data) and
        smushes all the columns together as strings so the data
        can be used to generate nifty little graphs

        Args:
            df (pd.DataFrame): dataframe to pass in

        Returns:
            pd.DataFrame: dataframe going back out with only one column
        """
        for col in df.columns:
            df[col] = df[col].astype(str)
        df["Trend"] = df[df.columns.values].agg(" ".join, axis=1)
        return df["Trend"]

    def get_df_with_trend_data(
        self, func: Callable, years_to_go_back: int, index_col: str, trend_col: str
    ) -> pd.DataFrame:
        """Returns main report with trend data appended as a
        "Trend" column

        Args:
            func (function): function to pass
            years_to_go_back (int): how many years to go back
            index_col (str): column on which to index
            trend_col (str): numerical column to trend

        Returns:
            pd.DataFrame: a dataframe with trend data
        """
        trend = self.trendsetter(
            self.get_trend_data(func, years_to_go_back, index_col, trend_col)
        )
        df = func().set_index(index_col)
        return df.join(trend)

    def iconize(self, column):
        return column.str.lower() + ".png"

    def get_summary(self, groupby: list, include_depreciation=False):
        return (
            self.get_farm_cash_transactions(include_depreciation=include_depreciation)
            .sort_values("account_code")
            .groupby(groupby)
            .sum(["quantity", "amt"])
            .reset_index()
        )

    def get_summary_by_account(self, include_depreciation=False):
        return self.get_summary(
            groupby=["account_code", "account_name"],
            include_depreciation=include_depreciation,
        ).rename(
            columns={
                "account_code": "Code",
                "account_name": "Account",
                "amt": "Amount",
                "quantity": "Quantity",
            }
        )[
            ["Code", "Account", "Quantity", "Amount"]
        ]

    def get_summary_by_finpack_account(self, include_depreciation=False):
        return self.get_summary(
            groupby=["account_type", "finpack_account"],
            include_depreciation=include_depreciation,
        ).rename(
            columns={
                "account_type": "Type",
                "finpack_account": "Account",
                "amt": "Amount",
                "quantity": "Quantity",
            }
        )[
            ["Type", "Account", "Quantity", "Amount"]
        ]

    def get_executive_summary(self, include_depreciation=False):
        df = (
            self.get_farm_cash_transactions(include_depreciation=include_depreciation)
            .reset_index(drop=True)
            .sort_values(by=["account_code", "post_date"])
            .groupby("account_type")
            .sum(numeric_only=True)
            .drop(columns="quantity")
        )
        df["order"] = 100
        df.loc["INCOME", "order"] = 10
        df.loc["EXPENSE", "order"] = 20
        df.loc["OIBDA", "order"] = 30
        df.loc["OIBDA", "amt"] = df.loc["INCOME", "amt"] + df.loc["EXPENSE", "amt"]
        if include_depreciation:
            df.loc["DEPRECIATION", "order"] = 40
            df.loc["NET INCOME", "order"] = 50
            df.loc["NET INCOME", "amt"] = (
                df.loc["OIBDA", "amt"] + df.loc["DEPRECIATION", "amt"]
            )
        return (
            df.sort_values("order")[:5]
            .reset_index()
            .reindex()
            .drop(columns="order")
            .rename(columns={"account_type": "Account", "amt": "Amount"})
        )

    def get_1099_vendor_report(self):
        """Pulls 1099 Vendors from database and drops the data in an
        Excel file to be shipped off for 1099 creation
        """
        dates = {
            "post_date": self.date_format,
        }
        tax_1099_vendors = self.pdw.read_sql_file("sql/1099_vendors.sql")
        vendors = self.pdw.df_fetch(tax_1099_vendors, parse_dates=dates)

        # Filter to transactions from a given year
        vendors = vendors[vendors["post_date"].dt.year == self.year]
        # Drop the time, not needed
        vendors["post_date"] = vendors["post_date"].dt.date

        all_accounts = self.get_all_accounts().reset_index()
        all_accounts.rename(columns={"guid": "acct_guid"}, inplace=True)
        all_accounts.set_index("acct_guid", drop=True, inplace=True)

        # Pull in "Finpack Account" - Which is not Filtered based on Commodity
        vendors = vendors.join(
            all_accounts[["finpack_account", "parent_accounts"]], on="acct_guid"
        )

        grouped_vendors = (
            vendors.groupby(
                [
                    "finpack_account",
                    "vendor_name",
                    "vendor_id",
                    "addr_addr1",
                    "addr_addr2",
                    "addr_addr3",
                ]
            )
            .sum(numeric_only=True)
            .round(2)
        )
        grouped_vendors.reset_index(inplace=True)

        writer = pd.ExcelWriter(
            f"export/{self.year}-1099_Vendor_Data.xlsx", engine="xlsxwriter"
        )
        grouped_vendors.to_excel(writer, index=False, sheet_name="Corporation")
        workbook = writer.book
        sheet = writer.sheets["Corporation"]
        fmt_currency = workbook.add_format({"num_format": "$#,##0.00", "bold": False})
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

        for col, value in enumerate(grouped_vendors.columns.values):
            sheet.write(0, col, value, fmt_header)
        sheet.set_column("G:G", 10, fmt_currency)

        writer.close()

    def get_personal_business_expenses(self):
        dates = {
            "post_date": self.date_format,
        }
        pdw_personal = pd_db_wrangler.Pandas_DB_Wrangler(
            connect_string=get_gnucash_file_path(books="personal")
        )
        sql = pdw_personal.read_sql_file("sql/personal_business_expenses.sql")
        business_expenses = pdw_personal.df_fetch(
            sql.format(self.year), parse_dates=dates
        )

        # Filter to transactions from a given year
        year_mask = business_expenses["post_date"].dt.year == self.year
        # Apply the filter to the dataframe
        business_expenses.dropna(inplace=True)
        business_expenses = business_expenses[year_mask]
        # Drop the time, not needed
        business_expenses["post_date"] = business_expenses["post_date"].dt.date
        business_expenses["Deduct_Total"] = round(
            business_expenses["Amt"] * (business_expenses["Deduct_Percentage"] * 0.01),
            2,
        )
        business_expenses.to_csv(f"export/{self.year}-personal-farm-expenses.csv")
        return business_expenses.groupby("Acct").sum(["Amt", "Deduct_Total"])

    def get_1099_personal_vendors(self):
        dates = {
            "post_date": self.date_format,
        }
        pdw_personal = pd_db_wrangler.Pandas_DB_Wrangler(
            get_gnucash_file_path(books="personal")
        )

        tax_1099_vendors = pdw_personal.read_sql_file("sql/1099_vendors_personal.sql")
        personal_vendors = pdw_personal.df_fetch(tax_1099_vendors, parse_dates=dates)

        # Filter to transactions from a given year
        year_mask = personal_vendors["post_date"].dt.year == self.year
        # Apply the filter to the dataframe
        personal_vendors = personal_vendors[year_mask]
        # Drop the time, not needed
        personal_vendors["post_date"] = personal_vendors["post_date"].dt.date

        grouped_personal_vendors = (
            personal_vendors.groupby(["description"]).sum(numeric_only=True).round(2)
        )
        grouped_personal_vendors.reset_index(inplace=True)

        writer = pd.ExcelWriter(
            f"export/{self.year}-1099_Personal.xlsx", engine="xlsxwriter"
        )
        grouped_personal_vendors.to_excel(writer, index=False, sheet_name="Totals")
        workbook = writer.book

        totals_sheet = writer.sheets["Totals"]
        # Adding currency format
        fmt_currency = workbook.add_format({"num_format": "$#,##0.00", "bold": False})
        fmt_header = workbook.add_format(self.excel_formatting["header"])

        for col, value in enumerate(grouped_personal_vendors.columns.values):
            totals_sheet.write(0, col, value, fmt_header)

        totals_sheet.set_column("B:B", 10, fmt_currency)

        personal_vendors.to_excel(writer, index=False, sheet_name="Detail")
        details_sheet = writer.sheets["Detail"]
        for col, value in enumerate(personal_vendors.columns.values):
            details_sheet.write(0, col, value, fmt_header)

        details_sheet.set_column("D:D", 10, fmt_currency)
        del pdw_personal
        writer.close()

    def generate_wage_reports(self):
        dates = {
            "post_date": self.date_format,
        }
        sql = self.pdw.read_sql_file("sql/w-2_employees.sql")
        df = self.pdw.df_fetch(sql.format(self.year), parse_dates=dates)

        df["month"] = df["post_date"].dt.month
        # Filter to transactions from a given year
        year_mask = df["post_date"].dt.year == self.year
        # Apply the filter to the dataframe
        df = df[year_mask]
        # Drop the time, not needed
        df["post_date"] = df["post_date"].dt.date

        # DETAILS
        writer = pd.ExcelWriter(f"export/{self.year}-W2_Data.xlsx", engine="xlsxwriter")
        df.to_excel(writer, index=False, sheet_name="Transactions")
        workbook = writer.book
        transactions_sheet = writer.sheets["Transactions"]
        fmt_header = workbook.add_format(self.excel_formatting["header"])
        fmt_currency = workbook.add_format(self.excel_formatting["currency"])

        for col, value in enumerate(df.columns.values):
            transactions_sheet.write(0, col, value, fmt_header)
        transactions_sheet.set_column("E:E", 10, fmt_currency)

        # WITHHOLDING
        monthly_withholding = df.groupby(["month", "memo"]).sum(numeric_only=True)
        monthly_withholding.reset_index(inplace=True)
        monthly_withholding.to_excel(
            writer, index=False, sheet_name="Monthly_Withholding"
        )
        monthly_sheet = writer.sheets["Monthly_Withholding"]
        for col, value in enumerate(monthly_withholding.columns.values):
            monthly_sheet.write(0, col, value, fmt_header)
        monthly_sheet.set_column("C:C", 10, fmt_currency)

        # WAGE TOTALS
        w2 = df.groupby(
            ["code", "emp_id", "emp_name", "emp_addr1", "emp_addr2", "memo"]
        )["amt"].sum()
        w2 = w2.reset_index()
        w2.to_excel(writer, index=False, sheet_name="W2_Wage_Totals")
        wages_sheet = writer.sheets["W2_Wage_Totals"]
        for col, value in enumerate(w2.columns.values):
            wages_sheet.write(0, col, value, fmt_header)
        wages_sheet.set_column("G:G", 10, fmt_currency)

        # LABOR DEPOSITS
        labor_sql = self.pdw.read_sql_file("sql/federal_labor_deposits.sql")
        labor_deposits = self.pdw.df_fetch(labor_sql, parse_dates=dates)

        # Filter to transactions from a given year
        year_mask = labor_deposits["post_date"].dt.year == self.year
        labor_deposits = labor_deposits[year_mask]
        # Drop the time, not needed
        labor_deposits["post_date"] = labor_deposits["post_date"].dt.date
        labor_deposits.to_excel(writer, index=False, sheet_name="Labor_Deposits")
        deposits_sheet = writer.sheets["Labor_Deposits"]
        for col, value in enumerate(labor_deposits.columns.values):
            deposits_sheet.write(0, col, value, fmt_header)
        deposits_sheet.set_column("C:C", 10, fmt_currency)
        writer.close()

    def get_rented_acres(self) -> pd.DataFrame:
        """Get a listing of all landowners and the farms they are leasing
        Data input into GnuCash is important for this to work properly.

        This function will filter to project entries only.
        "Project" is one of three dropdown choices available in GNUCash
        when creating bills with entries. The others being "Hours" and
        Material". When entering these values in the system, it's
        important "Project" for Base Rent payments, as it will
        differentiate between the base rent and bonus rent.

        Returns:
            pd.DataFrame: df listing the pertinent information for
            land rent. Used to calculate acreage and production.
        """
        df = (
            self.get_invoices()
            .drop(
                columns=["paytype", "disc_how", "disc_amt", "taxable", "taxincluded"],
            )
            .join(self.get_all_accounts(), on="account_guid", rsuffix="_acct")
        )

        cash_rent = df["account_code"].isin(["424b", "424c"])

        base_only = df["quantity_type"] == "Project"
        df = (
            df[cash_rent & base_only]
            .groupby(["crop", "operation", "operation_id", "org_name"])[
                ["quantity", "amt"]
            ]
            .sum(numeric_only=True)
            .round(2)
            .rename(index=str, columns={"quantity": "acres"})
        )
        df["base_rent"] = round(df["amt"] / df["acres"])
        return df

    def get_planted_acres(self, include_operation_id: bool = False) -> pd.DataFrame:
        """Pull the acreage planted for the year

        Args:
            include_operation_id (bool, optional): whether or not to include operation_id
            which may have indexing and aggregation implications. Defaults to False.

        Returns:
            pd.DataFrame: dataframe including acreage data
        """
        if include_operation_id == True:
            columns_to_drop = ["org_name", "amt", "base_rent"]
            groupby_columns = ["crop", "operation", "operation_id"]
        else:
            columns_to_drop = ["org_name", "amt", "base_rent", "operation_id"]
            groupby_columns = ["crop", "operation"]
        return (
            self.get_rented_acres()
            .reset_index()
            .drop(columns=columns_to_drop)
            .groupby(groupby_columns)
            .sum(numeric_only=True)
            .round(2)
        )

    def get_production(self, include_operation_id=True) -> pd.DataFrame:
        """pulls production per field

        Returns:
            pd.DataFrame: _description_
        """
        df = (
            self.get_invoices()
            .drop(
                columns=["paytype", "disc_how", "disc_amt", "taxable", "taxincluded"],
            )
            .join(self.get_all_accounts(), on="account_guid", rsuffix="_acct")
        )

        harvest = df["account_code"].isin(["301c", "303b"])

        groupby_columns = ["crop", "operation", "operation_id"]
        if not include_operation_id:
            groupby_columns.remove("operation_id")
        df = (
            df[harvest]
            .groupby(groupby_columns)["quantity",]
            .sum(numeric_only=True)
            .round(2)
            .rename(index=str, columns={"quantity": "total_bushels"})
            .join(
                self.get_planted_acres(include_operation_id=include_operation_id),
                rsuffix="_planted",
            )
        )

        df["bu_per_acre"] = round(df["total_bushels"] / df["acres"], 2)
        return df

    def flexible_lease_calculator(self) -> pd.DataFrame:
        """Calculates Flexible Lease arrangements with data from GnuCash
        First parses TOML from Vendor Notes to get Lease terms and percentages
        Uses Rental Invoices to determine acres and Production data for yield.
        All Grain elevator bids are entered in to GnuCash as well, which is used
        to calculate average grain prices

        Returns:
            pd.DataFrame: df containing the calculated Cash rent bonuses for a given
            year.
        """
        terms = self.get_invoices().set_index("operation_id")["org_notes"]
        terms = self.toml_to_df(terms, "Vendor_Details")

        df = terms.reset_index().drop(columns=["org_notes", "Receive_1099", "Min"])
        df = (
            df.melt(
                id_vars=["operation_id", "Max"],
                var_name="crop",
                value_name="Percentage",
            )
            .drop_duplicates()
            .set_index(["operation_id", "crop"], drop=True)
        )

        rental = (
            self.get_rented_acres()
            .join(self.get_production().drop(columns="acres"))
            .reset_index()
            .set_index(["operation_id", "crop"])
            .join(self.get_commodity_bids().set_index("crop"))
            .rename(columns={"cash": "price"})
        )
        df = (
            rental.join(df, how="inner")
            .reset_index()
            .rename(columns={"Max": "rent_cap"})
        )
        df["rev_pct"] = df["Percentage"] / 100

        df["revenue"] = round(df["bu_per_acre"] * df["price"], 2)
        df["capped_total"] = round(df["rent_cap"] * df["acres"], 2)
        df["capped_bonus"] = round(
            df["capped_total"] - df["base_rent"] * df["acres"], 2
        )
        df["raw_bonus"] = round(
            (
                (df["revenue"] * df["acres"] * df["rev_pct"])
                - df["base_rent"] * df["acres"]
            ),
            2,
        )
        df["bonus"] = df[["raw_bonus", "capped_bonus"]].min(axis=1)
        df["adj_rent"] = round((df["bonus"] + df["amt"]) / df["acres"], 2)
        df["price"] = round(df["price"], 2)
        return df.drop(
            columns=[
                "amt",
                "Percentage",
                "commodity_guid",
                "operation_id",
                "currency_guid",
            ]
        ).rename(
            index=str,
            columns={
                "crop": "Crop",
                "operation": "Field",
                "org_name": "Owner",
                "acres": "Acres",
                "base_rent": "Base Rent",
                "total_bushels": "Total Bushels",
                "bu_per_acre": "Bu/Acre",
                "price": "Price",
                "rent_cap": "Rent Cap",
                "rev_pct": "Revenue %",
                "revenue": "Revenue",
                "capped_total": "Capped Total",
                "capped_bonus": "Capped Bonus",
                "raw_bonus": "Raw Bonus",
                "bonus": "Bonus",
                "adj_rent": "Adjusted Rent",
            },
        )

    def get_existing_records(
        self, identifiers: list, table: str = "TRANSACTIONS", column: str = "num"
    ) -> list:
        sql = f"""SELECT * FROM {table} WHERE {column} IN {[str(x) for x in identifiers]}""".replace(
            "[",
            "(",
        ).replace(
            "]", ")"
        )
        return self.pdw.df_fetch(sql).set_index(column)
        # return [x for x in existing_records.index.to_list()]

    def get_joplin_notes(self, ticket_nums: list):
        self.joplin = get_config()["Joplin"]
        pdw_joplin = pd_db_wrangler.Pandas_DB_Wrangler(
            connect_string=self.joplin["joplin_db"]
        )
        sql = "SELECT id as joplin_id, title FROM notes"
        sql += f""" WHERE title IN {["Scale Ticket " + str(x) for x in ticket_nums]}""".replace(
            "[",
            "(",
        ).replace(
            "]", ")"
        )
        joplin_notes = pdw_joplin.df_fetch(sql)
        joplin_notes["num"] = joplin_notes["title"].str.replace("Scale Ticket ", "")
        del pdw_joplin
        return joplin_notes.set_index("num").drop(columns="title")

    def joplin_note_query(self, where_clause=""):
        self.joplin = get_config()["Joplin"]
        pdw_joplin = pd_db_wrangler.Pandas_DB_Wrangler(
            connect_string=self.joplin["joplin_db"]
        )
        sql = pdw_joplin.read_sql_file("sql/joplin_base_query.sql")
        sql += where_clause
        return pdw_joplin.df_fetch(sql)

    def get_associated_uris(self, tx_df: pd.DataFrame):
        """find existing assoc_uris from db"""

        def build_slots_df():
            df = tx_df.join(self.get_joplin_notes(tx_df["num"].tolist()), on="num")
            slots = df[["guid", "joplin_id"]]

            slots["name"] = "assoc_uri"
            slots["slot_type"] = 4
            slots["int64_val"] = 0
            slots["string_val"] = self.joplin["joplin_uri_prefix"] + slots["joplin_id"]
            slots.drop(columns="joplin_id", inplace=True)
            slots["timespec_val"] = "1970-01-01 00:00:00"
            slots["numeric_val_num"] = 0
            slots["numeric_val_denom"] = 1
            return slots.dropna().rename(columns={"guid": "obj_guid"})

        df = self.get_existing_records(
            identifiers=tx_df.reset_index().set_index("guid").index.tolist(),
            table="slots",
            column="obj_guid",
        )
        slots = build_slots_df()
        return slots
        # df_nope = df[~df["num"].isin(df["Ticket Number"])]
        # print(df_nope)

    def set_load_file(self, filename: str):
        self.load_file = filename

    def read_loads_from_file(self):
        self.elevator = get_config()["Elevator"]
        df = pd.read_csv(
            self.load_file,
            parse_dates=[" Tare Time Stamp", " Gross Time Stamp"],
            date_format="%m/%d/%y %H:%M:%S",
        )
        df.columns = df.columns.str.strip()
        log.info(df.head())
        return df

    def get_elevator_loads_with_commodity_ids(self):
        df = (
            self.read_loads_from_file()
            .groupby(["Ticket Number", "Tare Time Stamp", "Crop Description"])
            .sum(numeric_only=True)
            .reset_index()
            .set_index("Ticket Number")
        )
        df["guid"] = ""
        df["guid"] = df["guid"].apply(lambda v: uuid4().hex)
        df.index = df.index.map(str)
        df = df.reset_index().rename(
            columns={
                "Ticket Number": "num",
                "Tare Time Stamp": "post_date",
            }
        )
        df.loc[df["Crop Description"].str.match("CORN"), "crop"] = "Corn"
        df.loc[df["Crop Description"].str.match("BEANS"), "crop"] = "Soybeans"
        commodities = self.get_commodity_bids().set_index("crop")
        price_count = len(commodities)
        log.info(f"{price_count} commodity price entries for {self.year}")
        if price_count < 1:
            log.warning(
                "No prices exist for commodity, this is likely to cause problems"
            )
        df = df.join(commodities, on="crop")
        df["enter_date"] = datetime.now()
        df["description"] = self.elevator["elevator_name"]
        return df

    def get_split_accounts(self, search_term: str) -> pd.Series:
        """Function to find split accounts for a given search term
        refactored 2025-05-23 to handle renamed tree structure

        Args:
            search_term (str): search term used for filtering accounts
            e.g. "Harvested" for harvested grain

        Returns:
            pd.Series: returns a pandas series indexed by commodity_guid
        """
        self.get_all_accounts()
        name_search = self.all_accounts.loc[
            self.all_accounts["name"].str.contains(search_term)
        ]
        parent_search = self.all_accounts.loc[
            self.all_accounts["parent_accounts"].str.contains(search_term)
        ]
        df = pd.concat([name_search, parent_search])
        return df.reset_index().set_index("commodity_guid")["guid"]

    def process_elevator_load_file(self) -> pd.DataFrame:
        """Function to process a CSV list of loads downloaded from
        an elevator account

        Returns:
            pd.DataFrame: Dataframe containing the minimum columns
            needed to generated transactions and splits (some of the
            columns will be dropped later)
        """
        df = self.get_elevator_loads_with_commodity_ids()
        from_df = self.get_split_accounts("Harvested").rename("from_guid")
        to_df = self.get_split_accounts("Delivered").rename("to_guid")
        df = df.join(from_df, on="commodity_guid").join(to_df, on="commodity_guid")
        entered_tx_df = self.get_existing_records(df["num"].tolist())
        entered_tix = entered_tx_df.index.tolist()
        df = df[~df["num"].isin(entered_tix)]  # filter out the txns already entered
        return df[
            [
                "guid",
                "currency_guid",
                "num",
                "post_date",
                "enter_date",
                "description",
                "Net Units",
                "cash",
                "from_guid",
                "to_guid",
            ]
        ].reset_index(drop=True)

    def create_db_records_from_load_file(
        self, filename: Path, write_to_db: bool = False
    ):
        """Build transactions, splits, and slots from a loads file
        downloaded from an elevator website

        Args:
            write_to_db (bool, optional): Will write the changes to the
            database. Defaults to False.
        """
        try:
            self.set_load_file(filename)
            transactions = self.process_elevator_load_file()
            splits_buy = transactions[
                ["guid", "Net Units", "cash", "to_guid", "from_guid"]
            ]
            transactions.drop(
                columns=["Net Units", "cash", "to_guid", "from_guid"], inplace=True
            )
            splits_buy.reset_index(inplace=True, drop=True)
            splits_buy.rename(columns={"guid": "tx_guid"}, inplace=True)
            splits_buy["guid"] = ""
            splits_buy["guid"] = splits_buy["guid"].apply(lambda v: uuid4().hex)
            splits_buy["account_guid"] = splits_buy["to_guid"]
            splits_buy["memo"] = "imported from CSV"
            splits_buy["action"] = "Buy"
            splits_buy["reconcile_state"] = "n"
            splits_buy["reconcile_date"] = None
            splits_buy["value_num"] = round(
                (splits_buy["Net Units"] * splits_buy["cash"] * 100), 2
            ).astype(int)
            splits_buy["value_denom"] = 100
            splits_buy["quantity_num"] = (splits_buy["Net Units"] * 100).astype(int)
            splits_buy["quantity_denom"] = 100
            splits_buy["lot_guid"] = None
            splits_buy = splits_buy.drop(
                columns=[
                    "Net Units",
                    "cash",
                ]
            )

            splits_sell = splits_buy.copy(deep=True)
            splits_sell["action"] = "Sell"
            splits_sell["account_guid"] = splits_sell["from_guid"]
            splits_sell["quantity_num"] = splits_sell["quantity_num"] * -1
            splits_sell["value_num"] = splits_sell["value_num"] * -1
            splits_sell["guid"] = splits_sell["guid"].apply(lambda v: uuid4().hex)

            # done with to/from columns, drop 'em
            splits_buy = splits_buy.drop(
                columns=[
                    "to_guid",
                    "from_guid",
                ]
            )
            splits_sell = splits_sell.drop(
                columns=[
                    "to_guid",
                    "from_guid",
                ]
            )

            splits = pd.concat([splits_buy, splits_sell])
            splits.reset_index(inplace=True, drop=True)
            splits["reconcile_date"] = "1970-01-01 00:00:00"
            splits["lot_guid"] = None

            assert splits.sum()["value_num"] == 0

            log.info("***TRANSACTIONS***")
            log.info(transactions)
            log.info("***SPLITS***")
            log.info(splits)
            slots = self.get_associated_uris(transactions)
            log.info("***SLOTS***")
            log.info(slots)

            if write_to_db:
                log.warning("Attempting to write dataframes to the database!")
                log.warning("Checking for database lock...")
                if self.db_locked():
                    log.error("Database locked, cannot proceed.")
                    return -1
                log.warning("Database not locked. Proceeding...")
                tx_len = len(transactions)
                if tx_len > 0 and len(splits) == tx_len * 2:
                    transactions.to_sql(
                        "transactions", con=self.engine, if_exists="append", index=False
                    )
                    splits.to_sql(
                        "splits", con=self.engine, if_exists="append", index=False
                    )
                    slots.to_sql(
                        "slots", con=self.engine, if_exists="append", index=False
                    )
                    log.warning("Updated Database!")
                else:
                    log.warning("No new records to process, db not updated!")
                return 0
            else:
                pass
        except ValueError as e:
            log.warning("Empty or invalid DataFrame, cannot process")

    def get_grain_invoices(self):
        """_summary_"""
        dates = {
            "post_date": self.date_format,
            "enter_date": self.date_format,
            "reconcile_date": self.date_format,
        }
        invoice_query = self.pdw.read_sql_file("sql/invoices_master.sql")

        # fetch invoices
        all_inv = self.pdw.df_fetch(invoice_query, parse_dates=dates)
        # format date
        all_inv["date_posted"] = pd.to_datetime(
            all_inv["date_posted"], utc=True, yearfirst=True, errors="coerce"
        ).dt.tz_localize(None)
        # drop unnecessary columns
        all_inv["amount"] = all_inv["amt"] - all_inv["disc_amt"]
        all_inv.drop(
            columns=[
                "paytype",
                "disc_how",
                "disc_amt",
                "taxable",
                "taxincluded",
                "price",
                "amt",
            ],
            inplace=True,
        )
        year_mask = all_inv["date_posted"].dt.year == self.year
        code_mask = all_inv["account_code"].str.match("133|134")
        inv_mask = all_inv["inv_type"] == "INVOICE"
        invoices = all_inv[year_mask & inv_mask & code_mask].join(
            self.get_all_accounts()["crop"], on="account_guid", rsuffix="_acct"
        )
        # Calculate the discounts using by getting inverse code matches
        grain_invoice_list = invoices["inv_id"].to_list()
        inverse_code_mask = ~all_inv["account_code"].str.match("133|134")
        grain_mask = all_inv["inv_id"].isin(grain_invoice_list)
        discounts = (
            all_inv[grain_mask & inverse_code_mask & inv_mask][["inv_id", "amount"]]
            .groupby(["inv_id"])
            .sum(numeric_only=True)
            .rename(columns={"amount": "discount_amt"})
        )
        # Group Grain Invoices
        grain = invoices.groupby(
            [
                "date_posted",
                "due_date",
                "inv_id",
                "crop",
                "account_name",
                "account_code",
                "org_name",
                "post_lot",
            ]
        ).sum(numeric_only=True)

        # Add the discounts
        grain = grain.join(discounts["discount_amt"], on="inv_id").fillna(0)
        grain["Price"] = round(grain["amount"] / grain["quantity"], 2)
        # 2024-09-13 added payment status
        payment_query = self.pdw.read_sql_file("sql/payments.sql")
        payments = self.pdw.df_fetch(payment_query).groupby("lot_guid").sum()
        grain = grain.join(payments, on="post_lot")
        grain["paid"] = abs(grain["amount"] 
                            + grain["discount_amt"] 
                            + grain["payment_amt"]) <= 0.02
        # grain["paid"] = abs(round(grain["payment_amt"], 2))
        # grain["settlement"] = grain["amount"] + grain["discount_amt"]
        return (
            grain.reset_index()
            .drop(columns=["payment_amt", "post_lot", "account_code", "discount_amt"])
            .rename(
                columns={
                    "date_posted": "Contract Date",
                    "due_date": "Delivery Date",
                    "inv_id": "Contract ID",
                    "crop": "Crop",
                    "org_name": "Elevator",
                    "quantity": "Bushels",
                    "amount": "Amount",
                    "paid": "Fulfilled",
                    "account_name": "Post Acct",
                    # "account_code": "Code",
                }
            )
        )

    def get_config(self):
        return get_config()

    def sanity_checker(self) -> bool:
        all_tx = self.get_all_cash_transactions()
        tx = self.get_farm_cash_transactions()

        # *** TOTALS ***
        account_totals = tx.groupby(["account_type"]).sum(numeric_only=True)
        net_cash_flow = round(account_totals.sum(numeric_only=True).iloc[0], 2)
        account_totals.loc["TOTAL (NET)"] = net_cash_flow

        last_year_mask = all_tx["post_date"].dt.year < self.year
        year_mask = all_tx["post_date"].dt.year <= self.year
        chk_mask = all_tx["src_type"].str.match("(BANK)|(CREDIT)|(CASH)")
        # chk_mask = all_tx["src_code"].str.match("100")
        all_tx[chk_mask & year_mask].groupby(["src_code", "src_name"]).sum(
            numeric_only=True
        ).to_csv(f"export/{self.year}-cash.csv")
        all_tx[chk_mask & last_year_mask].groupby(["src_code", "src_name"]).sum(
            numeric_only=True
        ).to_csv(f"export/{self.year - 1}-cash.csv")
        ar_mask = all_tx["src_type"].str.match("RECEIVABLE")
        ap_mask = all_tx["src_type"].str.match("PAYABLE")

        # Get ending checking balances, ensure consistency with
        # Balance Sheet from GNUCash
        ending_chk_bal = round(all_tx[chk_mask & year_mask]["amt"].sum(), 2)
        ending_ap_bal = round(all_tx[ap_mask & year_mask]["amt"].sum(), 2)
        ending_ar_bal = round(all_tx[ar_mask & year_mask]["amt"].sum(), 2)
        last_year_bal = round(all_tx[chk_mask & last_year_mask]["amt"].sum(), 2)
        last_year_ar_ap_bal = round(
            all_tx[(ar_mask | ap_mask) & last_year_mask]["amt"].sum(),
            2,
        )
        net_ar_ap = round(ending_ap_bal + ending_ar_bal, 2)
        net = round(net_cash_flow + last_year_ar_ap_bal + last_year_bal - net_ar_ap, 2)

        log.warning(
            "{} Ending cash balance was:                   {}".format(
                self.year - 1, last_year_bal
            )
        )
        log.warning(
            "{} Ending AR/AP balance was:                 {}".format(
                self.year - 1, last_year_ar_ap_bal
            )
        )
        sanity = net == ending_chk_bal
        log.warning(
            "{} Finpack net inflows and outflows:  (+){}".format(
                self.year, net_cash_flow
            )
        )
        log.warning(
            "{} ending AR/AP balance:              (+){}".format(self.year, net_ar_ap)
        )
        log.warning(
            "{} Finpack net minus AR/AP balance:   (=){}".format(self.year, net)
        )
        log.warning("-----------------------------------------------------")
        log.warning(
            "{} Ending balance sheet balance was: {}".format(self.year, ending_chk_bal)
        )
        log.warning(" -- We balance, right? ----------------- {}".format(sanity))
        log.warning("Difference = {}".format(round(net - ending_chk_bal, 2)))
        return sanity
