import pandas as pd
import pd_db_wrangler
from tabulate import tabulate
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from dateutil.relativedelta import relativedelta
from uuid import uuid4
from .config import get_datadir, get_gnucash_file_path, get_config
from .helpers import get_keys, parse_toml


class GnuCash_Data_Analysis:
    def __init__(self, cached_mode=False):
        self.CACHED_MODE = cached_mode
        self.data_directory = get_datadir()
        # Set Reporting year constant
        self.year = datetime.now().year  # defaults to current year
        self.all_accounts = None
        self.cash_accounts = ["RECEIVABLE", "PAYABLE", "BANK", "CREDIT", "CASH"]
        # Suppress warnings, format numbers
        pd.options.mode.chained_assignment = None  # default='warn'
        pd.set_option("display.float_format", lambda x: "%.2f" % x)
        self.pdw = pd_db_wrangler.Pandas_DB_Wrangler()
        self.pdw.set_connection_string(get_gnucash_file_path(), db_type="sqlite")

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

    def get_all_accounts(self) -> pd.DataFrame:
        """
        Optionally Override cached mode

        True = use CSV files
        False = use live data from database
        """
        if self.all_accounts is None:
            if self.CACHED_MODE:
                df = pd.read_csv(
                    f"{self.data_directory}/ALL_ACCOUNTS.csv", dtype={"code": object}
                )
            else:
                df = self.pdw.df_fetch(self.pdw.read_sql_file("sql/all_accounts.sql"))
                df.to_csv(f"{self.data_directory}/ALL_ACCOUNTS.csv", index=False)

            all_account_types = df["account_type"].unique().tolist()
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

            def get_third_level(x) -> str:
                """For the Finpack report, we want the 3rd Level of acct categories"""
                parents = x.split(">")
                level_count = len(parents)
                if level_count > 2:
                    return str(parents[level_count - 3])
                else:
                    return ""

            df["parent_accounts"] = (
                df["guid"].apply(lambda x: find_parent(x)).str.rstrip(">")
            )
            df["finpack_account"] = df["parent_accounts"].apply(
                lambda x: get_third_level(x)
            )
            df.loc[df["finpack_account"] == "", "finpack_account"] = df["name"]

            df.set_index("guid", drop=True, inplace=True)

            # Enterprise Column
            df["crop"] = "General"
            df.loc[df["parent_accounts"].str.contains("Soybeans"), "crop"] = "Soybeans"
            df.loc[df["parent_accounts"].str.contains("Corn"), "crop"] = "Corn"
            df.loc[df["name"].str.contains("Soybeans"), "crop"] = "Soybeans"
            df.loc[df["name"].str.contains("Corn"), "crop"] = "Corn"

            if not self.CACHED_MODE:
                # Create CSV with Parental Tree for reporting
                df.to_csv(f"{self.data_directory}/ALL_ACCOUNTS_W_PARENTS.csv")
                # Create CSV with Prices which is used for valuation
            self.all_accounts = df
        return self.all_accounts

    def toml_to_df(self, toml_series: pd.Series, str_to_match: str) -> pd.DataFrame:
        """Get accounts matching a string to be passed in. Usually contains some
        toml to parse.

        Make sure to pass a unique and meaningful index for the toml series

        Returns:
            pd.DataFrame: a dataframe with the matched accounts
        """
        column_name = toml_series.name
        match = toml_series[
            toml_series.str.contains(f"\[{str_to_match}\]").replace(
                "\\n", "\n", regex=True
            )
            == True
        ]
        match = match[match.index.drop_duplicates()]  # match.drop_duplicates()
        keys = get_keys(
            match[0], str_to_match
        )  # this assumes the first account will have all the keys we need
        df = match.to_frame()
        # populate columns into dataframe using keys from get_keys
        for key in keys:
            df[key] = df[column_name].apply(lambda x: parse_toml(x, str_to_match, key))
        return df

    def get_commodity_prices(self) -> pd.DataFrame:
        if not self.CACHED_MODE:
            prices = self.pdw.df_fetch(
                self.pdw.read_sql_file("sql/prices.sql"), parse_dates=["date"]
            )
            prices.to_csv(f"{self.data_directory}/PRICES.csv")
        else:
            prices = pd.read_csv(
                f"{self.data_directory}/PRICES.csv", parse_dates=["date"]
            )
        return self.filter_by_year(prices, "date")

    def get_commodity_bids(self, how: str = "mean") -> pd.DataFrame:
        """gets the commodity bids from the price database in GnuCash

        Args:
            how (str, optional): Aggregation method - how to aggregate
            the dataframe. last or mean are common. Defaults to "mean".

        Returns:
            pd.DataFrame: small df with the grouped and aggregated bids
        """
        prices = self.get_commodity_prices()  # .set_index("date").sort_index()
        commodity_list = prices["commodity_guid"].unique().tolist()
        prices["cash"] = prices["value_num"] / prices["value_denom"]
        bids = prices[prices["type"].str.match("bid")].drop(
            columns=[
                "guid",
                "source",
                "currency_guid",
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
            .groupby(["commodity_guid", "fullname"])
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
            basis = amount - sec_179
            depreciation_codes = []
            accounts = []
            dates = []
            amounts = []
            descriptions = []
            date_in_service = datetime(date_in_service.year, 12, 31)
            if method == "MO S/L":
                deduction_month_frequency = 6
                amount_per_term = basis / (term * 2)
            elif method == "HY DOB":
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
            return depreciation_df.reset_index(
                drop=True
            )  # .set_index(["tx_guid", "account_guid"])

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
        invoices = (
            self.get_invoices()
            .groupby(["tx_guid", "account_guid"])
            .sum(numeric_only=True)
        )
        tx.set_index(["tx_guid", "account_guid"], inplace=True)
        tx = tx.join(invoices[["quantity"]])
        tx["post_date"] = pd.to_datetime(tx["post_date"], yearfirst=True)
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

    def get_commodity_stock_values(self) -> pd.DataFrame:
        """_summary_

        Returns:
            pd.DataFrame: _description_
        """
        df = (
            self.get_stock()
            .groupby("commodity_guid")
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
        invoices_sql = self.pdw.read_sql_file("sql/invoices_master.sql")
        invoices = self.pdw.df_fetch(
            invoices_sql, parse_dates=["date_posted", "date_opened", "entry_date"]
        )
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

    def get_executive_summary(self):

        df = (
            self.get_farm_cash_transactions(include_depreciation=True)
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
        df.loc["DEPRECIATION", "order"] = 40
        df.loc["NET INCOME", "order"] = 50
        df.loc["NET INCOME", "amt"] = (
            df.loc["OIBDA", "amt"] + df.loc["DEPRECIATION", "amt"]
        )

        return df.sort_values("order")[:5].reset_index().reindex().drop(columns="order")

    def get_1099_vendor_report(self):
        """Pulls 1099 Vendors from database and drops the data in an
        Excel file to be shipped off for 1099 creation
        """
        tax_1099_vendors = self.pdw.read_sql_file("sql/1099_vendors.sql")
        vendors = self.pdw.df_fetch(tax_1099_vendors, parse_dates=["post_date"])

        # Filter to transactions from a given year
        vendors = vendors[vendors["post_date"].dt.year == self.year]
        # Drop the time, not needed
        vendors["post_date"] = vendors["post_date"].dt.date

        all_accounts = self.get_all_accounts().reset_index()
        all_accounts.rename(columns={"guid": "acct_guid"}, inplace=True)
        print(all_accounts)
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

    def get_1099_personal_vendors(self):
        pdw_personal = pd_db_wrangler.Pandas_DB_Wrangler()
        pdw_personal.set_connection_string(
            get_gnucash_file_path(books="personal"), db_type="sqlite"
        )

        tax_1099_vendors = pdw_personal.read_sql_file("sql/1099_vendors_personal.sql")
        personal_vendors = pdw_personal.df_fetch(
            tax_1099_vendors, parse_dates=["post_date"]
        )

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

    def get_production(self) -> pd.DataFrame:
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

        df = (
            df[harvest]
            .groupby(["crop", "operation", "operation_id"])[
                "quantity",
            ]
            .sum(numeric_only=True)
            .round(2)
            .rename(index=str, columns={"quantity": "total_bushels"})
            .join(
                self.get_planted_acres(include_operation_id=True),
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
        terms = terms[terms.index.drop_duplicates()]
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
            columns=["amt", "Percentage", "commodity_guid", "operation_id"]
        ).rename(index=str, columns={"operation": "field", "org_name": "owner"})

    def check_for_duplicates(self, nums: list, table: str = "TRANSACTIONS") -> list:
        sql = (
            f"""SELECT * FROM {table} WHERE NUM IN {[str(x) for x in nums]}""".replace(
                "[",
                "(",
            ).replace("]", ")")
        )
        existing_records = self.pdw.df_fetch(sql).set_index("num")
        return [x for x in existing_records.index.to_list()]

    def process_elevator_load_file(self):
        self.elevator = get_config()["Elevator"]
        df = pd.read_csv(
            self.elevator["loads_path"],
            parse_dates=[" Tare Time Stamp", " Gross Time Stamp"],
        )
        df.columns = df.columns.str.strip()
        tix = list(df["Ticket Number"])
        self.check_for_duplicates(tix)

        df = (
            df.groupby(["Ticket Number", "Tare Time Stamp", "Crop Description"])
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
        commodities = (
            self.get_commodity_bids()
            .set_index("crop")
            .rename(columns={"commodity_guid": "currency_guid"})
        )
        df.loc[df["Crop Description"].str.match("CORN"), "crop"] = "Corn"
        df.loc[df["Crop Description"].str.match("BEANS"), "crop"] = "Soybeans"
        df = df.join(commodities)
        df["enter_date"] = datetime.now()
        df["description"] = self.elevator["elevator_name"]
        df = df[~df["num"].isin(tix)]  # filter out the txns already entered
        return df[
            [
                "guid",
                "currency_guid",
                "num",
                "post_date",
                "enter_date",
                "description",
                "Net Units",
            ]
        ].reset_index(drop=True)

    def create_splits_from_loads(self):
        splits_buy = self.process_elevator_load_file()[["guid", "Net Units"]]
        splits_buy.reset_index(inplace=True, drop=True)
        splits_buy.rename(columns={"guid": "tx_guid"}, inplace=True)
        splits_buy["guid"] = ""
        splits_buy["guid"] = splits_buy["guid"].apply(lambda v: uuid4().hex)
        splits_buy["account_guid"] = self.elevator["grain_to_guid"]
        splits_buy["memo"] = "imported from CSV"
        splits_buy["action"] = "Buy"
        splits_buy["reconcile_state"] = "n"
        splits_buy["reconcile_date"] = None
        splits_buy["value_num"] = round(
            (splits_buy["Net Units"] * self.elevator["price"] * 100), 2
        ).astype(int)
        splits_buy["value_denom"] = 100
        splits_buy["quantity_num"] = (splits_buy["Net Units"] * 100).astype(int)
        splits_buy["quantity_denom"] = 100
        splits_buy["lot_guid"] = None
        splits_buy = splits_buy.drop(
            columns=[
                "Net Units",
            ]
        )

        splits_sell = splits_buy.copy(deep=True)
        splits_sell["action"] = "Sell"
        splits_sell["account_guid"] = self.elevator["grain_from_guid"]
        splits_sell["quantity_num"] = splits_sell["quantity_num"] * -1
        splits_sell["value_num"] = splits_sell["value_num"] * -1
        splits_sell["guid"] = splits_sell["guid"].apply(lambda v: uuid4().hex)

        splits = pd.concat([splits_buy, splits_sell])
        splits.reset_index(inplace=True, drop=True)
        splits["reconcile_date"] = "1970-01-01 00:00:00"
        splits["lot_guid"] = None

        assert splits.sum()["value_num"] == 0

        return splits

    def sanity_checker(self) -> bool:
        all_tx = self.get_all_cash_transactions()
        tx = self.get_farm_cash_transactions()

        # *** TOTALS ***
        account_totals = tx.groupby(["account_type"]).sum(numeric_only=True)
        net_cash_flow = round(account_totals.sum(numeric_only=True)[0], 2)
        account_totals.loc["TOTAL (NET)"] = net_cash_flow

        last_year_mask = all_tx["post_date"].dt.year < self.year
        year_mask = all_tx["post_date"].dt.year <= self.year
        chk_mask = all_tx["src_type"].str.match("(BANK)|(CREDIT)|(CASH)")
        # chk_mask = all_tx["src_code"].str.match("100")
        ar_mask = all_tx["src_type"].str.match("RECEIVABLE")
        ap_mask = all_tx["src_type"].str.match("PAYABLE")

        # Get ending checking balances, ensure consistency with
        # Balance Sheet from GNUCash
        ending_chk_bal = round(
            all_tx[chk_mask & year_mask]["amt"].sum(numeric_only=True), 2
        )
        ending_ap_bal = round(
            all_tx[ap_mask & year_mask]["amt"].sum(numeric_only=True), 2
        )
        ending_ar_bal = round(
            all_tx[ar_mask & year_mask]["amt"].sum(numeric_only=True), 2
        )
        last_year_bal = round(
            all_tx[chk_mask & last_year_mask]["amt"].sum(numeric_only=True), 2
        )
        last_year_ar_ap_bal = round(
            all_tx[(ar_mask | ap_mask) & last_year_mask]["amt"].sum(numeric_only=True),
            2,
        )
        net_ar_ap = round(ending_ap_bal + ending_ar_bal, 2)
        net = round(net_cash_flow + last_year_ar_ap_bal + last_year_bal - net_ar_ap, 2)

        print(
            "{} Ending cash balance was:                   {}".format(
                self.year - 1, last_year_bal
            )
        )
        print(
            "{} Ending AR/AP balance was:                 {}".format(
                self.year - 1, last_year_ar_ap_bal
            )
        )
        sanity = net == ending_chk_bal
        print(
            "{} Finpack net inflows and outflows:  (+){}".format(
                self.year, net_cash_flow
            )
        )
        print(
            "{} ending AR/AP balance:              (+){}".format(self.year, net_ar_ap)
        )
        print("{} Finpack net minus AR/AP balance:   (=){}".format(self.year, net))
        print("-----------------------------------------------------")
        print(
            "{} Ending balance sheet balance was: {}".format(self.year, ending_chk_bal)
        )
        print(" -- We balance, right? ----------------- {}".format(sanity))
        print("Difference = {}".format(round(net - ending_chk_bal, 2)))
        return sanity


# year = 2022
# gda = GnuCash_Data_Analysis()

# print(gda.process_elevator_load_file())
# print(gda.create_splits_from_loads())
# acct_types = ["RECEIVABLE", "PAYABLE", "BANK", "CREDIT", "CASH"]
# cash_accounts = gda.fetch_transactions(acct_types)
# print(cash_accounts)
# print(gda.get_stock())
# all_tx = gda.get_all_cash_transactions(year)
# tx = gda.get_farm_cash_transactions(year)
# tx_w_depr = pd.concat([tx, depr])
# tx_w_depr.reset_index(inplace=True)
# tx_w_depr.sort_values(by=["account_code", "post_date"], inplace=True)

# balance_sheet = gda.get_balance_sheet()
# balance_sheet = balance_sheet[balance_sheet["post_date"].dt.year <= year]
# print(balance_sheet)
# print(balance_sheet.groupby("balance_sheet_category").sum())

# prices = gda.get_commodity_prices()
# print(prices)

# all_accounts = gda.get_all_accounts()
# commodities = all_accounts["commodity_guid"].unique()
# print(commodities.tolist())
