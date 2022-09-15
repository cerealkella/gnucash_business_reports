import pandas as pd
import pd_db_wrangler
import tomli
from tabulate import tabulate
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from dateutil.relativedelta import relativedelta
from uuid import uuid4
from .config import get_datadir, get_gnucash_file_path


class GnuCash_Data_Analysis:
    def __init__(self, cached_mode=False):
        self.CACHED_MODE = cached_mode
        self.data_directory = get_datadir()
        # Set Reporting year constant
        self.year = 2022
        # Suppress warnings, format numbers
        pd.options.mode.chained_assignment = None  # default='warn'
        pd.set_option("display.float_format", lambda x: "%.2f" % x)
        self.pdw = pd_db_wrangler.Pandas_DB_Wrangler()
        self.pdw.set_connection_string(get_gnucash_file_path(), db_type="sqlite")

    def get_all_accounts(self):
        """
        Optionally Override cached mode

        True = use CSV files
        False = use live data from database
        """
        if self.CACHED_MODE:
            all_accounts = pd.read_csv(
                f"{self.data_directory}/ALL_ACCOUNTS.csv", dtype={"code": object}
            )
        else:
            all_accounts = self.pdw.df_fetch(
                self.pdw.read_sql_file("sql/all_accounts.sql")
            )
            all_accounts.to_csv(f"{self.data_directory}/ALL_ACCOUNTS.csv", index=False)

        all_account_types = all_accounts["account_type"].unique().tolist()
        id_name_dict = dict(zip(all_accounts["guid"], all_accounts["name"]))
        parent_dict = dict(zip(all_accounts["guid"], all_accounts["parent_guid"]))

        def find_parent(x):
            """Function to recursively determine parents for accounts"""
            value = parent_dict.get(x, None)
            if value is None:
                return ""
            else:
                # In case there is a id without name.
                if id_name_dict.get(value, None) is None:
                    return "" + find_parent(value)
                return str(id_name_dict.get(value)) + ">" + find_parent(value)

        def get_third_level(x):
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

        return all_accounts

    def get_prices(self):
        if self.CACHED_MODE:
            query = "SELECT * FROM prices"
            prices = self.pdw.df_fetch(query, parse_dates=["date"])
            prices.to_csv(f"{self.data_directory}/PRICES.csv")
        else:
            prices = pd.read_csv(
                f"{self.data_directory}/PRICES.csv", parse_dates=["date"]
            )
        return prices


gda = GnuCash_Data_Analysis()
all_accounts = gda.get_all_accounts()
print(all_accounts)
