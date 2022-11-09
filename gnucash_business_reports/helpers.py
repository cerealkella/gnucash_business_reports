from datetime import datetime
from pandas import DataFrame
import tomli


def add_hline(latex: str, index: int) -> str:
    """
    For report creation and formatting
    Adds a horizontal `index` lines before the last line of the table

    Args:
        latex: latex table
        index: index of horizontal line insertion (in lines)
    """
    lines = latex.splitlines()
    lines.insert(len(lines) - index - 2, r"\midrule")
    return "\n".join(lines).replace("NaN", "")


def column_type_changer(df: DataFrame, caption: str) -> str:
    """
    For report creation and formatting
    Changes latex tables from tabular to longtblr

    Args:
        df: dataframe to convert to latex table
        caption: Caption text to place atop the table

    Returns:
        string containing formatted latex table
    """
    column_count = len(df.axes[1])
    colspec = "Xr".rjust(column_count, "X")
    width = 0.95
    if column_count < 5:  # yeah, I know, this could be better. shut up
        if column_count == 2:
            width = 0.6
        elif column_count == 3:
            width = 0.7
        elif column_count == 4:
            width = 0.8
    latex = (
        df.style.hide(axis="index")
        .format(
            {"Date": lambda t: t.strftime("%Y-%m-%d")},
            decimal=".",
            thousands=",",
            precision=2,
            escape="latex",
        )
        .to_latex(
            hrules=False,
        )
    )
    latex_lines = latex.splitlines()
    items_to_remove = ["\\toprule", "\midrule", "\\bottomrule"]
    lines = [x for x in latex_lines if x not in items_to_remove]
    new_firstline = f"""
\\begin{{longtblr}}[
theme = fancy,
caption = {{ {caption} }}
 	]{{
 		colspec = {{ {colspec} }}, width = {width}\linewidth,
 		rowhead = 1, rowfoot = 1,
 		row{{odd}} = {{odd-row}}, row{{even}} = {{even-row}},
 		row{{1}} = {{header-row}}, row{{Z}} = {{footer-row}},
 	}}
"""
    lines[0] = new_firstline
    # trim off the trailing \\s on last row
    lines[len(lines) - 2] = lines[len(lines) - 2][:-2]
    lines[len(lines) - 1] = "\end{longtblr}"

    return "\n".join(lines).replace("NaN", "")


def column_filler(df: DataFrame) -> list:
    """
    For report creation and formatting
    Fills blank columns with blank strings for use with loc[]

    Args:
        df: DataFrame
    """
    blank_columns = []
    column_count = len(df.columns)
    for column in range(column_count - 2):
        blank_columns.append("")
    return blank_columns


def get_keys(toml_string: str, section: str) -> dict:
    """
    Gets keys from a toml string
    Originally written and used for parsing toml in GnuCash
    Account Notes for the purposes of calculating depreciation

    Args:
        toml_string (str): string containing toml
        section (str): Section of toml to search e.g. "Depreciation"

    Returns:
        dict containing keys
    """
    toml_dict = tomli.loads(toml_string)
    return toml_dict[section].keys()


def parse_toml(toml_string: str, section: str, key: str) -> dict:
    """Parses toml to obtain the toml for a given key

    Args:
        toml_string (str): _description_
        section (str): Section of toml to search e.g. "Depreciation"
        key (str): _description_

    Returns:
        dict: dict containing toml
    """
    try:
        toml_dict = tomli.loads(toml_string)
        return toml_dict[section][key]
    except tomli.TOMLDecodeError:
        return None


def nearest(items: list, pivot: datetime) -> datetime:
    """Function to find the nearest date in a list of dates
    Obligatory hat tip to StackOverflow
    https://stackoverflow.com/questions/32237862/find-the-closest-date-to-a-given-date

    Args:
        items (list): list of dates (typically from a pandas df
        using the .tolist() function)
        pivot (datetime): date for which you'd like to find the nearest
        value

    Returns:
        datetime: the date nearest, which can then be used for indexing purposes.
    """
    return min(items, key=lambda x: abs(x - pivot))
