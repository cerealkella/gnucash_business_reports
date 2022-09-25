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


def column_type_changer(latex: str, caption: str) -> str:
    """
    For report creation and formatting
    Changes latex tables from tabular to longtblr

    Args:
        latex: latex table
        index: index of horizontal line insertion (in lines)
    """
    latex_lines = latex.splitlines()
    items_to_remove = ["\\toprule", "\midrule", "\\bottomrule"]
    lines = [x for x in latex_lines if x not in items_to_remove]
    new_firstline = f"""
\\begin{{longtblr}}[
theme = fancy,
caption = {{ {caption} }}
 	]{{
 		colspec = {{XXXXr}}, width = 0.95\linewidth,
 		rowhead = 2, rowfoot = 1,
 		row{{odd}} = {{gray9}}, row{{even}} = {{blue9}},
 		row{{1}} = {{green8}}, row{{Z}} = {{blue7}},
 	}}
"""
    lines[0] = new_firstline
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
