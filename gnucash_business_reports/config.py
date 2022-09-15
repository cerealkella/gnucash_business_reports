from pathlib import Path
from appdirs import AppDirs
import tomli


def get_datadir() -> Path:
    """Find datadir, create it if it doesn't exist"""
    datadir = Path(AppDirs(__package__).user_data_dir)
    try:
        datadir.mkdir(parents=True)
    except FileExistsError:
        pass
    return datadir


def get_gnucash_file_path() -> Path:
    """Parse config file from datadir
    This function assumes a config.toml file exists in the local datadir
    """
    with open(get_datadir().joinpath("config.toml"), "rb") as f:
        toml_dict = tomli.load(f)
        return toml_dict["GNUCash"]["file_path"]
