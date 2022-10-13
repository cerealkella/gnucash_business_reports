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


def get_logdir() -> Path:
    """Find logdir, create it if it doesn't exist"""
    logdir = Path(AppDirs(__package__).user_log_dir)
    try:
        logdir.mkdir(parents=True)
    except FileExistsError:
        pass
    return logdir


def get_config() -> dict:
    with open(get_datadir().joinpath("config.toml"), "rb") as f:
        return tomli.load(f)


def get_gnucash_file_path(books: str = "business") -> Path:
    """Parse config file from datadir
    This function assumes a config.toml file exists in the local datadir

    Args:
        books (str, optional): Which books to process. The books are
        defined in the config.toml file, "business" and "personal" are the
        two options
        Defaults to "business".

    Returns:
        Path: Path to business or personal data file
    """
    return get_config()["GNUCash"][f"{books}_path"]
