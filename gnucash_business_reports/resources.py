import shutil

import requests

from pathlib import Path
from .config import get_config


def download_file(url: str, path: Path):
    with requests.get(url, stream=True) as r:
        with open(path, "wb") as f:
            shutil.copyfileobj(r.raw, f)


def get_invoice_path() -> Path:
    return Path(get_config()["Paths"]["invoices"])


def get_joplin_token() -> str:
    return get_config()["Joplin"]["joplin_api_token"]


def get_joplin_token_suffix():
    return "?token=" + get_joplin_token()


def extract_id(uri: str):
    joplin_uri_prefix = get_config()["Joplin"]["joplin_uri_prefix"]
    return uri.split(joplin_uri_prefix)[1]


note_id = extract_id(
    "joplin://x-callback-url/openNote?id=f1e1bc900a9d4356a456d14834ff1c2a"
)

JOPLIN_HOST = "http://localhost:41184/"
JOPLIN_SUFFIX = get_joplin_token_suffix()


note_resources = requests.get(
    f"""{JOPLIN_HOST}notes/{note_id}/resources/{JOPLIN_SUFFIX}"""
).json()
resource_id = note_resources["items"][0]["id"]
print(resource_id)
note_file = requests.get(
    f"""{JOPLIN_HOST}resources/{resource_id}/{JOPLIN_SUFFIX}"""
).json()
local_filename = f"""{get_invoice_path()}/{note_file["title"]}"""
note_file_url = f"""{JOPLIN_HOST}resources/{resource_id}/file/{JOPLIN_SUFFIX}"""
download_file(note_file_url, local_filenamegit)
