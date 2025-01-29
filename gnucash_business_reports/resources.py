import shutil
import requests
from pathlib import Path
from .config import get_config
from .builder import GnuCash_Data_Analysis


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


def get_linked_documents_for_capital():
    invoices = gda.get_invoices()
    return (
        invoices[invoices["account_code"].str.startswith("15")][
            ["inv_id", "linked_document"]
        ]
        .drop_duplicates()["linked_document"]
        .to_dict()
    )


gda = GnuCash_Data_Analysis()
gda.year = 2024
JOPLIN_HOST = "http://localhost:41184/"
JOPLIN_SUFFIX = get_joplin_token_suffix()

notes = get_linked_documents_for_capital()

for note in notes.values():
    note_id = extract_id(note)
    note_resources = requests.get(
        f"{JOPLIN_HOST}notes/{note_id}/resources/{JOPLIN_SUFFIX}"
    ).json()
    print(note_resources)
    for note in note_resources.items():
        if note[0] == "items":
            note_items = note[1]
            # print(note[0])
            # print(note[1][0])
            for resource in note_items:
                print(resource["id"])
                resource_file = requests.get(
                    # f"{JOPLIN_HOST}resources/{resource["id"]}/{JOPLIN_SUFFIX}").json()
                    f"{JOPLIN_HOST}resources/{resource["id"]}/{JOPLIN_SUFFIX}&fields=id,title,file_extension"
                ).json()
                print(resource_file)
                local_filename = f"{get_invoice_path()}/{note_id[-4:]}-{resource_file["id"][-4:]}-{resource_file["title"]}"
                print(f"downloading file {local_filename}...")
                note_file_url = (
                    f"{JOPLIN_HOST}resources/{resource["id"]}/file/{JOPLIN_SUFFIX}"
                )
                # download_file(note_file_url, local_filename)
