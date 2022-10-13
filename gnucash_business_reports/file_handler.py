import logging
import os
import time
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .builder import GnuCash_Data_Analysis
from .config import get_config

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)


class MyHandler(FileSystemEventHandler):
    def __init__(self):
        config = get_config()["Elevator"]
        self.match_len = len(config["file_match_pattern"])
        if self.match_len > 0:
            self.pattern_match = config["file_match_pattern"]
        else:
            self.pattern_match = None
        self.gda = GnuCash_Data_Analysis()

    def valid_file(self, path):
        """Ensure file is completely written before processing"""
        size_past = -1
        while True:
            size_now = os.path.getsize(path)
            if size_now == size_past:
                logging.info(f"File xfer complete. Size={size_now}")
                return size_now
            else:
                size_past = os.path.getsize(path)
                logging.info(f"File transferring...{size_now}")
                time.sleep(1)
        return -1

    def _event_handler(self, path):
        try:
            # allow file to fully write
            if self.valid_file(path) > -1:
                downloaded_file = Path(path)
                if (
                    downloaded_file.name[: self.match_len] == self.pattern_match
                    or self.pattern_match == None
                ):
                    if downloaded_file.suffix.lower() == ".csv":
                        time.sleep(5)  # give the file time to write
                        self.gda.create_db_records_from_load_file(
                            downloaded_file, write_to_db=True
                        )
            else:
                logging.warning("invalid or incomplete file")
        except FileNotFoundError as e:
            logging.warning("temp file deleted, ignoring")

    def on_created(self, event):
        logging.info(f"{event.event_type} -- {event.src_path}")
        self._event_handler(event.src_path)


def watcher(path=Path.joinpath(Path.home(), "Downloads")):
    event_handler = MyHandler()
    logging.warning(f"Monitoring directory: {path}")
    observer = Observer()
    observer.schedule(event_handler, path=path, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


watcher()
