import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from .config import get_config
from .builder import GnuCash_Data_Analysis


class MyHandler(FileSystemEventHandler):
    def __init__(self):
        config = get_config()["Elevator"]
        self.match_len = len(config["file_match_pattern"])
        if self.match_len > 0:
            self.pattern_match = config["file_match_pattern"]
        else:
            self.pattern_match = None
        self.gda = GnuCash_Data_Analysis()

    def _event_handler(self, path):
        downloaded_file = Path(path)
        if (
            downloaded_file.name[: self.match_len] == self.pattern_match
            or self.pattern_match == None
        ):
            if downloaded_file.suffix == ".CSV":
                time.sleep(5)  # give the file time to write
                self.gda.create_db_records_from_load_file(
                    downloaded_file, write_to_db=True
                )

    def on_created(self, event):
        print(event.event_type + " -- " + event.src_path)
        self._event_handler(event.src_path)


def watcher(path=Path.joinpath(Path.home(), "Downloads")):
    event_handler = MyHandler()
    print(f"Monitoring directory: {path}")
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
