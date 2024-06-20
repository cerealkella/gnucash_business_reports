import time
from selenium import webdriver
from .config import get_config


driver = webdriver.Firefox()  # Or Firefox(), or Ie(), or Opera()
driver.fullscreen_window()
sites = get_config()["display_websites"]


while True:
    for site in sites:
        driver.get(site["uri"])
        time.sleep(site["duration"])
