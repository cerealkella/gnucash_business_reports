import time
import keyring
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from .config import get_config
from .logger import log


options = Options()
options.add_argument('--headless=new')

class Production:
    config = get_config()["Production"]
    driver = webdriver.Chrome(options=options)  # Or Firefox(), or Ie(), or Opera()
    driver.get(config["webpage"])

    def __init__(self):
        time.sleep(8)
        # Find username and password elements
        username = self.driver.find_element(By.ID, "idp-discovery-username")
        # Log in
        username.send_keys(self.config["username"])
        username.submit()
        time.sleep(5)
        password = self.driver.find_element(By.ID, "okta-signin-password")
        password.send_keys(keyring.get_password("production", self.config["username"]))
        password.submit()
        log.info("Logged in!")
        time.sleep(10)

    def get_production_report(self):
        self.driver.get(self.config["reports"])
        log.info("Loading reports module (30 seconds)...")
        time.sleep(30)
        self.driver.find_element(By.ID, "share-export-dropdown-selector").click()
        time.sleep(10)
        self.driver.find_element(By.ID, "download-reports").click()
        time.sleep(10)
        log.info("Downloading work reports...")
        self.driver.find_element(By.CLASS_NAME, "download-report-button").click()
        log.info("XLSX Report downloaded!")
        time.sleep(1)

    def close_browser(self):
        time.sleep(1)
        log.info("All done, closing browser!")
        self.driver.close()

    
production = Production()
production.get_production_report()
production.close_browser()
