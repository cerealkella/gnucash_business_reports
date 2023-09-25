import time

import keyring
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

from .config import get_config
from .logger import log

from .builder import GnuCash_Data_Analysis


class Elevator():
    config = get_config()["Elevator"]
    driver = webdriver.Chrome()  # Or Firefox(), or Ie(), or Opera()
    driver.get(config["webpage"])

    def __init__(self):
        # Find username and password elements
        username = self.driver.find_element("name", "Z0CNO")
        password = self.driver.find_element("name", "Z0GTOT3")

        # Log in
        username.send_keys(self.config["username"])
        password.send_keys(keyring.get_password("elevator", self.config["username"]))
        password.submit()
        log.info("Logged in!")
    
    def close_browser(self):
        time.sleep(1)
        log.info("All done, closing browser!")
        self.driver.close()

    def list_contracts(self):
        time.sleep(5)
        log.info("Loading grain contracts!")
        self.driver.find_element(
            By.XPATH, "/html/body/div/table[1]/tbody/tr/td[3]/form/input[5]"
        ).submit()
        time.sleep(5)
        log.info("Listing Loads!")
        self.driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div[1]/ul/li[4]/a").click()

    def download_scale_tix(self):
        self.list_contracts()
        time.sleep(2)
        gnuc = GnuCash_Data_Analysis()
        x = 2
        try:
            while x < 15:
                scale_tickets = []
                scale_tickets.append(int(self.driver.find_element(By.XPATH, f"/html/body/div[2]/div[2]/div[2]/table/tbody/tr[{x}]/td[2]/a").text))
                log.info(scale_tickets[0])
                existing_ticket = gnuc.get_existing_records(scale_tickets)
                log.info(len(existing_ticket))
                x += 1 
        except NoSuchElementException:
            log.info("End of list")
            return 0
        # self.close_browser()

    def download_elevator_csv(self):
        self.list_contracts()
        time.sleep(5)
        log.info("Downloading .csv!")
        self.driver.find_element(
            By.XPATH, "/html/body/div[2]/div[2]/div[2]/div/div[1]/a"
        ).click()


darth_elevator = Elevator()
darth_elevator.download_elevator_csv()
