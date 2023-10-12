import time

import keyring
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

from .builder import GnuCash_Data_Analysis
from .config import get_config
from .logger import log


class Elevator:
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
        self.driver.find_element(
            By.XPATH, "/html/body/div[2]/div[2]/div[1]/ul/li[4]/a"
        ).click()

    def download_scale_tix(self):
        count = 0
        self.list_contracts()
        time.sleep(2)
        gnuc = GnuCash_Data_Analysis()
        x = 2
        exception_count = 0
        while x < 500:
            # arbitrary upper limit. Will exit loop when there's an exception
            try:
                scale_tickets = []
                scale_tickets.append(
                    int(
                        self.driver.find_element(
                            By.XPATH,
                            f"/html/body/div[2]/div[2]/div[2]/table/tbody/tr[{x}]/td[2]/a",
                        ).text
                    )
                )
                log.info(f"Found Scale Ticket {scale_tickets[0]}")
                existing_ticket = gnuc.get_existing_records(scale_tickets)
                if len(existing_ticket) < 1:
                    count += 1
                    log.info(f"Downloading Scale Ticket {scale_tickets[0]}")
                    self.driver.find_element(
                        By.XPATH,
                        f"/html/body/div[2]/div[2]/div[2]/table/tbody/tr[{x}]/td[13]/a"
                    ).click()
                else:
                    log.info(
                        f"Scale Ticket {existing_ticket.index.values[0]} exists in db"
                    )
            except NoSuchElementException:
                log.info(f"No such element at index {x}")
                exception_count += 1
                if exception_count > 5:
                    log.info("End of list")
                    break
            x += 1
        log.info(f"{count} new scale tickets found!")
        return count

    def download_elevator_csv(self):
        time.sleep(5)
        log.info("Downloading .csv!")
        self.driver.find_element(
            By.XPATH, "/html/body/div[2]/div[2]/div[2]/div/div[1]/a"
        ).click()
        return 0
    
    def process_loads(self):
        tix_to_process = self.download_scale_tix()
        if tix_to_process > 0:
            # allow the tickets to get processed into external app
            time.sleep(10 * tix_to_process)
            self.download_elevator_csv()
        # self.close_browser()
        return 0


darth_elevator = Elevator()
darth_elevator.process_loads()
