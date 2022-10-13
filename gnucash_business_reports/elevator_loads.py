import time

import keyring
from selenium import webdriver
from selenium.webdriver.common.by import By

from .config import get_config
from .logger import log


def download_elevator_csv():
    config = get_config()["Elevator"]
    driver = webdriver.Chrome()  # Or Firefox(), or Ie(), or Opera()
    driver.get(config["webpage"])

    # Find username and password elements
    username = driver.find_element("name", "Z0CNO")
    password = driver.find_element("name", "Z0GTOT3")

    # Log in
    username.send_keys(config["username"])
    password.send_keys(keyring.get_password("elevator", config["username"]))
    password.submit()
    log.info("Logged in!")

    time.sleep(5)
    log.info("Loading grain contracts!")
    driver.find_element(
        By.XPATH, "/html/body/div/table[1]/tbody/tr/td[3]/form/input[5]"
    ).submit()

    time.sleep(5)
    log.info("Listing Loads!")
    driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div[1]/ul/li[4]/a").click()

    time.sleep(5)
    log.info("Downloading .csv!")
    driver.find_element(
        By.XPATH, "/html/body/div[2]/div[2]/div[2]/div/div[1]/a"
    ).click()

    time.sleep(5)
    log.info("All done, closing browser!")
    driver.close()


download_elevator_csv()
