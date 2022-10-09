import keyring
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from .config import get_config


def download_elevator_csv():
    config = get_config()["Elevator"]
    driver = webdriver.Firefox()  # Or Chrome(), or Ie(), or Opera()
    driver.get(config["webpage"])

    # Find username and password elements
    username = driver.find_element("name", "Z0CNO")
    password = driver.find_element("name", "Z0GTOT3")

    # Log in
    username.send_keys(config["username"])
    password.send_keys(keyring.get_password("elevator", config["username"]))
    password.submit()

    # Wait for page to load, and click on Grain Contracts
    time.sleep(5)
    driver.find_element(
        By.XPATH, "/html/body/div/table[1]/tbody/tr/td[3]/form/input[5]"
    ).submit()

    # Wait for page to load, and click on Loads
    time.sleep(10)
    driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div[1]/ul/li[4]/a").click()

    # Wait for page to load, and click on CSV button
    time.sleep(15)
    driver.find_element(
        By.XPATH, "/html/body/div[2]/div[2]/div[2]/div/div[1]/a"
    ).click()

    # Allow file to download, close browser
    time.sleep(5)
    driver.close()
