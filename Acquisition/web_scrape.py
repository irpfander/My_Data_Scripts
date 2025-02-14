
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from selenium import webdriver
import shutil
import time
import os
import zipfile

def web_scrape_simple(url, file_path, dataset_to_pull, download_xpaths):
    """
    Function to download data with selenium. Loops through a list of xpaths and saves data.
    Args:
        url: link to dataset
        file_path: File path where the data will be saved
        datasets_to_pull: name of dataset name to be pulled
        download_xpaths: list of xpaths to click
    Returns: None

    """

    prefs = {
        "download.default_directory": file_path,
        "download.directory_upgrade": True,
        "download.prompt_for_download": False, }
    chromeOptions = ChromeOptions()
    chromeOptions.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=chromeOptions)
    driver.get(url)
    time.sleep(5)

    # loop through xpaths clicking each
    for xpath in download_xpaths:
        # if first button has not loaded wait 5 seconds and try again
        try:
            driver.find_element(By.XPATH, xpath).click()
        except:
            time.sleep(5)
            driver.find_element(By.XPATH, xpath).click()
        # check if last in list, if so allow longer for download
        if xpath==download_xpaths[-1]:
            seconds = 0
            dl_wait = True
            while dl_wait and seconds < 90:
                time.sleep(1)
                dl_wait = False
                for fname in os.listdir(file_path):
                    if fname.endswith('.crdownload'):
                        dl_wait = True
                seconds += 1


    # get most recent file name and rename
    old_filename = max([file_path + "\\" + f for f in os.listdir(file_path)], key=os.path.getctime)
    shutil.move(old_filename,
                os.path.join(file_path, dataset_to_pull))

    # unzip
    if os.path.join(file_path, dataset_to_pull)[-3:] == 'zip':
        with zipfile.ZipFile(os.path.join(file_path, dataset_to_pull), 'r') as file:
            file.extractall(path=file_path)