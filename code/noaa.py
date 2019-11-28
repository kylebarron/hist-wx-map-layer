
import lxml
import pandas as pd
from datetime import datetime, timedelta

order_id = 'HAS011395379'
def get_files_for_order(order_id, hours_done=5):
    url = f'https://www1.ncdc.noaa.gov/pub/has/{order_id}/'
    df = read_apache_directory_listing(url)

    extract_done = df['date'].max() + timedelta(hours=hours_done) < datetime.now()



    df

def read_apache_directory_listing(url):
    """Helper to read standard Apache HTML directory listing
    """

    dfs = pd.read_html(url, skiprows=2, header=0)
    assert len(dfs) == 1
    df = dfs[0]

    # Keep middle 3 columns
    df = df.iloc[:,1:4]

    # Rename columns
    df.columns = ['name', 'date', 'size']

    # Cast string dates to datetime objects
    df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y %H:%M')

    # Remove empty last row
    df = df.iloc[:-1,:]

    return df














import os
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()

from selenium import webdriver
from selenium.webdriver.common.by import By
import selenium.webdriver.support.expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import ElementNotVisibleException
from time import sleep

def main():
    email = os.getenv("EMAIL")
    order_id = 'HAS011397020'

order_id = 'HAS011395969'
def check_order_status(order_id, email):
    url = 'https://www.ncdc.noaa.gov/has/HAS.CheckOrderStatus?'
    url += f'hasreqid={order_id}'
    url += f'&emailadd={email}'

    driver = webdriver.Chrome()
    driver.get(url)
    wait_for_status_div(driver)
    soup = BeautifulSoup(driver.page_source)
    soup.find(id='progressbar').attrs['value']

    r = requests.get(url)
    soup = BeautifulSoup(r.content)
    soup = BeautifulSoup(r.content)
    soup.find(class_='has-progress-bar')

def wait_for_status_div(driver):
    try:
        status_div = driver.find_element_by_class_name('has-progress-bar')
    except ElementNotVisibleException:
        sleep(1)
        wait_for_status_div(driver)

    # Now the status div is _visible_, but the status bar might not show 100%
    # yet because of the animation
    dir(driver.find_element_by_id('progressbar'))

    sleep(8)
