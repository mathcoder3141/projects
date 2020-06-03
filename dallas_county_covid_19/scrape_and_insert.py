#!/home/randall/anaconda3/bin/python

import requests
import time
import psycopg2
import os
import datetime
from contextlib import contextmanager
from bs4 import BeautifulSoup
import re

# url to scrape
DCHHS_URL = 'https://www.dallascounty.org/departments/dchhs/2019-novel-coronavirus.php'

# get html of page
DCHHS_HTML = requests.get(DCHHS_URL)

soup = BeautifulSoup(DCHHS_HTML.text, "html.parser")

paragraph_tags = soup.find_all('p')
statement = paragraph_tags[5].text.split(' ')

TABLE_TAGS = soup.find_all("table")
COVID_STATISTICS = TABLE_TAGS[1].find_all('td')

# statistics
NEW_CASES = int(re.search(r"\d{1,3}(?=.additional)", paragraph_tags[5].text).group())
TOTAL_DEATHS = int(COVID_STATISTICS[-2].text)
TOTAL_CASES = int(COVID_STATISTICS[-3].text.replace("*", "").replace(",", "").strip())
image_tags = soup.find_all("img")
RISK_LEVEL = image_tags[-5]['title'].split('-')[-1].strip().lower()
CURRENT_TIMESTAMP = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

LOCAL_PSQL_PASSWORD=os.environ.get("COVID")

@contextmanager
def psql():
    DSN = "host=/var/run/postgresql port=5432 dbname=covid user=randall password={}".format(
        LOCAL_PSQL_PASSWORD
    )

    connection = psycopg2.connect(DSN)
    yield connection
    connection.close()

with psql() as connection, connection.cursor() as cursor:
    SQL_INSERT = """
    INSERT INTO dallas_covid (new_cases, total_cases, total_deaths, risk_level, county, created_at) VALUES (%s, %s, %s, %s, %s, %s)
    """
    RECORD = (NEW_CASES, TOTAL_CASES, TOTAL_DEATHS, RISK_LEVEL, 'Dallas', CURRENT_TIMESTAMP)
    cursor.execute(SQL_INSERT, RECORD)
    connection.commit()
