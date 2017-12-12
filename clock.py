import time
import pandas as pd
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from bs4 import BeautifulSoup
from collections import Counter
from sqlalchemy import create_engine
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium import webdriver
from datetime import datetime



sched = BlockingScheduler()


@sched.scheduled_job('interval', minutes=5)
def timed_job():
    print('This job is run every 5 minutes.')

    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.binary_location = os.environ['GOOGLE_CHROME_BIN']
    chromeOptions.add_argument('--headless')
    downloadDir = os.environ['DOWNLOAD_DIRECTORY']
    prefs = {"download.default_directory" : downloadDir}
    chromeOptions.add_experimental_option("prefs",prefs)
    wd = webdriver.Chrome(executable_path=os.environ['CHROMEDRIVER_PATH'], chrome_options=chromeOptions)

    wd.get('https://thatoneprivacysite.net/vpn-comparison-chart/')
    html_page = wd.page_source

    
    wd.find_element_by_link_text("csv").click()

    time.sleep(2)
    wd.quit()

    print('Downloaded CSV on : ', datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

    db_user = os.environ['DB_USER']
    db_pwd = os.environ['DB_PWD']
    db_host = os.environ['DB_HOST']
    db_schema = os.environ['DB_SCHEMA']
    db_url = 'mysql://' + db_user + ':' + db_pwd + '@' + db_host + ':3306/' + db_schema

    engine = create_engine(db_url)
    conn = engine.connect()

    vpnCSVData = os.path.join(os.environ['DOWNLOAD_DIRECTORY_NAME'],"That One Privacy Guy's VPN Comparison Chart - VPN Comparison.csv")
    vpnCompDataDF = pd.read_csv(vpnCSVData)
    vpnCompDataDF1 = vpnCompDataDF[3:-3]
    vpnCompDataDF2 = vpnCompDataDF1.fillna('Unknown')
    vpnCompDataDF2.columns = vpnCompDataDF2.iloc[0]
    vpnCompDataDF2.reindex(vpnCompDataDF2.index.drop(3))
    vpnCompDataDF3 = vpnCompDataDF2[1:]
    vpnCompDataDF3.reindex
    columns = vpnCompDataDF3.columns 
    columnsList = columns.tolist()
    counts = Counter(columns)
    for s,num in counts.items():
        if num > 1: # ignore strings that only appear once
            for suffix in range(1, num + 1): # suffix starts at 1 and increases by 1 each time
                columnsList[columnsList.index(s)] = s + str(suffix) # replace each appearance of s
    columns = pd.Index(columnsList)
    columns = [i.replace(' ', '_') for i in columns]
    vpnCompDataDF4 = vpnCompDataDF3
    vpnCompDataDF4.columns = columns

    vpnCompDataDF4.to_sql('vpn_master', engine, if_exists='replace', chunksize=10000)
    conn.execute("ALTER TABLE vpn_master DROP COLUMN `index`;")
    conn.execute("ALTER TABLE vpn_master ADD id INT PRIMARY KEY AUTO_INCREMENT;")




@sched.scheduled_job('cron', day_of_week='mon-fri', hour=10)
def scheduled_job():
    print('This job is run every weekday at 10am.')

sched.start()
