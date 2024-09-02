from selenium.webdriver.common.by import By
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from openpyxl import Workbook
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
import os
import time
import datetime
import requests
import pandas as pd
import asyncio


def run_crawler(place_id, place_num):

    print(f"place ID: {place_id}에 대한 {place_num}크롤러 실행 중")

    url = 'https://m.place.naver.com/place/' + \
        str(place_num) + '/review/visitor?entry=plt&reviewSort=recent'

    print("*****", url)

    # BS4 setting for secondary access
    session = requests.Session()
    headers = {
        "User-Agent": "user value"}

    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])

    session.mount('http://', HTTPAdapter(max_retries=retries))

    # New xlsx file
    now = datetime.datetime.now()
    list_sheet = []

    # FastAPI 프로젝트 루트 디렉토리의 경로 가져오기
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # files 폴더 경로 설정
    files_folder = os.path.join(base_dir, 'files')

    # files 폴더가 없으면 생성
    if not os.path.exists(files_folder):
        os.makedirs(files_folder)

    # Start crawling/scraping!
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('headless')
        options.add_argument('window-size=1920x1080')
        options.add_argument("disable-gpu")
        options.add_argument('--start-fullscreen')

        ChromeDriverManager().install()

        driver = webdriver.Chrome(options=options)
        driver.get(url)
        driver.implicitly_wait(30)

        # Pagedown
        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)

        while True:
            try:
                # 새로운 페이지를 불러오기 위해 스크롤 다운
                more_button = driver.find_element(
                    By.XPATH, '//a[@class="fvwqf"]/span[text()="더보기"]')
                more_button.click()
                time.sleep(2)  # 새로운 리뷰가 로드될 때까지 기다림

            except NoSuchElementException:
                print('-더보기 버튼 모두 클릭 완료-')
                break

        # 새로운 페이지의 리뷰를 가져오기 위해 driver.page_source를 갱신하고 다시 BeautifulSoup으로 파싱
        html = driver.page_source
        bs = BeautifulSoup(html, 'html.parser')
        reviews = bs.select('li.owAeM')

        for r in reviews:
            # 리뷰 가져오는 부분은 그대로 유지
            content = r.select_one('span.zPfVt')

            # exception handling
            content = content.text if content else ''
            time.sleep(0.06)

            list_sheet.append([content])
            time.sleep(0.06)

        # Save the file
        rating_df = pd.DataFrame(list_sheet, columns=['content'])
        file_path = os.path.join(
            files_folder, 'review_' + str(place_id) + '.csv')
        rating_df.to_csv(file_path, encoding='utf-8-sig', index=False)

    except Exception as e:
        print(e)
        rating_df = pd.DataFrame(list_sheet, columns=['content'])
        file_path = os.path.join(
            files_folder, 'review_' + str(place_id) + '.csv')
        rating_df.to_csv(file_path, encoding='utf-8-sig', index=False)
