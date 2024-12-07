from selenium.webdriver.common.by import By
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import os
import time
import datetime
import requests
import pandas as pd
import re
import importlib.util

spec_analyze = importlib.util.spec_from_file_location(
    "review_analyze", "review_analyze.py")
review_analyze = importlib.util.module_from_spec(spec_analyze)
spec_analyze.loader.exec_module(review_analyze)


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
        # options.add_argument('headless')
        options.add_argument('window-size=1920x1080')
        options.add_argument("disable-gpu")
        options.add_argument('--start-fullscreen')

        ChromeDriverManager().install()

        driver = webdriver.Chrome(options=options)
        driver.get(url)
        driver.implicitly_wait(30)
        driver.execute_script(
            "document.querySelector('div.flicking-camera').style.display='none';")

        # Pagedown
        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)

        for i in range(0, 3, 1):
            try:
                # 새로운 페이지를 불러오기 위해 스크롤 다운
                more_button = driver.find_element(
                    By.XPATH, '//a[@class="fvwqf"]/span[text()="더보기"]')

                # 스크롤해서 해당 요소가 보이도록 하기
                actions = ActionChains(driver)
                actions.move_to_element(more_button).perform()
                time.sleep(2)

                # 그 후에 클릭 시도
                more_button.click()

            except NoSuchElementException:
                print('-더보기 버튼 모두 클릭 완료-')
                break

        # 새로운 페이지의 리뷰를 가져오기 위해 driver.page_source를 갱신하고 다시 BeautifulSoup으로 파싱
        html = driver.page_source
        bs = BeautifulSoup(html, 'html.parser')
        reviews = bs.select('li.pui__X35jYm.place_apply_pui.EjjAW')

        for r in reviews:
            # 리뷰 가져오는 부분은 그대로 유지
            content = r.select_one('a.pui__xtsQN-')

            # exception handling
            if content:
                content = content.text

                # 정규식으로 전처리
                content_cleaned = re.sub(r'[^가-힣0-9\s!?().,]', '', content)
                time.sleep(0.06)

                if content_cleaned:
                    list_sheet.append([content_cleaned])
                    time.sleep(0.06)

        # Save the file
        rating_df = pd.DataFrame(list_sheet, columns=['content'])
        file_path = os.path.join(
            files_folder, 'review_' + str(place_id) + '.csv')
        rating_df.to_csv(file_path, encoding='utf-8-sig', index=False)

        # 크롤링 완료 후 분석 실행
        print(f"place_id {place_id} 리뷰 분석 실행")
        review_analyze.run_analyze(place_id)  # review_analyze.py 실행

    except Exception as e:
        print(e)
        rating_df = pd.DataFrame(list_sheet, columns=['content'])
        file_path = os.path.join(
            files_folder, 'review_' + str(place_id) + '.csv')
        rating_df.to_csv(file_path, encoding='utf-8-sig', index=False)
