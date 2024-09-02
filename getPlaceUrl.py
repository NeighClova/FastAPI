from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import re


def getUrl(url: str):
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('headless')
        options.add_argument('window-size=1920x1080')
        options.add_argument("disable-gpu")

        ChromeDriverManager().install()

        driver = webdriver.Chrome(options=options)
        driver.get(url)
        driver.implicitly_wait(30)

        cu = driver.current_url
        res_code = re.findall(r"place/(\d+)", cu)

    except Exception as e:
        print(e)
        return

    return res_code[0]
