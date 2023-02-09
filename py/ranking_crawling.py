from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time

# browser = webdriver.Chrome('/usr/local/bin/chromedriver')
browser = webdriver.Chrome('/opt/homebrew/bin/chromedriver')
# browser = webdriver.Chrome('./chromedriver.exe')

browser.implicitly_wait(3)

URL = 'https://signal.bz/news/'
browser.get(url=URL)
# time.sleep(3)

naver_results = browser.find_elements(By.CSS_SELECTOR,'#app > div > main > div > section > div > section > section:nth-child(2) > div > div > div > div > a > span.rank-text')

naver_list = []

for naver_result in naver_results:
    naver_list.append(naver_result.text)
    
print(naver_list)