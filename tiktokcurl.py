import re
import subprocess
import time

url = "https://www.tiktok.com/@/video/7350639310271532293"
# url = "https://www.tiktok.com/@/video/7350629310271532293"  # invalid
# result = subprocess.run(["curl", url], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
# output = process_detail.communicate()[0].decode()
# print(result.stdout)
# process_detail.kill()
# canonical_link = re.search("(?P<url>https?://[^\s]+)+\?", result.stdout)  # .group("url")
# print("Canonical link: ", canonical_link)


import requests

headers = {
    "Accept": "*/*",
    "Accept-Encoding": "identity;q=1, *;q=0",
    "Accept-Language": "en-US;en;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "User-Agent": "Mozilla/5.0"
    # 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}
response = requests.get(url, headers=headers)
print(response.url)

# print(requests.head(url, allow_redirects=True).url)
# r = requests.get(url, headers=headers)
# print(r.url)


from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium import webdriver

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--incognito")
chrome_options.add_argument("--disable-dev-shm-usage")
# driver = webdriver.Chrome(
#     ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install(),
#     options=chrome_options,
# )
driver = webdriver.Chrome(options=chrome_options, service=ChromeService(ChromeDriverManager().install()))
driver.get(url)
# time.sleep(10)
print(driver.current_url)
print(driver)