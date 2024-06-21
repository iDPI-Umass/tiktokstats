import threading
from time import sleep
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

thread_local = threading.local()


def get_driver(reset_driver=False):
    if reset_driver:
        setattr(thread_local, 'driver', None)
    driver = getattr(thread_local, 'driver', None)
    if driver is None:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(options=chrome_options, service=ChromeService(ChromeDriverManager().install()))
    setattr(thread_local, 'driver', driver)
    return driver


def check_url(url):
    tries = 0
    reset_driver = False
    while tries < 5:
        try:
            driver = get_driver(reset_driver)
            reset_driver = False
            driver.get(url)
            if driver.title == "Access Denied":  # ’s videos with | TikTok
                reset_driver = True
                driver.close()
                sleep(5)
            else:
                return {"id": url[-11:], "is_shorts": "https://www.youtube.com/shorts/" in driver.current_url}
        except Exception as e:
            print(f"{url[-11:]} {str(e)}")
            reset_driver = True
            sleep(5)
        tries += 1
    print(f"returning none for {url[-11:]}")
    return {"id": url[-11:], "is_shorts": False}




df = pd.read_csv("all IDs.csv", header=None)
video_ids = df[0].to_list()

with tqdm(total=len(video_ids)) as pbar:
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = []
        futures = [executor.submit(check_url, f"https://www.youtube.com/shorts/{video_id}") for
                   video_id in video_ids]
        for future in as_completed(futures):
            results.append(future.result())
            pbar.update(1)
pd.DataFrame(results).to_csv("shorts_all_IDs.csv", index=False, header=True)
