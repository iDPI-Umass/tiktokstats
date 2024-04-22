import threading
from time import sleep
import datetime
import asyncio
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Thread

from id import generate_ids_from_date

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

# _DEFAULT_POOL = ThreadPoolExecutor()


# def threadpool(f, executor=None):
#     @wraps(f)
#     def wrap(*args, **kwargs):
#         return asyncio.wrap_future((executor or _DEFAULT_POOL).submit(f, *args, **kwargs))
#     return wrap


# @threadpool

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
        driver = get_driver(reset_driver)
        reset_driver = False
        driver.get(url)
        # print(driver.title)
        if driver.title == "Access Denied":  # ’s videos with | TikTok
            # print("access denied")
            reset_driver = True
            driver.close()
            sleep(5)
        else:
            if len(driver.current_url) > 50:
                print(driver.current_url)
            if driver.title in ['Log in | TikTok',
                                'This video is unavailable. Visit TikTok to discover more trending videos.']:
                return driver.current_url
            if "TikTok" in driver.title:
                return driver.current_url
        tries += 1
    #     current_url = driver.current_url
    print(f"returning none for {url}")
    return ""

with ThreadPoolExecutor(max_workers=5) as executor:
    time = datetime.datetime.now()
    futures = [executor.submit(check_url, f"https://www.tiktok.com/@/video/{url}") for url in generate_ids_from_date(10000, 2024, 3, 15)]
    results = [future.result() for future in as_completed(futures)]
    print(datetime.datetime.now() - time)
    print(results)
    print([result for result in results if len(result)>50])

    # executor.map(some_long_calculation, generate_ids_from_date(100, 2024, 3, 15))
#
# results = []
#
# def worker(q):
#     while not q.empty():
#         loop = asyncio.get_event_loop()
#         id = q.get()
#         url = f"https://www.tiktok.com/@/video/{id}"
#         final_url = loop.run_until_complete(some_long_calculation(url))
#         results.append(final_url)
#         print(final_url)
#         q.task_done()
#
#
# q = Queue(maxsize=0)
# for video_id in generate_ids_from_date(100000, 2024, 3, 15):
#     q.put(video_id)
# threads = []
# for i in range(10):
#     work_thread = Thread(target=worker, args=(q,))
#     threads.append(work_thread)
#     work_thread.start()
# q.join()

# def main():
#     results = []
#     url = "https://www.tiktok.com/@/video/7350639310271532293"
#     # for i in range(50):
#     for id in generate_ids_from_date(50, 2024, 3, 15):
#         # url = f"https://www.tiktok.com/@/video/{id}"
#         results.append(loop.run_until_complete(some_long_calculation(url)))
#         print(results[-1])
#     return results
#
