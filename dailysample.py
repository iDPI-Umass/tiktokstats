import os
import json
import argparse
import threading
from tqdm import tqdm
from time import sleep
from selenium import webdriver
from datetime import datetime
from selenium.webdriver.common.by import By
from tiktoktools.metadata import extract_metadata
from tiktoktools.time import generate_random_timestamp
from tiktoktools.id import generate_ids_from_timestamp
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium.webdriver.chrome.service import Service as ChromeService
from tiktoktools import ROOT_DIR, initialize_collection, JAN_1_2018, TIME_NOW


parser = argparse.ArgumentParser()
parser.add_argument("-s", "--samplesize", type=int, help="number of IDs to sample per second")
parser.add_argument("-t", "--threads", type=int, help="number of threads to use")
parser.add_argument("-b", "--begintimestamp", type=int, help="linux timestamp to start sampling from")
parser.add_argument("-e", "--endtimestamp", type=int, help="linux timestamp to end sampling at")
parser.add_argument("-i", "--incrementershortcut", action="store_true", help="take incrementer shortcut")
args = parser.parse_args()

sample_size, threads, begin_timestamp, end_timestamp, incrementer_shortcut = 50000, 15, JAN_1_2018, TIME_NOW, False
collection = f"random_tiktok_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
thread_local = threading.local()

if args.samplesize is not None:
    sample_size = args.samplesize
if args.threads is not None:
    threads = args.threads
if args.begintimestamp is not None:
    begin_timestamp = args.begintimestamp
if args.endtimestamp is not None:
    end_timestamp = args.endtimestamp
if args.incrementershortcut:
    incrementer_shortcut = args.incrementershortcut


def get_driver(reset_driver=False):
    """
    manage selenium webdriver instances for each running thread
    :param reset_driver:
    :return:
    """
    driver = getattr(thread_local, 'driver', None)
    if reset_driver:
        if driver is not None:
            driver.quit()
            sleep(5)
        setattr(thread_local, 'driver', None)
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
    """
    Check if url exists
    :param url:
    :return: dict: {"id": video id, "url": page url, "title": page title, "statusCode": status code in response,
                    "statusMsg": status message in response}
    """
    video_id = url.split("/")[-1]
    tries, reset_driver = 0, False
    current_title, current_errormsg = "", ""
    while tries < 4:
        try:
            driver = get_driver(reset_driver)
            driver.get(url)
            WebDriverWait(driver, 20).until(expected_conditions.presence_of_element_located((By.CSS_SELECTOR, "script#__UNIVERSAL_DATA_FOR_REHYDRATION__")))
            # driver.implicitly_wait(5)  # wait up to 5 secs just in case things don't load immediately?
            page_source, current_title, current_url = driver.page_source, driver.title, driver.current_url
            if not current_title == "Access Denied":  # and ("s videos with | TikTok" not in current_title):
                metadata_dict, current_statuscode, current_statusmsg = extract_metadata(page_source)
                if (current_url != url and current_statuscode != "0") or current_statuscode == "100004":
                    driver.quit()
                    sleep(5)
                    setattr(thread_local, 'driver', None)
                else:
                    if current_statuscode == "0":
                        with open(os.path.join(ROOT_DIR, "collections", collection, "metadata", f"{video_id}.json"), "w") as f:
                            json.dump(metadata_dict, f)
                    return {
                        "id": str(video_id),
                        "url": current_url,
                        "title": current_title,
                        "statusCode": current_statuscode,
                        "statusMsg": current_statusmsg
                    }
            else:
                driver.quit()
                sleep(5)
                setattr(thread_local, 'driver', None)
        except Exception as e:
            current_errormsg = str(e)
            if "Message: invalid session id" not in current_errormsg:  # "invalid session id" error is fixed with a driver reset
                tqdm.write(f"{video_id} {current_errormsg}")
            driver = getattr(thread_local, 'driver', None)
            if driver is not None:
                driver.quit()
            setattr(thread_local, 'driver', None)
        finally:
            tries += 1
            reset_driver = True
    tqdm.write(f"{video_id} returning none")
    return {
        "id": str(video_id),
        "url": url,
        "title": current_title,
        "statusCode": "ERROR",
        "statusMsg": current_errormsg
    }


def main():
    print(initialize_collection(collection))
    while True:
        random_timestamp = generate_random_timestamp(start_timestamp=begin_timestamp, end_timestamp=end_timestamp)
        all_ids = generate_ids_from_timestamp(random_timestamp, n=sample_size, incrementer_shortcut=incrementer_shortcut)
        print(datetime.utcfromtimestamp(random_timestamp))
        with open(os.path.join(ROOT_DIR, "collections", collection, "queries", f"{random_timestamp}_queries.json"), "w") as f:
            json.dump(all_ids, f)
        with tqdm(total=len(all_ids)) as pbar:
            with ThreadPoolExecutor(max_workers=15) as executor:
                results = []
                futures = [executor.submit(check_url, f"https://www.tiktok.com/@/video/{generated_id}") for
                           generated_id in all_ids]
                for future in as_completed(futures):
                    if future.result()["statusCode"] == "0":
                        tqdm.write(json.dumps(future.result()))
                        # tqdm.write("{:b}".format(int(future.result()["id"])).zfill(64))
                    results.append(future.result())
                    pbar.update(1)
        with open(os.path.join(ROOT_DIR, "collections", collection, "queries", f"{random_timestamp}_hits.json"), "w") as f:
            json.dump(results, f)


if __name__ == "__main__":
    main()
