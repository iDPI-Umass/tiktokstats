import json
import os
import stat
import threading
import pandas as pd
from tqdm import tqdm
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import date, timedelta, datetime
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from id import get_random_timestamp, generate_ids_from_timestamp
from selenium.webdriver.chrome.service import Service as ChromeService

from metadata import download_metadata

ROOT_DIR = os.path.realpath(os.path.dirname(__file__))

collection = f"random_tiktok_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"


def initialize_collection(collection_name: str):
    if not os.path.exists(os.path.join(ROOT_DIR, "collections")):
        os.makedirs(os.path.join(ROOT_DIR, "collections"))
        try:
            os.chmod(os.path.join(ROOT_DIR, "collections"),
                     stat.S_IRWXU | stat.S_IRGRP | stat.S_IRWXO)
        except Exception as e:
            print(e)
    if not os.path.exists(os.path.join(ROOT_DIR, "collections", collection_name)):
        os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name))
        os.chmod(os.path.join(ROOT_DIR, "collections", collection_name),
                 stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        # os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name, "logs"))
        # os.chmod(os.path.join(ROOT_DIR, "collections", collection_name, "logs"),
        #          stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name, "metadata"))
        os.chmod(os.path.join(ROOT_DIR, "collections", collection_name, "metadata"),
                 stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name, "queries"))
        os.chmod(os.path.join(ROOT_DIR, "collections", collection_name, "queries"),
                 stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        # os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name, "transcripts"))
        # os.chmod(os.path.join(ROOT_DIR, "collections", collection_name, "transcripts"),
        #          stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        # os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name, "wavs"))
        # os.chmod(os.path.join(ROOT_DIR, "collections", collection_name, "wavs"),
        #          stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    return os.path.join(ROOT_DIR, "collections", collection_name)


def date_range(start_date, end_date):
    for n in range(int((end_date-start_date).days)):
        yield start_date + timedelta(n)

thread_local = threading.local()

def get_driver(reset_driver=False):
    if reset_driver:
        setattr(thread_local, 'driver', None)
    driver = getattr(thread_local, 'driver', None)
    # print(driver)
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
    :return: dict: {"id": video id, "url": page url, "title": page title, "is_video": bool}
    """
    video_id = url.split("/")[-1]
    tries = 0
    reset_driver = False
    current_title = ""
    while tries < 4:
        try:
            driver = get_driver(reset_driver)
            reset_driver = False
            driver.get(url)
            driver.implicitly_wait(5)
            current_title = driver.title
            # tqdm.write(current_title)
            if driver.title == "Access Denied" or "s videos with | TikTok" in driver.title:
                reset_driver = True
                driver.quit()
                sleep(5)
            else:
                is_video = url != driver.current_url  # valid videos will autofill uploader username in url
                if is_video:
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    rehydration_script_elements = soup.select('script#__UNIVERSAL_DATA_FOR_REHYDRATION__')
                    if len(rehydration_script_elements) == 1:
                        rehydration_dict = json.loads(rehydration_script_elements[0].text)
                        with open(os.path.join(ROOT_DIR, "collections", collection, "metadata", f"{video_id}.json"), "w") as f:
                            json.dump(rehydration_dict, f)
                    # metadata_dict = download_metadata(driver.current_url)  # yt-dlp download (slow!)
                    # with open(f"metadata/{video_id}.json", "w") as f:
                    #     json.dump(metadata_dict, f)
                return {"id": str(video_id), "url": driver.current_url, "title": driver.title, "is_video": is_video}
        except Exception as e:
            if "Message: invalid session id" not in str(e):  # "invalid session id" error is fixed with a driver reset
                tqdm.write(f"{video_id} {str(e)}")
            reset_driver = True
            driver.quit()
            sleep(5)
        tries += 1
    # if check_url fails multiple times, ID is likely associated with private video
    tqdm.write(f"{video_id} returning none")
    return {"id": str(video_id), "url": url, "title": current_title, "is_video": False}


print(initialize_collection(collection))
while True:
    random_timestamp = get_random_timestamp()
    all_ids = generate_ids_from_timestamp(random_timestamp, incrementer_shortcut=False)
    print(datetime.utcfromtimestamp(random_timestamp))
    with open(os.path.join(ROOT_DIR, "collections", collection, "queries", f"{random_timestamp}_queries.json"), "w") as f:
        json.dump(all_ids, f)
    with tqdm(total=len(all_ids)) as pbar:
        with ThreadPoolExecutor(max_workers=15) as executor:
            results = []
            futures = [executor.submit(check_url, f"https://www.tiktok.com/@/video/{generated_id}") for
                       generated_id in all_ids]
            for future in as_completed(futures):
                if future.result()["is_video"]:
                    tqdm.write("")
                    tqdm.write(future.result()["id"])
                    tqdm.write("{:b}".format(int(future.result()["id"])).zfill(64))
                results.append(future.result())
                pbar.update(1)
    with open(os.path.join(ROOT_DIR, "collections", collection, "queries", f"{random_timestamp}_hits.json"), "w") as f:
        json.dump(results, f)
    # pd.DataFrame(results).to_csv(f"datetest/samesecond_{test_id}.csv", index=False, header=True)
