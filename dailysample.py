import json
import os
import stat
import threading
import pandas as pd
from tqdm import tqdm
from time import sleep
from selenium import webdriver
from datetime import date, timedelta
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from id import generate_ids_from_date, generate_all_same_second_ids, generate_same_second_ids_by_day
from selenium.webdriver.chrome.service import Service as ChromeService

from metadata import download_metadata

ROOT_DIR = os.path.realpath(os.path.dirname(__file__))
thread_local = threading.local()


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
    for year in range(2018, date.today().year+1):
        if not os.path.exists(os.path.join(ROOT_DIR, "collections", collection_name, str(year))):
            os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name, str(year)))
            os.chmod(os.path.join(ROOT_DIR, "collections", collection_name, str(year)),
                     stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name, str(year), "logs"))
            os.chmod(os.path.join(ROOT_DIR, "collections", collection_name, str(year), "logs"),
                     stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name, str(year), "metadata"))
            os.chmod(os.path.join(ROOT_DIR, "collections", collection_name, str(year), "metadata"),
                     stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name, str(year), "transcripts"))
            os.chmod(os.path.join(ROOT_DIR, "collections", collection_name, str(year), "transcripts"),
                     stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            os.makedirs(os.path.join(ROOT_DIR, "collections", collection_name, str(year), "wavs"))
            os.chmod(os.path.join(ROOT_DIR, "collections", collection_name, str(year), "wavs"),
                     stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)


def date_range(start_date, end_date):
    for n in range(int((end_date-start_date).days)):
        yield start_date + timedelta(n)


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
    video_id = url.split("/")[-1]
    tries = 0
    reset_driver = False
    while tries < 3:
        try:
            driver = get_driver(reset_driver)
            reset_driver = False
            driver.get(url)
            if driver.title == "Access Denied" or "s videos with | TikTok" in driver.title:
                reset_driver = True
                driver.close()
                sleep(4)
            else:
                is_video = url != driver.current_url
                if is_video:
                    metadata_dict = download_metadata(driver.current_url)
                    with open(f"metadata/{video_id}.json", "w") as f:
                        json.dump(metadata_dict, f)
                return {"id": str(video_id), "url": driver.current_url, "title": driver.title, "is_video": is_video}
        except Exception as e:
            tqdm.write("")
            tqdm.write(f"{video_id} {str(e)}")
            reset_driver = True
            sleep(5)
        tries += 1
    tqdm.write("")
    tqdm.write(f"{video_id} returning none")
    return {"id": str(video_id), "url": url, "title": "returned none, no title", "is_video": False}


# start_date = date(2020, 1, 1)
# n_ids = 50000
# end_date = date.today()
#
# for sample_date in date_range(start_date, end_date):
#     print(sample_date)
#     with tqdm(total=n_ids) as pbar:
#         with ThreadPoolExecutor(max_workers=20) as executor:
#             results = []
#             futures = [executor.submit(check_url, f"https://www.tiktok.com/@/video/{generated_id}") for
#                        generated_id in generate_ids_from_date(n_ids, sample_date.year, sample_date.month, sample_date.day)]
#             for future in as_completed(futures):
#                 results.append(future.result())
#                 pbar.update(1)
#     pd.DataFrame(results).to_csv(f"datetest/{sample_date}.csv", index=False, header=True)

# user8574804403074
# bitswapped_ids = [7346646469476240645, 7346646471623724293, 7346646470549982469, 7346646468939369733,
#                   7346646469744676101, 7346646469342022917, 7346646469409131781, 7346646469442686213,
#                   7346646469459463429, 7346646469467852037, 7346646469480434949, 7346646469478337797,
#                   7346646469477289221, 7346646469476764933, 7346646469476502789, 7346646469476109573,
#                   7346646469476306181, 7346646469476273413, 7346646469476224261, 7346646469476248837,
#                   7346646469476244741, 7346646469476238597, 7346646469476239621, 7346646469476241157,
#                   7346646469476240389, 7346646469476240773, 7346646469476240709, 7346646469476240677,
#                   7346646469476240661, 7346646469476240653, 7346646469476240641, 7346646469476240647,
#                   7346646469476240644]

# undefined.toss
# bitswapped_ids = [7346646471623724293, 7346646469476240645, 7346646472697466117, 7346646471086853381, 7346646471892159749, 7346646471489506565, 7346646471556615429, 7346646471590169861, 7346646471606947077, 7346646471615335685, 7346646471627918597, 7346646471625821445, 7346646471624772869, 7346646471624248581, 7346646471623986437, 7346646471623593221, 7346646471623789829, 7346646471623757061, 7346646471623707909, 7346646471623732485, 7346646471623728389, 7346646471623722245, 7346646471623723269, 7346646471623724805, 7346646471623724037, 7346646471623724421, 7346646471623724357, 7346646471623724325, 7346646471623724309, 7346646471623724301, 7346646471623724289, 7346646471623724295, 7346646471623724292]


# khaled_55_dk
# bitswapped_ids = [7346646469476224261, 7346646471623707909, 7346646470549966085, 7346646468939353349, 7346646469744659717, 7346646469342006533, 7346646469409115397, 7346646469442669829, 7346646469459447045, 7346646469467835653, 7346646469480418565, 7346646469478321413, 7346646469477272837, 7346646469476748549, 7346646469476486405, 7346646469476093189, 7346646469476289797, 7346646469476257029, 7346646469476240645, 7346646469476232453, 7346646469476228357, 7346646469476222213, 7346646469476223237, 7346646469476224773, 7346646469476224005, 7346646469476224389, 7346646469476224325, 7346646469476224293, 7346646469476224277, 7346646469476224269, 7346646469476224257, 7346646469476224263, 7346646469476224260]


# all same second ids
# test_id = 7346646469476240645
dates = [[2020, 9, 15], [2020, 11, 15],  # [2020, 1, 15], [2020, 3, 15], [2020, 5, 15], [2020, 7, 15],
         [2021, 1, 15], [2021, 3, 15], [2021, 5, 15], [2021, 7, 15], [2021, 9, 15], [2021, 11, 15],
         [2022, 1, 15], [2022, 3, 15], [2022, 5, 15], [2022, 7, 15], [2022, 9, 15], [2022, 11, 15],
         [2023, 1, 15], [2023, 3, 15], [2023, 5, 15], [2023, 7, 15], [2023, 9, 15], [2023, 11, 15],
         [2024, 1, 15], [2024, 3, 15], [2024, 5, 15]]

for date in dates:
    all_ids = generate_same_second_ids_by_day(date[0], date[1], date[2])

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
    with open(f"queries/{date[0]}{date[1]}{date[2]}.json", "w") as f:
        json.dump(results, f)
    # pd.DataFrame(results).to_csv(f"datetest/samesecond_{test_id}.csv", index=False, header=True)
