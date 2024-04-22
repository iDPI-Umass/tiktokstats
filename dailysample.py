import os
import stat
from datetime import date, timedelta
ROOT_DIR = os.path.realpath(os.path.dirname(__file__))


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


start_date = date(2018, 1, 1)
end_date = date.today()

for sample_date in date_range(start_date, end_date):

    print(sample_date)

print(ROOT_DIR)
initialize_collection("test_collection")
