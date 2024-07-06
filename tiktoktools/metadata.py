import json
from bs4 import BeautifulSoup


def extract_metadata(page_source) -> tuple[dict, str, str]:
    soup = BeautifulSoup(page_source, "html.parser")
    rehydration_script_elements = soup.select('script#__UNIVERSAL_DATA_FOR_REHYDRATION__')
    if len(rehydration_script_elements) == 1:
        rehydration_dict = json.loads(rehydration_script_elements[0].text)
        # extract status code / status message
        if "__DEFAULT_SCOPE__" in rehydration_dict and "webapp.video-detail" in rehydration_dict["__DEFAULT_SCOPE__"]:
            return (rehydration_dict["__DEFAULT_SCOPE__"]["webapp.video-detail"],
                    str(rehydration_dict["__DEFAULT_SCOPE__"]["webapp.video-detail"]["statusCode"]),
                    rehydration_dict["__DEFAULT_SCOPE__"]["webapp.video-detail"]["statusMsg"])
        else:
            return {}, "ERROR", "__DEFAULT_SCOPE__ or webapp.video-detail not in data for rehydration"
    else:
        return {}, "ERROR", "script#__UNIVERSAL_DATA_FOR_REHYDRATION__"
