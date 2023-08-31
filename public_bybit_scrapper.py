"""
This script will scrape data from https://public.bybit.com and download it in
the same directory as this script.
"""

import datetime
import logging
import os
import time

import requests
from bs4 import BeautifulSoup

PUBLIC_BYBIT_URL = "https://public.bybit.com/"
DEFAULT_REQUEST_TIMEOUT = 6
MAX_RETRIES = 3
WAIT_TIME_BETWEEN_REQUESTS = 0
WAIT_TIME_FOR_RETRIES = 3
RESULTS_FOLDER = "results"

# setup logger
runned_at = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
filename_without_ext = os.path.splitext(__file__)[0]
log_file = f"{filename_without_ext}_{runned_at}.log"

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%H:%M:%S"

file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


class MaxRetriesReached(Exception):
    """
    Custom exception to raise when max retries is reached.
    """

    def __init__(self, message="An error occured"):
        self.message = message
        super().__init__(self.message)


# raise MaxRetriesReached("Max retries reached")


def attempt_request(
    url,
    timeout=DEFAULT_REQUEST_TIMEOUT,
    max_retries=MAX_RETRIES,
    wait_time_for_retry=WAIT_TIME_FOR_RETRIES,
):
    """
    Request a content and return the response handling errors and retries.
    """
    for attempt in range(max_retries):
        if attempt >= 1:
            time.sleep(wait_time_for_retry)

        retry_feedback = (
            f"Attempt {attempt + 1}/{max_retries} of " if attempt >= 1 else ""
        )

        try:
            logger.info("%sRequesting %s", retry_feedback, url)
            response = requests.get(url, timeout=timeout)

            if response.status_code == 200:
                return response.content

            logger.error("Status code: %s", response.status_code)

        except (
            requests.exceptions.Timeout,
            requests.exceptions.TooManyRedirects,
            requests.exceptions.ConnectionError,
        ) as exception:
            logger.error("%s when requesting %s", type(exception).__name__, url)

            if attempt < max_retries - 1:
                continue

            raise MaxRetriesReached(f"Max retries reached for {url}") from exception

    return None


def crawl_and_download_from(base_url, download_dir):
    """
    crawl and download
    """
    response_content = attempt_request(base_url)
    soup = BeautifulSoup(response_content, "html.parser")

    for link in soup.find_all("a"):
        href = link.get("href")
        next_url = base_url + href

        if href.endswith("/"):  # It's a folder
            extracted_path_from_url = href.replace(base_url, "")
            folder_path = os.path.join(download_dir, extracted_path_from_url)

            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            time.sleep(WAIT_TIME_BETWEEN_REQUESTS)
            crawl_and_download_from(next_url, folder_path)

        else:  # It's a file
            extracted_path_from_url = href.replace(base_url, "")
            file_path = os.path.join(download_dir, extracted_path_from_url)

            if not os.path.exists(file_path):
                logger.info("Downloading %s", file_path)
                response_content = attempt_request(next_url)

                with open(file_path, "wb") as file:
                    file.write(response_content)
                    time.sleep(WAIT_TIME_BETWEEN_REQUESTS)


def main():
    """
    Main function that will run the script.
    """
    if not os.path.exists(RESULTS_FOLDER):
        logger.info("Creating local folder to store scrapped data...")
        os.makedirs(RESULTS_FOLDER)

    crawl_and_download_from(PUBLIC_BYBIT_URL, RESULTS_FOLDER)


if __name__ == "__main__":
    logger.info("Starting script")
    main()
