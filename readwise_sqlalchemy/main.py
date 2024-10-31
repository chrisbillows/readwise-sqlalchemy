import os

import requests

import readwise_sqlalchemy.config 
from readwise_sqlalchemy.logger import logger

# The config import ensures the .env file is loaded on module import
READWISE_API_TOKEN = os.getenv("READWISE_API_TOKEN")


def _fetch_from_export_api(self, updated_after: str|None=None) -> list[dict]:
    """Fetch function copied from the Readwise docs.
    
    Parameter
    ---------
    updated_after: str
        Fetch only highlights updated after this date, formatted as ISO 8601 string.
    
    Returns
    -------
    list[dict]
        A list where every Readwise highlight is a dict.
    """
    full_data = []
    next_page_cursor = None
    while True:
        params = {}
        if next_page_cursor:
            params['pageCursor'] = next_page_cursor
        if updated_after:
            params['updatedAfter'] = updated_after
        logger.debug("Making export api request with params " + str(params) + "...")
        response = requests.get(
            url="https://readwise.io/api/v2/export/",
            params=params,
            headers={"Authorization": f"Token {READWISE_API_TOKEN}"}, verify=True
        )
        full_data.extend(response.json()['results'])
        next_page_cursor = response.json().get('nextPageCursor')
        if not next_page_cursor:
            break
        return full_data


def main():
    test_data = _fetch_from_export_api("2024-10-20")
    print(len(test_data))
    print(test_data[0])


if __name__ == "__main__":
    main()
