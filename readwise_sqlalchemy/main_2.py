@dataclass
class UserConfig:
    last_fetch_file : Path = Path("readwise_obsidian") / "data" / "last_fetch.txt" 

@dataclass
class ReadwiseIndividualHighlight:
        id: int
        text: str
        location: int
        location_type: str
        note: str
        color: str
        highlighted_at: str # ISO format with Z
        created_at: str # ISO format with Z
        updated_at: str # ISO format with Z
        external_id: str | None
        end_location: str | None
        url: str
        book_id: int
        tags: list
        is_favourite: bool 
        is_discard: bool
        readwise_url: str


@dataclass
class ReadwiseHighlightsSummary:
    user_book_id : int
    title : str
    author: str                  
    source: str 
    cover_image_url: str
    unique_url: str 
    book_tags: list
    category: str 
    document_note: str | None 
    summary: str | None  
    readwise_url: str
    source_url: str
    asin: str 
    highlights: list[dict]

    readable_title: str


class ReadwiseFetcher:
    """Class to manage fetching highlights from the Readwise API.
    
    Methods
    -------
    fetch_all_time_highlights
    
    fetch_new_highlights
    
    """
    def __init__(self, user_config: UserConfig) -> None:
        """Initializer."""
        self.user_config = UserConfig()
    
    def fetch_all_time_highlights(self) -> list[dict]:
        """Fetch all highlights.
        
        Returns
        -------
        all_highlights
        
        """
        all_highlights = self._fetch_from_export_api(None)
        return all_highlights
        
    def fetch_new_highlights(self) -> tuple[str, list[dict]]:
        """Fetch most recent highlights only.
        
        Checks saved file for a last fetch datetime (in ISO format).

        """
        last_fetch = self.user_config.last_fetch_file.read_text()
        now = datetime.datetime.now(datetime.UTC).isoformat()
        if last_fetch:
            try: 
                datetime.datetime.fromisoformat(last_fetch)
            except TypeError as error:
                logger.info(f"Last fetched datetime {last_fetch} is not a valid ISO string.")
                raise error
            fetched_data = self._fetch_from_export_api(last_fetch)
        else:
            fetched_data = self.fetch_all_time_highlights()
        self.user_config.last_fetch_file.write_text(now)
        return (now, fetched_data)
    
    @cache
    def _fetch_from_export_api(self, updated_after: str|None=None) -> list[dict]:
        """Fetch function from the Readwise docs."""
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
    

class FileHandler:
    @staticmethod
    def write_json(data, file_path):
        with open(file_path, "w") as file_handle:
            json.dump(data, file_handle)

    @staticmethod
    def read_json(file_path):
        with open(file_path, "r") as file_handle:
            content = json.load(file_handle)
        return content


# all_data = read_json("readwise.json")

# sources = set()
# category = set()
# for item in all_data:
#     sources.add(item["source"])
#     category.add(item["category"])

# # sources = {'airr', 'api_article', 'hypothesis', 'ibooks', 'kindle', 'pdf', 'pocket', 'podcast', 'reader', 'snipd', 'twitter', 'web_clipper'}
# # catergories = {'articles', 'books', 'podcasts', 'tweets'}

# tweets = []
# for item in all_data:
#     if item["category"] == "tweets":
#         tweets.append(item)

# # write_json(tweets, "readwise_tweets.json")

# tweet_titles = []
# for tweet in tweets:
#     tweet_titles.append(tweet['title'])

# tweet_titles.sort()
# write_json(tweet_titles, "readwise_tweets_titles.json")

# individual_tweets = []
# for tweet in tweets:
#     for highlight in tweet["highlights"]:
