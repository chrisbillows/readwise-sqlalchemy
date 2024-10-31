import os

import readwise_sqlalchemy.config 
from readwise_sqlalchemy.logger import logger


def main():
    logger.info("Test logging message")
    print("Hello world!")
    print(READWISE_API_TOKEN)


if __name__ == "__main__":
    main()
