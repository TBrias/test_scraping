import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy_script.predictice_scrapy.spiders import legifrance_spider
import insertion
import es.create_es_index as create_es_index
import logging
from datetime import datetime

logger = logging.getLogger("main")
logging.basicConfig(level=logging.INFO)

def run_create_index():
    create_es_index.create_index()

def run_scrapy():
    os.chdir(f"{os.path.dirname(__file__)}/scrapy_script")
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(legifrance_spider.LegifranceSpider)
    process.start()

def run_insertion():
    os.chdir(os.path.dirname(__file__))
    insertion.main()

if __name__ == '__main__':
    logger.info(f"Lancement des scripts : {datetime.now()}")

    run_create_index()
    run_scrapy()
    run_insertion()

    logger.info(f"Fin des scripts : {datetime.now()}")

