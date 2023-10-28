import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy_script.predictice_scrapy.spiders import legifrance_spider
import insertion
import logging
from datetime import datetime

logger = logging.getLogger("main")
logging.basicConfig(level=logging.INFO)

def run_scrapy():
    os.chdir("scrapy_script")
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(legifrance_spider.LegifranceSpider)
    process.start()

def run_insertion(owd):
    os.chdir(owd)
    insertion.main()

if __name__ == '__main__':
    logger.info(f"Lancement des scripts : {datetime.now()}")
    owd = os.getcwd()
    run_scrapy()
    run_insertion(owd)

    logger.info(f"Fin des scripts : {datetime.now()}")

