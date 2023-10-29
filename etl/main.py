import logging
import os
from datetime import datetime

import es.create_es_index as create_es_index
import insertion
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy_script.predictice_scrapy.spiders import legifrance_spider

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

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
    """
    main script du projet joué dans le Dockerfile
    Tâches:
        Création d'un index elastic avec le bon mapping
        Scrapping des données avec scrapy
        Insertion des données dans un ES
    """
    logger.info(f"Lancement des scripts : {datetime.now()}")

    run_create_index()
    run_scrapy()
    run_insertion()

    logger.info(f"Fin des scripts : {datetime.now()}")

