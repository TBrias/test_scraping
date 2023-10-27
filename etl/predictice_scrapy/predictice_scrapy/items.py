# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import re
from datetime import datetime
import logging

logger = logging.getLogger("mylog")
logging.basicConfig(level=logging.INFO)
start_time = datetime.now()

logger.info(f"Début du script d'insertion : {start_time}")
print('Starting: {}'.format(start_time))

diff_time = datetime.now() - start_time
print(f"Fin du script: {datetime.now()}")
print(f"Durée du script: {diff_time}")


diff_time = datetime.now() - start_time
logger.info(f"Fin du script d'insertion : {start_time} en {diff_time}")

end_time = datetime.now()
print('Done: {}'.format(end_time))
logger.info('Duration: {}'.format(end_time - start_time))

logger.info('Duration: {}'.format(datetime.now() - start_time))
