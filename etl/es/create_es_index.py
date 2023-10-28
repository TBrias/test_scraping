from elasticsearch import Elasticsearch
import json
import os
import time
import logging
from datetime import datetime

logger = logging.getLogger("create_es_index")
logging.basicConfig(level=logging.INFO)

def create_index():
    logger.info(f"Lancement du script de création de l'index ES : {datetime.now()}")
    os.chdir(os.path.dirname(__file__))
    es = Elasticsearch(host = "es_container", port = 9200)

    while True:
        cluster_health = es.cluster.health()
        if cluster_health['status'] == 'green':
            logger.info("Le cluster Elasticsearch est en état 'GREEN'.")
            break
        else:
            logger.warn(f"En attente que le cluster atteigne l'état 'GREEN'. Actuellement en état: {cluster_health['status']}")
            time.sleep(5)


    with open("./es_mapping.json", 'r') as file:
        mapping = json.load(file)

    es.indices.create(
        index="legifrance",
        body=mapping,
        ignore=400
    )

    logger.info(f"Fin du script de création de l'index ES : {datetime.now()}")

if __name__ == '__main__':
    create_index()
    
