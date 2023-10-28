import json
import logging
import os
import time
from datetime import datetime

from elasticsearch import Elasticsearch

logger = logging.getLogger("create_es_index")
logging.basicConfig(level=logging.INFO)

def create_index():
    """
    Création de l'index ES dans lequel seront stockées les données ensuite
    Vérification de la conformité du cluster, puis création de l'index avec son mapping
    """
    logger.info(f"Lancement du script de création de l'index ES : {datetime.now()}")
    os.chdir(os.path.dirname(__file__))
    
    #L'host 'es_container' est créé dans le docker-compose.yml
    es = Elasticsearch(host = "es_container", port = 9200)

    while True:
        #On vérifie que le cluster est bien OK
        cluster_health = es.cluster.health()
        if cluster_health['status'] == 'green':
            logger.info("Le cluster Elasticsearch est en état 'GREEN'.")
            break
        else:
            logger.warn(f"En attente que le cluster atteigne l'état 'GREEN'. Actuellement en état: {cluster_health['status']}")
            time.sleep(5) #On fait un tour de boucle si jamais le cluster n'est pas encore complètement démarré

    #Appel du fichier json de mapping
    with open("./es_mapping.json", 'r') as file:
        mapping = json.load(file)

    #Création de l'index
    es.indices.create(
        index="legifrance",
        body=mapping,
        ignore=400 #Au cas ou si l'index existe déjà
    )

    logger.info(f"Fin du script de création de l'index ES : {datetime.now()}")

if __name__ == '__main__':
    create_index()
    
