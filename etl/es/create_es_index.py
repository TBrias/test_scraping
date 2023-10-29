import json
import logging
import os
import time
from datetime import datetime

from elasticsearch import Elasticsearch, exceptions

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def create_index():
    """
    Création de l'index ES dans lequel seront stockées les données ensuite
    Vérification de la conformité du cluster, Docker peut mettre un certain temps avant de démarrer le service
    puis création de l'index avec son mapping
    """
    logger.info(f"Lancement du script de création de l'index ES : {datetime.now()}")
    os.chdir(os.path.dirname(__file__))
    
    # L'host 'es_container' est créé dans le docker-compose.yml
    es = Elasticsearch(host = "es_container", port = 9200)

    while True:
        # On vérifie que le service Elasticsearch soit bien démarré
        try:
            es.cluster.health()
            logger.warn(f"Le service Elasticsearch est bien démarré")
            break

        except (
            exceptions.ConnectionError,
            exceptions.TransportError
        ):
            # On fait un tour de boucle si jamais le cluster n'est pas encore complètement démarré
            logger.warn(f"En attente que le service Elasticsearch soit bien démarré")
            time.sleep(5)

    # Appel du fichier json de mapping
    with open("./es_mapping.json", 'r') as file:
        mapping = json.load(file)

    # Création de l'index
    es.indices.create(
        index="legifrance",
        body=mapping,
        ignore=400 # Au cas ou si l'index existe déjà
    )

    logger.info(f"Fin du script de création de l'index ES : {datetime.now()}")

if __name__ == '__main__':
    create_index()
    
