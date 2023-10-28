import locale
import logging
import os
from datetime import datetime

import pandas as pd
from elasticsearch import Elasticsearch, helpers

logger = logging.getLogger("insertion")
logging.basicConfig(level=logging.INFO)
# Pour pouvoir formater les dates en français
locale.setlocale(locale.LC_ALL, 'fr_FR')

es = Elasticsearch(host = "es_container", port = 9200)

index_name = "legifrance"
date_formattee = datetime.now().strftime("%Y-%m-%d")
parquet_filename = f"{date_formattee}_legifrance_data.parquet"

def main():
    logger.info(f"Début du script d'insertion dans es : {datetime.now()}")
    # Extract
    df = read_parquet()

    # Transform
    df_dict = transform_to_dict(df)

    # Load to ES
    bulk_insert(es, df_dict)

    logger.info(f"Fin du script d'insertion dans es : {datetime.now()}")


def read_parquet():
    df = pd.DataFrame() 
    try:
        df = pd.read_parquet(os.path.join(".","scrapy_script", "output", parquet_filename))
    except FileNotFoundError:
        logging.error(f"Le fichier {parquet_filename} n'existe pas")

    return df


def parse_to_strict_date_optional_time(date_originale):
    # Reformatage de la date au format "YYYY-MM-dd"
    date_obj = datetime.strptime(date_originale, "%d %B %Y")
    return date_obj.strftime("%Y-%m-%d")


def transform_to_dict(df):
    """
    Méthode qui transforme le champ 'date' au format attendu 
    et renvoit une liste de dictionnaires
    """
    try:
        df["date"] = df["date"].apply(parse_to_strict_date_optional_time)
    except KeyError:
        logging.error(f"La colonne ou le dataframe est vide")

    #Convertit le dataframe pandas en une liste de dictionnaires
    return df.to_dict('records')


def generator(df_dict):
    """
    On itère à travers chaque élément de la liste, pour chaque élément
    on récupère les informations qui nous intéressent en respectant le mapping ES
    """
    for c, line in enumerate(df_dict):
        yield {
            '_index': index_name,
            'database': 'database',
            'filename': parquet_filename,
            'loadedAt': date_formattee,
            '_id':line.get("id", None),
            'metadata':{
                'titre':line.get("titre", None),
                'juridiction':line.get("juridiction", None),
                'date':line.get("date", None),
                'numero_decision':line.get("numero_decision", None)
            },
            'texte':line.get("texte", None)
        }


def bulk_insert(es, df_dict):
    #Utilisation du bulk ES pour insérer les documents
    try:
        res = helpers.bulk(es, generator(df_dict))
        logger.info(f"Documents ayant été insérés dans ElasticSearch: {res}" )
    except Exception as e:
        logger.error("Problème lors de l'insertion bulk")
        logger.error(e)
        pass

if __name__ == '__main__':
    main()