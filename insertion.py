import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
import configparser
import locale

config = configparser.ConfigParser()
es = Elasticsearch(host = "localhost", port = 9200)

# Pour pouvoir formater les dates en français
locale.setlocale(locale.LC_ALL, 'fr_FR')

aujourdhui = datetime.now()
date_formattee = aujourdhui.strftime("%Y-%m-%d")

df = pd.read_parquet(f"./predictice_scrapy/{date_formattee}_legifrance_data.parquet")

# Reformatage de la date au format "YYYY-MM-dd"
def parse_to_strict_date_optional_time(date_originale):
    date_obj = datetime.strptime(date_originale, "%d %B %Y")
    return date_obj.strftime("%Y-%m-%d")

df["date"] = df["date"].apply(parse_to_strict_date_optional_time)

df2_dic = df.to_dict('records')

def generator(df2_dic):
    '''Create json format before inserting data to ES'''
    print("Launching generator")

    for c, line in enumerate(df2_dic):
        yield {
            '_index': 'test',
            'database': 'database',
            'filename':'filename',
            'loadedAt': date_formattee,
            '_id':line.get("numero_decision", None),
            'metadata':{
                'titre':line.get("titre", None),
                'juridiction':line.get("juridiction", None),
                'date':line.get("date", None),
                'numero_decision':line.get("numero_decision", None)
            },
            'texte':line.get("texte", None)
        }


# Load
try:
    res = helpers.bulk(es, generator(df2_dic))
    print("Data was inserted into ES")
    print("Response: ", res)
except Exception as e:
    print("Bulk Insertion did not work")
    print(e)
    pass
