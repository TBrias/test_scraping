import logging
import os
import random
import re
import string
from datetime import datetime

import pandas as pd
import scrapy
from scrapy import exceptions

from .correspondance_prenoms import table_correspondance

logger = logging.getLogger("scrapping_spider")

class LegifranceSpider(scrapy.Spider):
    logger.info(f"Début du script de scrapping : {datetime.now()}")

    name = "legifrance_spider"
    allowed_domains = ["legifrance.gouv.fr"]
    start_date = "01/06/2023"
    end_date = "30/06/2023"

    def __init__(self, arg_start_date=None, arg_end_date=None, *args, **kwargs):
        super(LegifranceSpider, self).__init__(*args, **kwargs)
        # Si présence d'arguments pour spécifier la date de début/fin
        #  dans la commande de lancement du crawl, on override. Utiliser pour le dev
        if (arg_start_date and arg_end_date) is not None:
            self.start_date = arg_start_date
            self.end_date = arg_end_date
        self.data = []
        self.page = 1
        self.max_page = 1
        self.start_urls = [f"https://www.legifrance.gouv.fr/search/juri?tab_selection=juri&searchField=ALL&query=*&searchType=ALL&dateDecision={self.start_date}+%3E+{self.end_date}&typePagination=DEFAULT&sortValue=DATE_DESC&pageSize=10&page=1&tab_selection=juri#juri"]
    
    def parse(self, response):
        # Récupération du nombre max de page, fermeture du spider si pas de résultats
        try:
            self.max_page=response.css("li.pager-item a::attr(data-num)").getall()[-1]
        except IndexError:
            raise exceptions.CloseSpider('Pas de résultat sur la page demandée')

        # Récupération de tous les liens présents
        links = response.css("article.result-item > h2 a::attr(href)").getall()

        # Fermeture du spider si plus de résultat sur la page courante
        if  "Aucun résultat pour la page" in response.css("div.container-pager ::text").get():
            raise exceptions.CloseSpider('La pagination ne retourne plus de résultats')
        
        # Appel de la fonction parse_detail pour récupérer toutes les informations voulues
        for link in links:
            yield response.follow(link, callback=self.parse_detail)

        # Pagination et appel de la prochaine page
        self.page += 1
        next_page = f"https://www.legifrance.gouv.fr/search/juri?tab_selection=juri&searchField=ALL&query=*&searchType=ALL&dateDecision={self.start_date}+%3E+{self.end_date}&typePagination=DEFAULT&sortValue=DATE_DESC&pageSize=10&page={self.page}&tab_selection=juri#juri"
        yield scrapy.Request(next_page, callback=self.parse)


    def get_date_from_string(self, str):
        """
        Récupère la date avec une regex au format par exemple: 21 octobre 2023
        """
        date_match = re.search(r'\d{2} \w+ \d{4}', str)
        if date_match:
            return date_match.group()
        
    def get_id_from_url(self, str_url):
        """
        Récupère l'id de l'URL qui servira d'id pour elasticsearch
        On utilise utilise un string randomiser si jamais la regex ne fonctionne pas
        """
        match = re.search(r"/JURITEXT(\d+)", str_url)
        if match:
            return match.group(1)
        else:
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))


    def parse_detail(self, response):  
        id = self.get_id_from_url(response.request.url)
        titre = response.css("h1::text").get()
        metadata = titre.split(', ')
        date = self.get_date_from_string(response.css("div.horsAbstract::text").get())
        numero_brut = response.xpath('//div[@class="frame-block print-sommaire"]//div//ul//li//text()').get()
        num_decision = numero_brut.split(":")[1].strip()
        # Le "\n" sert à ajouter des sauts de ligne en fin de phrase
        texte_document = "\n".join(response.xpath('//div[@class="content-page"]//div//text()').extract())                              

        item = {
            "titre": titre,
            "juridiction": metadata[0],
            "date": date,
            "numero_decision": num_decision,
            "texte": texte_document,
            "id":id
        }
        self.data.append(item)
  

    def replace_correspondance(self, texte):
        """
        Desanonymisation du texte: ex: [C] -> Charles
        """
        if isinstance(texte, str): 
            for lettre, nom in table_correspondance.items():
                texte = str(texte).replace(f"[{lettre}]", nom)
        return texte

    def closed(self, reason):
        df = pd.DataFrame(self.data)

        df['texte'] = df['texte'].apply(self.replace_correspondance)

        today = datetime.now()
        date_formattee = today.strftime("%Y-%m-%d")

        #Ecriture du parquet
        df.to_parquet(os.path.join("output",f"{date_formattee}_legifrance_data.parquet"), index=False)

        logger.info(f"Fin du script de scrapping : {datetime.now()}")


