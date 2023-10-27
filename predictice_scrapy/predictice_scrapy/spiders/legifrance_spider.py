import scrapy
from scrapy import exceptions
import logging
import pandas as pd
from datetime import datetime
from .correspondance_prenoms import table_correspondance

logger = logging.getLogger("mylog")

class LegifranceSpider(scrapy.Spider):
    logger.warn("\n **** legifrance_spider script is starting **** \n")

    name = "legifrance_spider"
    allowed_domains = ["legifrance.gouv.fr"]
    start_date = "01/06/2022"
    end_date = "30/06/2022"

    def __init__(self, arg_start_date=None, arg_end_date=None, *args, **kwargs):
        super(LegifranceSpider, self).__init__(*args, **kwargs)
        # Si présence d'arguments pour spécifier la date de début/fin dans la commande de lancement du crawl, on override
        if (arg_start_date and arg_end_date) is not None:
            self.start_date = arg_start_date
            self.end_date = arg_end_date
        self.page = 1
        self.max_page = 1
        self.start_urls = [f"https://www.legifrance.gouv.fr/search/juri?tab_selection=juri&searchField=ALL&query=*&searchType=ALL&dateDecision={self.start_date}+%3E+{self.end_date}&typePagination=DEFAULT&sortValue=DATE_DESC&pageSize=10&page=1&tab_selection=juri#juri"]
    
    def parse(self, response):
        self.data = []
        # Récupération du nombre max de page
        self.max_page=response.css("li.pager-item a::attr(data-num)").getall()[-1]

        # Récupération de tous les liens présents
        links = response.css("article.result-item > h2 a::attr(href)").getall()

        # Fermeture du spider si plus de résultat sur la page courante
        if  "Aucun résultat pour la page" in response.css("div.container-pager ::text").get():
            raise exceptions.CloseSpider('No more result')
        
        # Appel de la fonction parse_detailpour récupérer toutes les informations voulues
        for link in links:
            yield response.follow(link, callback=self.parse_detail)

        # Pagination
        self.page += 1
        next_page = f"https://www.legifrance.gouv.fr/search/juri?tab_selection=juri&searchField=ALL&query=*&searchType=ALL&dateDecision={self.start_date}+%3E+{self.end_date}&typePagination=DEFAULT&sortValue=DATE_DESC&pageSize=10&page={self.page}&tab_selection=juri#juri"
        yield scrapy.Request(next_page, callback=self.parse)


    def parse_detail(self, response):        
        titre = response.css("h1.main-title::text").get()
        metadata = titre.split(', ')
        texte_document = "\n".join(response.xpath('//div[@class="content-page"]//div//text()').extract())                                    

        item = {
            "titre": titre,
            "juridiction": metadata[0],
            "date": metadata[1],
            "numero_decision": metadata[2],
            "texte": texte_document
        }
        self.data.append(item)
  

    def remplacer_correspondance(args, texte):
        if isinstance(texte, str): 
            for lettre, prenom in table_correspondance.items():
                texte = str(texte).replace(f"[{lettre}]", prenom)
        return texte


    def closed(self, reason):
        df = pd.DataFrame(self.data)

        # Desanonymisation: ex: [C] -> Charles
        df['texte'] = df['texte'].apply(self.remplacer_correspondance)

        today = datetime.now()
        date_formattee = today.strftime("%Y-%m-%d")

        df.to_parquet(f"{date_formattee}_legifrance_data.parquet", index=False)

