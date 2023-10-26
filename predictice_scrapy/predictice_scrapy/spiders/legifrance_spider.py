import scrapy
import logging
from scrapy.utils.log import configure_logging 
import pandas as pd

logger = logging.getLogger("mylog")



class LegifranceSpider(scrapy.Spider):
    logger.warn("\n **** legifrance_spider script is starting **** \n")

    name = "legifrance_spider"
    allowed_domains = ["legifrance.gouv.fr"]
    start_urls = ["https://www.legifrance.gouv.fr/search/juri?tab_selection=juri&searchField=ALL&query=*&searchType=ALL&dateDecision=01%2F06%2F2022+%3E+30%2F06%2F2022&typePagination=DEFAULT&sortValue=DATE_DESC&pageSize=10&page=1&tab_selection=juri#juri"]

    def parse(self, response):
        self.data = []
        logger.warning("\n Parse is called \n")
        logger.warning("Parse is called on URL: " + response.url)
        links = response.css("article.result-item > h2 a::attr(href)").getall()
        logger.warning("\n**** links ****:  %s", links)

        for link in links:
            logger.warning("\n**** link ****:  %s", link)
            yield response.follow(link, callback=self.parse_detail)

    def parse_detail(self, response):
        logger.warning("\n parse_detail is called \n")
        
        titre = response.css("h1.main-title::text").get()
        logger.warning("**** titre ****: " + titre)

        metadata = titre.split(',')
        #texte_document = response.css("div.content-page > div::text").getall()
        texte_document = "\n".join(response.xpath('//div[@class="content-page"]//div//text()').extract())
        

        item = {
            "titre": titre,
            "juridiction": metadata[0],
            "date": metadata[1],
            "numero_decision": metadata[2],
            "texte": texte_document
        }
        self.data.append(item)

    def closed(self, reason):
        logger.warning("\n write_parquet is called \n")
        logger.warning(reason)
        df = pd.DataFrame(self.data)
        logger.warning(df.size)
        df.to_parquet("legifrance_data.parquet", index=False)