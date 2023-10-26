import scrapy
import logging
from scrapy.utils.log import configure_logging 

logger = logging.getLogger("mylog")

class LegifranceSpider(scrapy.Spider):
    configure_logging(install_root_handler=False)
    logging.basicConfig(
        filename='log.txt',
        format='%(levelname)s: %(message)s',
        level=logging.INFO
    )

    logger.warn("legifrance_spider script is starting")

    name = "legifrance_spider"
    allowed_domains = ["legifrance.gouv.fr"]
    start_urls = ["https://www.legifrance.gouv.fr/search/juri?tab_selection=juri&searchField=ALL&query=*&searchType=ALL&dateDecision=01%2F06%2F2022+%3E+30%2F06%2F2022&typePagination=DEFAULT&sortValue=DATE_DESC&pageSize=10&page=1&tab_selection=juri#juri"]

    def parse(self, response):
        logger.warning("Parse is called")
        links = response.css("article.result-item::attr(href)").getall()
        logger.warning("AAAAAAAAAAAAAAA   AAAAAAAAAAAAAAAAAAAAAa")
        logger.warning("**** links ****: ", links)

        for link in links:
            logger.warning("**** link ****: ", link)
            yield response.follow(link, callback=self.parse_detail)

    def parse_detail(self, response):
        # Ici, vous pouvez extraire les détails de la page individuelle
        titre = response.css("h1.titre-texte::text").get()
        juridiction = response.css(".juridiction::text").get()
        date = response.css(".date-decision::text").get()
        numero_decision = response.css(".numero-decision::text").get()
        texte_document = response.css(".texte-document::text").get()

        yield {
            "titre": titre,
            "juridiction": juridiction,
            "date": date,
            "numero_decision": numero_decision,
            "texte": texte_document
        }