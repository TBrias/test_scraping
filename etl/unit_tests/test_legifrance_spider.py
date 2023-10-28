import sys

import pytest

sys.path.append('../etl/')
from scrapy_script.predictice_scrapy.spiders.legifrance_spider import \
    LegifranceSpider


@pytest.fixture
def sample_spider():
    return LegifranceSpider()

def test_get_date_from_string(sample_spider):
    date_str = "Le 21 octobre 2023 après l'été"
    result = sample_spider.get_date_from_string(date_str)
    assert result == "21 octobre 2023"

def test_get_id_from_url(sample_spider):
    url = "https://www.legifrance.gouv.fr/juri/id/JURITEXT000047700631"
    result = sample_spider.get_id_from_url(url)
    assert result == "000047700631"

def test_replace_correspondance(sample_spider):
    sample_text = "Monsieur [C] [R]"
    result = sample_spider.replace_correspondance(sample_text)
    assert result == "Monsieur Charles Redon"