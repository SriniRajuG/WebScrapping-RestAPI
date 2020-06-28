from scrapy import  Request
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class Spider1(CrawlSpider):

    name = "spider_1"
    start_urls = ("http://books.toscrape.com",)
    rules = (
        Rule(
            link_extractor=LinkExtractor(
                allow=r'page-[0-4]{1,}\.html$',
                restrict_xpaths="//li[@class='next']"
            ),
            callback='parse_page',
            follow=False,
        ),
    )

    def parse_page(self, response):
        print()
        print(response.url)
        print()
        pass
