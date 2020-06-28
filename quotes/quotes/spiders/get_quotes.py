# -*- coding: utf-8 -*-
# import scrapy
from scrapy import Spider
from scrapy.http import Request
from scrapy.http import FormRequest



class GetQuotesSpider(Spider):
    name = 'get_quotes'
    allowed_domains = ['quotes.toscrape.com']
    start_urls = (
        'http://quotes.toscrape.com/login',
    )
    # start_urls = ['http://quotes.toscrape.com/']




    def parse(self, response):
        csrf_token = response.xpath("//input[@name = 'csrf_token']/@value").get()
        yield FormRequest(
            url="http://quotes.toscrape.com/login", 
            formdata={
                'csrf_token': csrf_token,
                'username': 'dummy_username',
                'password': 'dummy_password',
            },
            callback=self.parse_after_login
        )


    def parse_after_login(self, response):
        sel_list = response.xpath("//div[@class = 'quote']")
        for sel in sel_list:
            quote = sel.xpath("child::span[@class = 'text']/text()").get() 
            author = sel.xpath("descendant::small[@class = 'author']/text()").get() 
            tags = sel.xpath("descendant::a[@class = 'tag']/text()").getall() 
            yield {
                'quote': quote,
                'author': author,
                'tags': tags,
            }
                                                                                     
        next_page_url = response.xpath("//li[@class = 'next']/a/@href").get()
        absolute_next_page_url = response.urljoin(next_page_url)
        yield Request(absolute_next_page_url, callback=self.parse_after_login)
   
   
   
