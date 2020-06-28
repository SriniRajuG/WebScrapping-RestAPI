# -*- coding: utf-8 -*-

from scrapy import Item
from scrapy import Field


class JobItem(Item):
    title = Field()
    post_id = Field()
    datetime = Field()
    location = Field()
    latitude = Field()
    longitude = Field()
    compensation = Field()
    employment_type = Field()
    description = Field()

