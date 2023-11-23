# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AsScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class PostDetailsItem(scrapy.Item):
    post_id = scrapy.Field()
    stage = scrapy.Field()
    created_date = scrapy.Field()
    external_links = scrapy.Field()

class PostItem(scrapy.Item):
    thread_id = scrapy.Field()
    platform = scrapy.Field()
    product_id = scrapy.Field()
    title = scrapy.Field()
    details = scrapy.Field() # This field will store a list of PostDetailsItem objects

