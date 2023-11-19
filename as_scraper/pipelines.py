# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sys
import os
import re
import pymongo
from bson.json_util import dumps

WHITELIST_LINKS = [
    'rapidgator.net',
    'katfile.com',
    'rosefile.net',
    'mexa.sh',
    'fikper.com',
    'ddownload.com',
    'k2s.cc',
    'fboom.me',
    'mega.nz',
    'ul.to',
    'uploadrocket.net'
]

class AsScraperFormatPipeline:
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)
        
        ## strip all whitespaces from strings
        value = adapter.get('title')        
        adapter['title'] = value.strip()
            
        ## remove external links that aren't in the whitelist
        whitelist = re.compile('|'.join([re.escape(word) for word in WHITELIST_LINKS]))        
        meta_values = adapter.get('meta')
        for meta_value in meta_values:
            meta_value['external_links'] = [word for word in meta_value['external_links'] if whitelist.search(word)]
                  
        return item
    
class CleanEmptyPostsPipeline:
   
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)
        
        meta_values = adapter.get('meta')
        for meta_value in meta_values:
            if not meta_value['external_links']:
                meta_values.remove(meta_value)

        return item

class MongoDBPipeline:

    collection_name = "as_items"

    def __init__(self, mongodb_uri, mongodb_db):
        self.mongodb_uri = mongodb_uri
        self.mongodb_db = mongodb_db
        if not self.mongodb_uri: sys.exit("You need to provide a Connection String.")

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongodb_uri=crawler.settings.get('MONGODB_URI'),
            mongodb_db=crawler.settings.get('MONGODB_DATABASE', 'as_items')
        )

    def open_spider(self, spider):
        self.mongo_username = os.environ['MONGODB_USERNAME']
        self.mongo_password = os.environ['MONGODB_PASSWORD']
        self.client = pymongo.MongoClient(
            self.mongodb_uri, username=self.mongo_username, password=self.mongo_password)
        self.db = self.client[self.mongodb_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        exists = self.db[self.collection_name].find_one({'thread_id': dict(item)['thread_id']}, {'_id': False, 'thread_id': True})
        if not exists:
            ## insert if not exists
            self.db[self.collection_name].insert_one(ItemAdapter(item).asdict())
        else:
            ## $addToSet operator adds a value to an array unless the value is already present 
            ## in which case $addToSet does nothing to that array.
            self.db[self.collection_name].update_one(exists, {'$addToSet': {'meta': {'$each': item['meta'] }}})
        return item    