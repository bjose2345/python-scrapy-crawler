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
from pathlib import Path

FILEHOSTS = [
    'rapidgator.net',
    'mexa.sh',
    'katfile.com',
    'rosefile.net',    
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
        filehosts_whitelisted = re.compile('|'.join([re.escape(word) for word in FILEHOSTS]))        
        meta_values = adapter.get('meta')
        for meta_value in meta_values:
            meta_value['external_links'] = [word for word in meta_value['external_links'] if filehosts_whitelisted.search(word)]
                  
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

        ## create a crawljob file for each one of the post found, stored and stage is null in mongodb
        exports = self.db[self.collection_name].find({'meta.stage': None})
        p = Path(__file__).with_name('crawljob.template')

        if exports:
            with p.open('r') as f:
                template_json = f.read()

            for export in exports:            
                external_links = []
                for meta_value in export['meta']:
                    ## update stage to FETCHED status
                    self.db[self.collection_name].update_one({'_id': export['_id'], 'meta.post_id': meta_value['post_id']}, {'$set': {'meta.$[].stage': 'FETCHED'}})
                    external_links.append(meta_value['external_links'])
                    
                values = {
                    'text': ','.join(str(v) for v in external_links),
                    'packageName': export['thread_id'],
                    'comment': export['title']
                }

                after_replace = re.sub('<(.+?) placeholder>', lambda match: values.get(match.group(1)), template_json)
                filename = export['thread_id'] + '.crawljob'
                
                with open(filename, 'w') as f:
                    f.write(after_replace)
        
        self.client.close()

    def process_item(self, item, spider):
        exists = self.db[self.collection_name].find_one({'thread_id': dict(item)['thread_id']}, {'_id': False, 'thread_id': True})
        if not exists:
            ## insert if not exists
            self.db[self.collection_name].insert_one(ItemAdapter(item).asdict())
        else:            
            meta_values = dict(item)['meta']
            ## Remove an entry from META array when created_date is different from
            ## meta_values created_date object            
            for meta_value in meta_values:
                self.db[self.collection_name].update_one(exists,
                     {'$pull': {'meta': {'$and': [{'post_id': {'$eq': meta_value['post_id']}}, {'created_date': {'$ne': meta_value['created_date']}}]}}}
                )
            
            ## adds a new entry to META array unless the value is already present 
            ## in which case $SET does nothing to that array.
            self.db[self.collection_name].update_one(exists,
            [
                {'$set':  {'meta':  {'$concatArrays': [ 
                    "$meta",  
                    {'$filter': {
                            'input': meta_values,
                            'cond': {'$not': {'$in': [ '$$this.post_id', '$meta.post_id' ]}} 
                    }}
                ]}}}
            ])
        return item    