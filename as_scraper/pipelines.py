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
from scrapy.utils.project import get_project_settings

settings=get_project_settings()
FILEHOSTS = settings.get('FILEHOSTS')
OUTPUT = settings.get('OUTPUT')

class AsScraperFormatPipeline:
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)
        
        ## strip all whitespaces from strings
        value = adapter.get('title')        
        adapter['title'] = value.strip()
            
        ## remove external links that aren't in the whitelist
        filehosts_whitelisted = re.compile('|'.join([re.escape(word) for word in FILEHOSTS]))        
        details = adapter.get('details')
        for detail in details:
            detail['external_links'] = [word for word in detail['external_links'] if filehosts_whitelisted.search(word)]
                  
        return item
    
class CleanEmptyPostsPipeline:
   
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)
        
        details = adapter.get('details')        
        for detail in details[:]:            
            if not detail['external_links']:
                details.remove(detail)
       
        return item

class MongoDBPipeline:

    collection_name = "as_items"

    def __init__(self):
        self.mongodb_uri = os.getenv('MONGODB_URI')
        self.mongodb_db = os.getenv('MONGODB_DB')
        if not self.mongodb_uri: sys.exit("You need to provide a Connection String.")

    def open_spider(self, spider):
        self.mongo_username = os.getenv('MONGODB_USERNAME')
        self.mongo_password = os.getenv('MONGODB_PASSWORD')
        self.client = pymongo.MongoClient(
                        self.mongodb_uri, 
                        username=self.mongo_username, 
                        password=self.mongo_password
                        )
        self.db = self.client[self.mongodb_db]

    def close_spider(self, spider):        
        self.client.close()

    def process_item(self, item, spider):
        exists = self.db[self.collection_name].find_one({'thread_id': dict(item)['thread_id']}, {'_id': False, 'thread_id': True})
        if not exists:
            ## insert if not exists and details is not empty
            if dict(item)['details']:
                self.db[self.collection_name].insert_one(ItemAdapter(item).asdict())
        else:            
            details = dict(item)['details']
            ## Remove an document from DETAILS array when created_date is different from
            ## details.created_date entry
            for detail in details:
                self.db[self.collection_name].update_one(exists,
                     {'$pull': {'details': {'$and': [{'post_id': {'$eq': detail['post_id']}}, {'created_date': {'$ne': detail['created_date']}}]}}}
                )
            
            ## adds a new entry to DETAILS array unless the value is already present 
            ## in which case $SET does nothing to that array.
            self.db[self.collection_name].update_one(exists,
            [
                {'$set':  {'details':  {'$concatArrays': [ 
                    "$details",  
                    {'$filter': {
                            'input': details,
                            'cond': {'$not': {'$in': [ '$$this.post_id', '$details.post_id' ]}} 
                    }}
                ]}}}
            ])
        return item    