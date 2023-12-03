# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sys
import os
import re
from datetime import datetime
import pymongo
from pathlib import Path
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

    def __init__(self, mongodb_uri, mongodb_db):
        self.mongodb_uri = mongodb_uri
        self.mongodb_db = mongodb_db
        if not self.mongodb_uri: sys.exit("You need to provide a Connection String.")

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongodb_uri=crawler.settings.get('MONGODB_URI'),
            mongodb_db=crawler.settings.get('MONGODB_DATABASE')
        )

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

        ## create a crawljob file for each one of the post found, with stage null and grouped by product_id
        exports = self.db[self.collection_name].aggregate([
            {
                "$unwind": "$details"
            },
            {
                "$unwind": "$details.external_links"
            },    
            {
                "$match": {
                  "product_id": {
                    "$ne": None
                  },
                  "details.stage": None
                }
            },
            {
                "$group": {
                  "_id": "$product_id",
                  "post_id": {
                    "$push": "$details.post_id"
                  },
                  "external_links": {
                    "$push": "$details.external_links"
                  }
                }
            }    
        ])


        p = Path(__file__).with_name('crawljob.template')

        if exports:
            today = datetime.today().strftime('%Y%m%d-%H%M%S')

            with p.open('r') as f:
                template_json = f.read()

            for export in exports:
                for post_id in export['post_id']:
                    ## update stage to FETCHED status
                    self.db[self.collection_name].update_one({'details.post_id': post_id}, {'$set': {'details.$[].stage': 'FETCHED'}})
                    
                values = {
                    'text': '[' + ', '.join(str(x) for x in export['external_links']) + ']',
                    'packageName': export['_id'],
                    'comment': 'Created at ' + today
                }

                after_replace = re.sub('<(.+?) placeholder>', lambda match: values.get(match.group(1)), template_json)
                output_path = Path(OUTPUT)
                output_path.mkdir(exist_ok=True)
                filename = export['_id'] + '_' + today + '.crawljob'
                
                with open(output_path + os.sep + filename, 'w') as f:
                    f.write(after_replace)
        
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