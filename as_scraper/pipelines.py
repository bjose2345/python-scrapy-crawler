# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re

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