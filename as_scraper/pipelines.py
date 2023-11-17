# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import re

WHITELIST_LINKS = ['rapidgator.net','katfile.com','rosefile.net','mexa.sh','fikper.com','ddownload.com','k2s.cc','fboom.me']

class AsScraperPipeline:
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)
        
        ## strip all whitespaces from strings
        value = adapter.get('title')        
        adapter['title'] = value.strip()
            
        ## remove unnecessary links
        whitelist = re.compile('|'.join([re.escape(word) for word in WHITELIST_LINKS]))        
        meta_values = adapter.get('meta')
        for meta_value in meta_values:
            filtered_external_links = [word for word in meta_value['external_links'] if whitelist.search(word)]
            # remove message if not external links
            if not filtered_external_links:
                meta_values.remove(meta_value)
            meta_value['external_links'] = filtered_external_links
                  
        return item