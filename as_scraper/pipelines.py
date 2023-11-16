# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re

WHITELIST_LINKS = ['rapidgator.net','katfile.com','rosefile.net','mexa.sh','fikper.com','ddownload.com','k2s.cc','fboom.me']

class AsScraperPipeline:
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)

        ## strip all whitespaces from strings
        value =adapter.get('title')        
        adapter['title'] = value.strip()
            
        ## remove unnecessary links
        
        # This should outperform any explicit testing, especially as the number of words in the blacklist grows
        whitelist = re.compile('|'.join([re.escape(word) for word in WHITELIST_LINKS]))        
        value =adapter.get('external_links')
        adapter['external_links'] = [word for word in value if whitelist.search(word)]
            
        return item