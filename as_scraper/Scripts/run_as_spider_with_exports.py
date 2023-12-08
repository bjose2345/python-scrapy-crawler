import sys
sys.path.append('../')
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from exports import *

process = CrawlerProcess(get_project_settings())

## 'as_spider' is the name of the spiders of the project.
process.crawl("as_spider")
process.start()  # the script will block here until the crawling is finished

## create a crawljob file for each one of the exports added next,
## check the description for each export
## in case we don't need any of them we can only comment it
print('running export: group_by_platform_id ...')
group_by_platform_id.execute()
print('running export: group_by_platform_id ...')
group_by_thread_id_with_single_page.execute()
print('running export: group_by_platform_id ...')
group_by_thread_id_with_multi_pages.execute()
print('DONE')