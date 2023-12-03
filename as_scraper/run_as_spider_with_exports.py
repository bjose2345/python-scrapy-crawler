from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from exports import *

process = CrawlerProcess(get_project_settings())

## 'as_spider' is the name of the spiders of the project.
process.crawl("as_spider")
process.start()  # the script will block here until the crawling is finished

## create a crawljob file for each one of the post found with stage null and grouped by product_id
group_by_platform_id.execute()