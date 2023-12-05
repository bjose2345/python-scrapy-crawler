from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import dotenv
## default directory for .env file is the current directory
## if you set .env in different directory, put the directory address load_dotenv("directory_of_.env)
dotenv.load_dotenv()

process = CrawlerProcess(get_project_settings())

## 'as_spider' is the name of the spiders of the project.
process.crawl("as_spider")
process.start()  # the script will block here until the crawling is finished