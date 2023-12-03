import os
import re
from datetime import datetime
from pathlib import Path
import pymongo
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import dotenv
## default directory for .env file is the current directory
## if you set .env in different directory, put the directory address load_dotenv("directory_of_.env)
dotenv.load_dotenv()

OUTPUT_DIRECTORY = os.path.join(os.getcwd(), r'ourput')

mongodb_uri = os.getenv('MONGODB_URI')
mongodb_db = os.getenv('MONGODB_DB')
mongo_username = os.getenv('MONGODB_USERNAME')
mongo_password = os.getenv('MONGODB_PASSWORD')
client = pymongo.MongoClient(
            mongodb_uri, 
            username=mongo_username, 
            password=mongo_password
            )
db = client[mongodb_db]
collection_name = "as_items"

process = CrawlerProcess(get_project_settings())

## 'as_spider' is the name of the spiders of the project.
process.crawl("as_spider")
process.start()  # the script will block here until the crawling is finished

## create a crawljob file for each one of the post found with stage null and grouped by product_id
exports = db[collection_name].aggregate([
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

if exports:

    p = Path(__file__).with_name('crawljob.template')
    today = datetime.today().strftime('%Y%m%d-%H%M%S')

    with p.open('r') as f:
        template_json = f.read()

    for export in exports:
        for post_id in export['post_id']:
            ## update stage to FETCHED status
            db[collection_name].update_one({'details.post_id': post_id}, {'$set': {'details.$[].stage': 'FETCHED'}}) 

        values = {
            'text': '[' + ', '.join(str(x) for x in export['external_links']) + ']',
            'packageName': export['_id'],
            'comment': 'Created at ' + today
        }

        after_replace = re.sub('<(.+?) placeholder>', lambda match: values.get(match.group(1)), template_json)
        output_directory_path = Path(OUTPUT_DIRECTORY)
        output_directory_path.mkdir(exist_ok=True)
        filename = export['_id'] + '_' + today + '.crawljob'
        
        with open(str(output_directory_path) + os.sep + filename, 'w') as f:
            f.write(after_replace)

client.close()