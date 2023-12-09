import os
import re
from datetime import datetime
from pathlib import Path
import pymongo

# change this var to use a different path form the default one [as_craper/output]
output_directory = None

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

def execute():

    global output_directory
    ## this export is when the product_id is null and is a single page thread
    ## create a crawljob file for each one of the post found with stage null and grouped by thread_id

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
                "$eq": None
            },
            "thread_id": {
                "$regex": "^((?!page).)*$"
            },
            "details.stage": None
            }
        },
        {
            "$group": {
            "_id": "$thread_id",
            "title": {
                "$first": "$title"
            },
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
            ## update stage to FETCHED status
            db[collection_name].update_many({'details.post_id': {'$in': export['post_id']}}, {'$set': {'details.$[].stage': 'FETCHED'}})

            values = {
                'text': '[' + ', '.join(str(x) for x in export['external_links']) + ']',
                'packageName': export['_id'],
                'comment': export['title']
            }

            after_replace = re.sub('<(.+?) placeholder>', lambda match: values.get(match.group(1)), template_json)
            if output_directory is None:
                output_directory = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'output'))
            output_directory_path = Path(output_directory)
            output_directory_path.mkdir(exist_ok=True)
            filename = export['_id'] + '_' + today + '.crawljob'
            
            with open(str(output_directory_path) + os.sep + filename, 'w') as f:
                f.write(after_replace)

    client.close()

if __name__ == '__main__':
   execute()