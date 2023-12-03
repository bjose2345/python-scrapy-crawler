# Getting Started - Cheat Sheet

1. Clone this project `https://github.com/bjose2345/python-scrapy-crawler.git`
2. Create a Python Virtual Environment: `python3 -m venv venv`
3. Activate the Python Virtual Environment: `source venv/bin/activate`
4. Install Scrapy using pip: `pip install scrapy`
5. To install the required modules for this python project to run you need to install the required python modules using the following command: `pip install -r requirements.txt`
6. create a .env file and add these keys with their value pairs `SCRAPEOPS_API_KEY=xxx`, `MONGODB_USERNAME=xxx`, `MONGODB_PASSWORD=xxx` `MONGODB_URI=xxx` `MONGODB_DATABASE=xxx`
7. Listing the scrapy projects `scrapy list`
8. Running the scrapy project: `python3 run_as_spider_without_exports` in case just want to run and save or `python3 run_as_spider_with_exports` to run, save and export

# Helpful Debugging

If you have issues running the `pip install -r requirements.txt` command this can be due to some things not being up to date on your computer.

Running the following may solve some of these issues:

`pip install --upgrade pip`
