import scrapy
import re
from as_scraper.items import PostItem

MAX_PAGE_NUM = 2

class AsSpiderSpider(scrapy.Spider):
    name = "as_spider"
    allowed_domains = ["anime-sharing.com"]
    start_urls = ["https://www.anime-sharing.com/forums/hentai-games.38/"]

    def parse(self, response):
        posts = response.css('div.structItemContainer div.structItem-title')

        for post in posts:
            a_list = post.css('a::attr(href)').getall()
            relative_url = "".join(filter(lambda x: 'threads' in x, a_list))
            if(relative_url):
                post_url = 'https://www.anime-sharing.com' + relative_url
                yield response.follow(post_url, callback=self.parse_post_detail)

        next_page_response = response.css('li.pageNav-page--later a::attr(href)').get() #'/forums/hentai-games.38/page-?
        next_page = next_page_response.rpartition('/')[2] #page-?        
        next_page_number = re.sub(r"\D", "", next_page) #?
        
        if int(next_page_number) <= MAX_PAGE_NUM:
            next_page_url = 'https://www.anime-sharing.com/forums/hentai-games.38/' + next_page            
            yield response.follow(next_page_url, callback= self.parse)

    def parse_post_detail(self, response):

        post_item = PostItem()
        
        post_item['title'] = response.css('.p-title h1::text').get()
        post_item['external_links'] = response.css('div.message-inner a.link--external::attr(href)').getall()

        yield post_item