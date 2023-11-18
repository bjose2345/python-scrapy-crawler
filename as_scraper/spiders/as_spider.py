import scrapy
import re
from as_scraper.items import PostItem, PostMetaItem

THREAD_MAX_PAGE_NUM = 1
PAGE_MAX_PAGE_NUM = 2

class AsSpider(scrapy.Spider):
    name = "as_spider"
    allowed_domains = ["anime-sharing.com"]
    start_urls = ["https://www.anime-sharing.com/forums/hentai-games.38/"]

    def parse(self, response):
         
        threads = response.css('div.structItemContainer div.structItem-title')

        for thread in threads:
            a_list = thread.css('a::attr(href)').getall()
            relative_url = "".join(filter(lambda x: 'threads' in x, a_list))
            if(relative_url):
                thread_url = 'https://www.anime-sharing.com' + relative_url
                
                yield response.follow(thread_url, callback=self.parse_post_detail)

        next_page_response = response.css('li.pageNav-page--later a::attr(href)').get() #'/forums/hentai-games.38/page-?
        next_page = next_page_response.rpartition('/')[2] #page-?        
        next_page_number = re.sub(r"\D", "", next_page) #?
        
        if int(next_page_number) <= THREAD_MAX_PAGE_NUM:
            next_page_url = response.request.url + next_page            
            yield response.follow(next_page_url, callback= self.parse)

    def parse_post_detail(self, response):

        post_item = PostItem()

        post_item['thread_id'] = response.css('div.block-container::attr(data-lb-id)').get()
        post_item['title'] = response.css('.p-title h1::text').get()
        messages = response.css('article.message--post')

        meta_items = []
        for message in messages:
            post_meta_item = PostMetaItem()
            post_meta_item['post_id'] = message.css('article.message--post::attr(data-content)').get()            
            post_meta_item['created_date'] = message.css('li.u-concealed time.u-dt::attr(data-time)').get()
            post_meta_item['external_links'] = message.css('article.message--post  a.link--external::attr(href)').getall()
            meta_items.append(post_meta_item)

        post_item['meta'] = meta_items

        next_page_response = response.css('li.pageNav-page--later a::attr(href)').get() #'/threads/xxx.xxx/page-?
        if next_page_response is not None:
            post_item['page'] = response.css('li.pageNav-page--current a::text').get()
            next_page = next_page_response.rpartition('/')[2] #page-?        
            next_page_number = re.sub(r"\D", "", next_page) #?
            
            if int(next_page_number) <= PAGE_MAX_PAGE_NUM:                
                next_page_url = response.request.url + next_page            
                yield response.follow(next_page_url, callback= self.parse_post_detail)
        
        yield post_item