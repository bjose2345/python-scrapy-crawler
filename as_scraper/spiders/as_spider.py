import scrapy
import re
from as_scraper.items import PostItem, PostDetailsItem

THREAD_MAX_PAGE_NUM = 1
PAGE_MAX_PAGE_NUM = 2

PLATFORMS = [
    'getchu.com',
    'dlsite.com',
    'fanza.net',
    'dmm.co.jp'
]

DELIMETERS = ["/", "="]

class AsSpider(scrapy.Spider):

    name = "as_spider"
    allowed_domains = ["anime-sharing.com"]
    start_urls = ["https://www.anime-sharing.com/forums/hentai-games.38/"]

    def __init__(self):
        self.platforms_whitelisted = re.compile('|'.join([re.escape(word) for word in PLATFORMS]))
        self.pattern = "[" + re.escape("".join(DELIMETERS)) + "]"

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
        post_item['platform'] = None
        post_item['product_id'] = None
        messages = response.css('article.message--post')

        detail_items = []
        for message in messages:
            post_details_item = PostDetailsItem()
            post_number = message.css('ul.message-attribution-opposite a::text')[2].get().strip()
            ## search for the platform and product_id in the first post
            if(post_number=='#1'):
                ## search for all the links that startwith http and end with any number
                ## ex.
                ## https://www.dlsite.com/maniax/work/=/product_id/RJ00000001.html
                ## https://www.getchu.com/soft.phtml?id=000001
                ## https://www.dmm.co.jp/dc/doujin/-/detail/=/cid=d_000001/
                all_links = re.findall('http.*?\d+',message.get())                
                result = next(iter(word for word in all_links if self.platforms_whitelisted.search(word)), None)                
                if result is not None:
                    ## overwrite the default value in case we found something
                    post_item['platform'] = next(iter(sub for sub in PLATFORMS if sub in result), None)                    
                    post_item['product_id'] = re.split(self.pattern,result)[-1]

            post_details_item['stage'] = None
            post_details_item['post_id'] = message.css('article.message--post::attr(data-content)').get()            
            post_details_item['created_date'] = message.css('li.u-concealed time.u-dt::attr(data-time)').get()
            post_details_item['external_links'] = message.css('article.message--post  a.link--external::attr(href)').getall()
            detail_items.append(post_details_item)

        post_item['details'] = detail_items

        ## check if the post has mutliple pages
        next_page_response = response.css('li.pageNav-page--later a::attr(href)').get() #'/threads/xxx.xxx/page-?
        if next_page_response is not None:
            ## add the current page to the thread-id, to make it unique
            post_item['thread_id'] = post_item['thread_id'] + '-' + response.css('li.pageNav-page--current a::text').get()
            next_page = next_page_response.rpartition('/')[2] #page-?        
            next_page_number = re.sub(r"\D", "", next_page) #?
            
            if int(next_page_number) <= PAGE_MAX_PAGE_NUM:
                next_page_url = response.request.url + next_page
                yield response.follow(next_page_url, callback= self.parse_post_detail)
        
        yield post_item