# -*- coding: utf-8 -*-
import scrapy
import re  #正则表达式
import datetime #引入该库处理create_time字段
from scrapy.http import  Request
from urllib import parse #parse模块可以提取response中的主域名

from ArticleSpider.items import JobBoleArticleItem, ArticleItemLoader  #引入items.py中的类,ArticleItemLoader为自定义itemloader
from ArticleSpider.utils.common import get_md5   #引入md5函数，方面items处理url_object_id字段
from scrapy.loader import ItemLoader  #引入itemloader

class JobboleSpider(scrapy.Spider):
    name = 'jobbole'
    allowed_domains = ['blog.jobbole.com']
    start_urls = ['http://blog.jobbole.com/all-posts/']

    def parse(self, response):
        '''
        1. 获取文章列表页中的文章url并交给scrapy下载后并进行解析
        2. 获取下一页的url并交给scrapy进行下载，下载完成后交给parse
        '''

        #解析列表页中所有文章url并交给scrapy下载后并进行解析
        m=response.url   #测试返回的url
        post_nodes = response.css("#archive .floated-thumb .post-thumb a")
        for post_node in post_nodes:
            image_url = post_node.css("img::attr(src)").extract_first("")
            post_url = post_node.css("::attr(href)").extract_first("")
            yield Request(url=parse.urljoin(response.url, post_url), meta={"front_image_url":image_url}, callback=self.parse_detail)
            #yield将内容返回给scrapy进行下载
            print(post_url)  #测试post_url

        #提取下一页并交给scrapy进行下载
        next_url = response.css(".next.page-numbers::attr(href)").extract_first() #css中class标签包含两个空格隔开的值来修饰一个字段则修饰器里的空格去掉即可
        if next_url:
            yield Request(url=parse.urljoin(response.url,next_url), callback=self.parse)


    def parse_detail(self, response):
        # article_item = JobBoleArticleItem() #实例化JobBoleArticleItem

    #通过xpath提取字段
        # #提取文章具体字段
        #
        # re_selector = response.xpath("/html/body/div[1]/div[3]/div[1]/div[1]/h1/text()")
        # re_selector2 = response.xpath('//*[@id="post-114633"]/div[1]/h1')
        # re_selector3 = response.xpath('//div[@class="entry-header"]/h1/text()')
        #
        # # 取标题
        # title = re_selector3.extract()[0]  #可用extract_first()
        # # 取创建日期
        # create_date = response.xpath("//p[@class='entry-meta-hide-on-mobile']/text()").extract()[0].strip().replace("·",                                                                                                                    "").strip()
        # # 取点赞数
        # praise_nums = int(response.xpath("//span[contains(@class,'vote-post-up')]/h10/text()").extract()[0])
        #
        # #取收藏数
        # fav_nums =  response.xpath("//span[contains(@class,'bookmark-btn')]/text()").extract()[0]
        # match_re = re.match(".*?(\d+).*",fav_nums)
        # if match_re:
        #     fav_nums = int(match_re.group(1))
        # else:
        #     fav_nums = 0
        #
        # #取评论数
        # comment_nums = response.xpath("//a[contains(@href,'#article-comment')]/span/text()").extract()[0]
        # match_re = re.match(".*?(\d+).*", comment_nums)
        # if match_re:
        #     comment_nums = int(match_re.group(1))
        # else:
        #     comment_nums = 0
        #
        # #取文章内容
        # content = response.xpath("//div[@class='entry']").extract()[0]
        #
        # #取标签
        # tag_list = response.xpath("//p[@class='entry-meta-hide-on-mobile']/a/text()").extract()
        # [element for element in tag_list if not element.strip().endswith("评论")] #如果tag_list模式为 ['IT技术', ' 1 评论 ','TCP']
        # tags = ",".join(tag_list)   #将列表变为字符串

    # #通过css选择器提取字段
    #     front_image_url = response.meta.get("front_image_url","") #文章封面图
    #     title = response.css(".entry-header h1::text").extract()
    #     create_date = response.css("p.entry-meta-hide-on-mobile::text").extract()[0].strip().replace("·","").strip()
    #     praise_nums = response.css(".vote-post-up h10::text").extract()[0]
    #
    #     fav_nums = response.css("span.bookmark-btn::text").extract()[0]
    #     match_re = re.match(".*?(\d+).*", fav_nums)
    #     if match_re:
    #         fav_nums = int(match_re.group(1))
    #     else:
    #         fav_nums = 0
    #
    #     comment_nums = response.css("a[href='#article-comment'] span::text").extract()[0]
    #     match_re = re.match(".*?(\d+).*", comment_nums)
    #     if match_re:
    #         comment_nums = int(match_re.group(1))
    #     else:
    #         comment_nums = 0
    #
    #     content = response.css("div.entry").extract()[0]
    #
    #     tag_list = response.css("p.entry-meta-hide-on-mobile a::text").extract()
    #     tags = ",".join(tag_list)
    #
    #
    #     article_item["url_object_id"] = get_md5(response.url)
    #     article_item["title"] = title
    #     article_item["url"] = response.url
    #
    #     #处理create_date字段数据类型为date类型，便于数据库操作
    #     try:
    #         create_date = datetime.datetime.strptime(create_date,"%Y/%m/%d").date()
    #     except Exception as e:
    #         create_date = datetime.datetime.now().date()
    #     article_item["create_date"] = create_date
    #      #article_item['create_date'] = article_item['create_date'].strftime('%Y-%m-%d') #格式问题？
    #
    #     article_item["front_image_url"] = [front_image_url]  #列表传递
    #     article_item["praise_nums"] = praise_nums
    #     article_item["comment_nums"] = comment_nums
    #     article_item["fav_nums"] = fav_nums
    #     article_item["tags"] = tags
    #     article_item["content"] = content
    #      #front_image_path在pipelines.py中进行处理

    #通过item loader加载item
        item_loader = ArticleItemLoader(item=JobBoleArticleItem(), response=response) #实例化
        #xpath解释器：
        #item_loader.add_xpath()

        #css解释器：
        front_image_url = response.meta.get("front_image_url", "")  # 文章封面图
        item_loader.add_css("title", ".entry-header h1::text")
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_css("create_date", "p.entry-meta-hide-on-mobile::text")
        item_loader.add_value("front_image_url", [front_image_url])
        item_loader.add_css("praise_nums", ".vote-post-up h10::text")
        item_loader.add_css("comment_nums", "a[href='#article-comment'] span::text")
        item_loader.add_css("fav_nums", "span.bookmark-btn::text")
        item_loader.add_css("tags", "p.entry-meta-hide-on-mobile a::text")
        item_loader.add_css("content", "div.entry")

        article_item = item_loader.load_item() #将上面的规则解析，获得item

        yield  article_item  #传递到pipelines.py中
