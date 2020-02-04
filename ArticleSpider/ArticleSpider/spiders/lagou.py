# -*- coding: utf-8 -*-
import os #读使用取登录的cookie信息
import pickle #写下登录的cookie信息
from datetime import datetime
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from settings import BASE_DIR     #模拟登录存储cookie时使用
from items import LagouJobItemLoader, LagouJobItem
from ArticleSpider.utils.common import get_md5 #处理url_object_id字段

class LagouSpider(CrawlSpider):
    name = 'lagou'
    allowed_domains = ['www.lagou.com']
    start_urls = ['https://www.lagou.com/']

    custom_settings = {
        "COOKIES_ENABLED": True,
        "COOKIES_DEBUG": True
    }

    rules = (
        # Rule(LinkExtractor(allow=("zhaopin/.*",)),follow=True),
        # Rule(LinkExtractor(allow=("gongsi/j\d+.html",)), follow=True),
        Rule(LinkExtractor(allow=r'jobs/\d+.html'), callback='parse_job', follow=True),
    )

    # def parse_start_url(self, response):
    #     return []
    #
    # def process_results(self, response, results):
    #     return results

    def start_requests(self):
        #使用selenium模拟登录后拿到cookie交给scrapy的request使用
        #1、通过selenium模拟登录
        #从文件中读取cookies
        cookies = []
        if os.path.exists(BASE_DIR+"/cookies/lagou/cookie.txt"):
            cookies = pickle.load(open(BASE_DIR+"/cookies/lagou/cookie.txt", "rb"))

        if not cookies:
            from selenium import webdriver
            browser = webdriver.Chrome(executable_path="F:/temp/chromedriver_win32/chromedriver.exe")
            browser.get("https://passport.lagou.com/login/login.html")
            browser.find_element_by_css_selector('.form_body input[type="text"]').send_keys("18782081493")
            browser.find_element_by_css_selector('.form_body input[type="password"]').send_keys("123456")
            browser.find_element_by_css_selector('div[data-view="passwordLogin"] input.btn_lg').click()
            import time
            time.sleep(10)
            cookies = browser.get_cookies()

            #写入cookie到文件中
            pickle.dump(cookies, open(BASE_DIR+"/cookies/lagou/cookie.txt", "wb"))

        cookie_dict = {}
        for cookie in cookies:
            cookie_dict[cookie["name"]] = cookie["value"]

        for url in self.start_urls:    #重写start_requests前的逻辑不能丢掉
            yield scrapy.Request(url, dont_filter=True, cookies=cookie_dict)



    def parse_job(self, response):
        #解析拉勾网的职位信息
        item_loader = LagouJobItemLoader(item=LagouJobItem(), response=response)
        item_loader.add_css("title", ".job-name::attr(title)")
        item_loader.add_value( "url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_css("salary", ".job_request .salary::text")
        item_loader.add_xpath("job_city", "//*[@class='job_request']/p/span[2]/text()")
        item_loader.add_xpath("work_years", "//*[@class='job_request']/p/span[3]/text()")
        item_loader.add_xpath("degree_need", "//*[@class='job_request']/p/span[4]/text()")
        item_loader.add_xpath("job_type", "//*[@class='job_request']/p/span[5]/text()")

        item_loader.add_css("tags", '.position-label li::text')
        item_loader.add_css("publish_time", ".publish_time::text")
        item_loader.add_css("job_advantage", ".job-advantage p::text")
        item_loader.add_css("job_desc", ".job_bt div")
        item_loader.add_css("job_addr", ".work_addr")
        item_loader.add_css("company_name", "#job_company dt a img::attr(alt)")
        item_loader.add_css("company_url", "#job_company dt a::attr(href)")
        item_loader.add_value("crawl_time", datetime.now())

        job_item = item_loader.load_item()

        return job_item


