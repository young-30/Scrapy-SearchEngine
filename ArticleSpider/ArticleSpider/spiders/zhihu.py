# -*- coding: utf-8 -*-
import scrapy
import json

import re


class ZhihuSpider(scrapy.Spider):
    name = 'zhihu'
    allowed_domains = ['www.zhihu.com']
    start_urls = ['http://www.zhihu.com/']

    headers = {
        "HOST": "www.zhihu.com",
        "Referer": "https://www.zhihu.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0"
    }


    def parse(self, response):
        pass

    #重写start_requests，完成模拟登录功能
    def start_requests(self):
        return [scrapy.Request('http://www.zhihu.com/#signin', headers=self.headers, callback=self.login)]

    def login(self, response):
        response_text = response.text #获取response返回的内容，用以处理
        #使用正则表达式提取_xsrf
        match_obj = re.match('.*name="_xsrf" value="(.*?)"', response.text, re.DOTALL)  #re.DOTALL表示匹配全文，否则只匹配头一行
        xsrf = ''
        if match_obj:
            xsrf = match_obj.group(1)
        if xsrf:     #xsrf存在才有必要进行登录操作
            post_url = "https://www.zhihu.com/login/phone_num"
            post_data = {
                "_xsrf": xsrf,
                "phone_num": "13982992916",
                "password": "admin123"
            }
            return [scrapy.FormRequest(
                url=post_url,
                formdata=post_data,
                headers=self.headers,
                callback=self.check_login   #只能调用函数的对象，不能写成check_login()，否则会将值传递过去
            )]

    def check_login(self, response):
        #验证服务器的返回数据判断是否成功
        text_json = json.loads(response.text)
        if "msg" in text_json and text_json["msg"] == "登录成功":
            for url in self.start_urls:
                yield scrapy.Request(url, dont_filter=True, headers=self.headers)  #自动调用cookie
        pass
