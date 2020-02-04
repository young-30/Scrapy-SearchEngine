import re
import json
import datetime
import time          #time.sleep()使用
import pickle   #pickle模块实现了基本的数据序列和反序列化，但兼容性很差，重要的数json。这里在据需要用处理cookie时使用
import datetime   #crawl_time使用
from settings import BASE_DIR     #模拟登录存储cookie时使用


#python2与python3的兼容写法
try:
    import urlparse as parse
except:
    from urllib import parse


import scrapy
from scrapy.loader import ItemLoader
from ArticleSpider.items import ZhihuQuestionItem, ZhihuAnswerItem

from selenium import webdriver  #引入webdriver，这里使用下载的chrome的driver(selenium模拟登陆使用)


class ZhihuSpider(scrapy.Spider):
    name = "zhihu_sel"
    allowed_domains = ["www.zhihu.com"]
    start_urls = ['https://www.zhihu.com/']
    #question的第一页answer的请求url
    strat_answer_urls = "https://www.zhihu.com/api/v4/questions/{0}/answers?include=data%5B*%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%3Bdata%5B*%5D.mark_infos%5B*%5D.url%3Bdata%5B*%5D.author.follower_count%2Cbadge%5B*%5D.topics&offset={1}&limit={2}&sort_by=default&platform=desktop"


    headers = {
        "HOST": "www.zhihu.com",
        "Referer": "https://www.zhihu.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0"
    }

    custom_settings = {
        "COOKIES_ENABLED": True
    }

    def parse(self, response):
        """
        提取出html页面中的所有url 并跟踪这些url进一步爬取
        如果提取的url中格式为 /question/xxx 就下载之后直接进入解析函数
        """
        pass
        all_urls = response.css("a::attr(href)").extract()
        all_urls_withdomain = [parse.urljoin(response.url, url) for url in all_urls]
        #filter函数返回的是filter对象，必须转换为list
        all_urls_final = list(filter(lambda x:True if x.startswith("https") else False, all_urls_withdomain))
        for url in all_urls_final:
            print(url)
            match_obj = re.match("(.*zhihu.com/question/(\d+))(/|$).*", url)  #提取完整url与id
            if match_obj:
                #如果提取到question相关的页面则下载后交由提取函数进行提取
                request_url = match_obj.group(1)
                question_id = match_obj.group(2)
                yield scrapy.Request(url=request_url, headers=self.headers, callback=self.parse_question)
                # break #用于调试，只发一个yield
            else:
                pass #用于调试
                #如果不是question页面则直接进行进一步跟踪
                # yield scrapy.Request(url, headers=self.headers, callback=self.parse)  #不设置callback也可以，因为自动调用的就是parse函数

    def parse_question(self, response):
        #处理question页面，从页面中提取出具体的question item
          #获得zhihu_id字段(question_id)，也可以通过在request中meta方法直接传递获得(这里直接转换为了int类型)
        match_obj = re.match("(.*zhihu.com/question/(\d+))(/|$).*", response.url)
        if match_obj:
            question_id = int(match_obj.group(2))

        item_loader = ItemLoader(item=ZhihuQuestionItem(), response=response) #传入的item是一个ZhihuQuestionItem的实例
        item_loader.add_css("title", "h1.QuestionHeader-title::text")
        item_loader.add_css("content", ".QuestionHeader-detail")
        item_loader.add_value("url", response.url)
        item_loader.add_value("zhihu_id", question_id)
        item_loader.add_css("answer_num", "h4.List-headerText span::text") #("answer_num", "div#QuestionAnswers-answers")
        item_loader.add_css("comments_num", ".QuestionHeader-Comment button::text")
        item_loader.add_css("watch_user_num", ".NumberBoard-itemValue::text") #这里为一个列表包含两个数值，一个关注数，一个浏览数(click_num)
        item_loader.add_css("topics", ".QuestionHeader-topics .Popover div::text")

        question_item = item_loader.load_item()

        yield scrapy.Request(self.strat_answer_urls.format(question_id, 0, 5), callback=self.parse_answer, headers=self.headers)
        pass
        yield question_item  # 传到pipeline里


    def parse_answer(self, response):
        #处理question的answer
        ans_json = json.loads(response.text) #将json数据转换为dict
        is_end = ans_json["paging"]["is_end"]
        totals_answer = ans_json["paging"]["totals"]
        next_url = ans_json["paging"]["next"]

        #提取answer的具体字段：
        for answer in ans_json["data"]:
            answer_item = ZhihuAnswerItem() #为每个回答生成一个item
            #开始提取：
            answer_item["zhihu_id"] = answer["id"] #该条回答的id
            answer_item["url"] = answer["url"]
            answer_item["question_id"] = answer["question"]["id"]
            answer_item["author_id"] = answer["author"]["id"] if "id" in answer["author"] else None #可能是匿名回答，导致没有id
            answer_item["content"] = answer["content"] if "content" in answer else None #conten有可能为空(概率极小)
            answer_item["praise_num"] = answer["voteup_count"]
            answer_item["comments_num"] = answer["comment_count"]
            answer_item["create_time"] = answer["created_time"]
            answer_item["update_time"] = answer["updated_time"]
            answer_item["crawl_time"] = datetime.datetime.now()

            yield answer_item

        if not is_end:
            yield scrapy.Request(next_url, callback=self.parse_answer, headers=self.headers)  #调用下面五个item数据

    def start_requests(self):
        #使用firefox的webdriver：
        # from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
        # binary = FirefoxBinary('D:\\火狐浏览器\\firefox.exe')
        # browser = webdriver.Firefox(executable_path="F:/temp/geckodriver-v0.15.0-win64/geckodriver.exe", firefox_binary=binary)

        #先检测是否可以直接加载cookie文件，若不能，则调用selenium模拟登陆
        try:
            with open(BASE_DIR+'/cookies/zhihu/cookie.txt', 'rb') as fp:
                pickle.load(fp)  #若cookie文件为空，这该处会报错，跳转到except
            cookies_data = []
            with open(BASE_DIR+'/cookies/zhihu/cookie.txt', 'rb') as f:
                while True:
                    try:
                        cookies_data.append(pickle.load(f))  #pickle自动生成结尾符，每次load只会读取一行数据
                    except:
                        break
        except:
            browser = webdriver.Chrome(executable_path="F:/temp/chromedriver_win32/chromedriver.exe")
            browser.get("https://www.zhihu.com/signin")   #需要操作的url
            browser.find_element_by_css_selector(".SignFlow-accountInput.Input-wrapper input").send_keys("13982992916") #写入用户名
            browser.find_element_by_css_selector(".SignFlow-password input").send_keys("admin123")  #写入密码
            browser.find_element_by_css_selector(".Button.SignFlow-submitButton").click()           #点击登录按钮

            time.sleep(5)   #等待浏览器登录成功后跳转到首页
            Cookies = browser.get_cookies()  #获取cookie，此时是已经登录的cookie
            cookies_data = {}
            with open(BASE_DIR+'/cookies/zhihu/cookie.txt', 'wb') as f:
                for cookie in Cookies:
                    #写入文件
                    #f = open('F:/慕课1_Python分布式爬虫集成搜索引擎/代码/ArticleSpider/ArticleSpider/cookies/zhihu/' + cookie['name'] + '.zhihu', 'wb')
                    pickle.dump(cookie,f)   #使用pickle写入文件
                    cookies_data[cookie['name']] = cookie['value']  #使用这两个键值即可作为cookie登入知乎
            browser.close()
        return [scrapy.Request(url=self.start_urls[0], dont_filter=True, cookies=cookies_data, headers=self.headers)] #登录时将本机保存的已登录的cookie直接load进来，不用再进行登录操作。
              #dont_filter保证该request不会被知乎过滤掉；要在request中使用cookies，需要在settings文件中置 COOKIES_ENABLED = True
              #这里的request没有写callback参数，它会自己跳转到默认的parse函数里



