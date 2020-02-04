# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import datetime
import re

import redis #搜索条数使用，save_to_es()里
import scrapy
from scrapy.loader import ItemLoader #自定义loader中重载该类，使得每个字段调用TakeFirst方法
from scrapy.loader.processors import MapCompose, TakeFirst, Join
#MapCompose可以传递多个函数(包括匿名函数)处理item传递的字段;TakeFirst可以用于提取列表里的第一个值;Join用于处理tags字段，连接列表内容

from utils.common import extract_num     #提取数字
from settings import SQL_DATE_FORMAT     #处理zhihuquestion中crawltime字段
from settings import SQL_DATETIME_FORMAT #如上

from w3lib.html import remove_tags #处理LagouJobItem中job_desc字段，去除html标签
from models.es_types import ArticleType, JobType, QuestionType, AnswerType #引入es操作的类(jobbole)

from elasticsearch_dsl.connections import connections
es = connections.create_connection(ArticleType._doc_type.using) #进行搜索建议字段保存

redis_cli = redis.StrictRedis() #建立redis的连接，缓存记录条数

class ArticlespiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


def add_jobbole(value):   #可用在处理title字段的mapcompose中，每个标题后加个后缀
    return value+"-Young"


def date_convert(value):  #处理日期字段
    try:
        create_date = datetime.datetime.strptime(value, "%Y/%m/%d").date()
    except Exception as e:
        create_date = datetime.datetime.now().date()

    return create_date


#提取数字函数
def get_nums(value):
    match_re = re.match(".*?(\d+).*", value)
    if match_re:
        nums = int(match_re.group(1))
    else:
        nums = 0

    return nums


def remove_comment_tags(value):
    #去掉tags中提取的评论
    if "评论" in value:
        return ""
    else:
        return value


def return_value(value):    #用于覆盖掉default_output_processor=TakeFirst
    return value

def gen_suggests(index, info_tuple):
    #根据字符串生成搜索建议数组
    used_words = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            #调用es的analyze接口分析字符串
            words = es.indices.analyze(index=index, analyzer="ik_max_word", params={'filter':["lowercase"]}, body=text)
            anylyzed_words = set([r["token"] for r in words["tokens"] if len(r["token"])>1])
            new_words = anylyzed_words - used_words
        else:
            new_words = set()

        if new_words:
            suggests.append({"input":list(new_words), "weight":weight})

    return suggests

class ArticleItemLoader(ItemLoader):
    # 自定义itemloader，即重载ItemLoader类
    default_output_processor = TakeFirst() #父类中为default_output_processor = Identity();TakeFirst方法作用为取出列表第一个值


class JobBoleArticleItem(scrapy.Item):
    title = scrapy.Field()
    create_date = scrapy.Field(
        input_processor = MapCompose(date_convert)
        #output_processor = TakeFirst()
    )    #input_processor、output_processor参数是固定写法，因为TakeFirst写入重载的itemloader中，因此可以不在这里写了
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    front_image_url = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    front_image_path = scrapy.Field()
    praise_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    comment_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    fav_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    tags = scrapy.Field(
        input_processor=MapCompose(remove_comment_tags),   #处理tags中的"评论"
        output_processor=Join(",")
    )
    content = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into jobbole_article(title, create_date, url, url_object_id, front_image_url, front_image_path, comment_nums, 
            fav_nums, praise_nums, tags, content)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (self["title"], self["create_date"], self["url"], self["url_object_id"], self["front_image_url"], "[default]",
                  self["comment_nums"], self["fav_nums"], self["praise_nums"], self["tags"], self["content"],)

        return insert_sql, params

    def save_to_es(self):
        article = ArticleType()
        article.title = self['title']
        article.create_date = self["create_date"]
        article.content = remove_tags(self["content"])
        article.front_image_url = self["front_image_url"]
        if "front_image_path" in self:
            article.front_image_path = self["front_image_path"]
        article.praise_nums = self["praise_nums"]
        article.fav_nums = self["fav_nums"]
        article.comment_nums = self["comment_nums"]
        article.url = self["url"]
        article.tags = self["tags"]
        article.meta.id = self["url_object_id"]

        article.suggest = gen_suggests(ArticleType._doc_type.index, ((article.title, 10), (article.tags, 7))) #搜索建议的保存

        article.save()

        redis_cli.incr("jobbole_count") #redis中"jobbole_count"字段加一，每执行一次就加一

        return


class ZhihuQuestionItem(scrapy.Item):
    #知乎问题的item
    zhihu_id = scrapy.Field()
    topics = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    answer_num = scrapy.Field()
    comments_num = scrapy.Field()
    watch_user_num = scrapy.Field()
    click_num = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        #插入知乎question表的sql语句
        insert_sql = """
            insert into zhihu_question(zhihu_id, topics, url, title, content, answer_num, comments_num, 
              watch_user_num, click_num, crawl_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE content=VALUES(content), answer_num=VALUES(answer_num), comments_num=VALUES(comments_num),
            watch_user_num=VALUES(watch_user_num), click_num=VALUES(click_num)
        """

        params = (zhihu_id, topics, url, title, content, answer_num, comments_num,
                  watch_user_num, click_num, crawl_time)

        return insert_sql, params

        zhihu_id = self["zhihu_id"][0] #将list转换为字符串，再变为数据库对应的int类型；
                                                  #也可直接写为int(self["zhihu_id"][0])
        topics = ",".join(self["topics"])
        url = "".join(self["url"]) #或者self["url"][0]
        title = "".join(self["title"])
        content = remove_tags("".join(self["content"]))
        answer_num = extract_num("".join(self["answer_num"]))
        comments_num = extract_num("".join(self["comments_num"]))

        if len(self["watch_user_num"]) == 2:
            watch_user_num = int(self["watch_user_num"][0].replace(',',''))  #可能网页中形式为1,033
            click_num = int(self["watch_user_num"][1].replace(',',''))
        else:
            watch_user_num = int(self["watch_user_num"][0])
            click_num = 0

        # watch_user_num = extract_num("".join(self["watch_user_num"]))
        # click_num = extract_num("".join(self["click_num"]))
        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)

        params = (zhihu_id, topics, url, title, content, answer_num, comments_num,
              watch_user_num, click_num, crawl_time)

        return insert_sql, params

    def save_to_es(self):
        question = QuestionType()
        question.zhihu_id = self["zhihu_id"][0]
        question.topics = ",".join(self["topics"])
        question.url = "".join(self["url"])
        question.title = "".join(self["title"])
        question.content = remove_tags("".join(self["content"]))
        question.answer_num = extract_num("".join(self["answer_num"]))
        question.comments_num = extract_num("".join(self["comments_num"]))
        if len(self["watch_user_num"]) == 2:
            question.watch_user_num = int(self["watch_user_num"][0].replace(',',''))  #可能网页中形式为1,033
            question.click_num = int(self["watch_user_num"][1].replace(',',''))
        else:
            question.watch_user_num = int(self["watch_user_num"][0])
            question.click_num = 0
        question.crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)

        question.suggest = gen_suggests(QuestionType._doc_type.index, ((question.title, 10), (question.topics, 7))) #搜索建议的保存

        question.save()

        return


class ZhihuAnswerItem(scrapy.Item):
    #知乎的问题回答Item
    zhihu_id = scrapy.Field()
    url = scrapy.Field()
    question_id = scrapy.Field()
    author_id = scrapy.Field()
    content = scrapy.Field()
    praise_num = scrapy.Field()
    comments_num = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        #插入知乎question表的知乎语句,注意后面的update语句表示answer入库时，可能回答更新后主键会有冲突，此时需要做处理，如果数据库中有，则更新，没有则插入；
        insert_sql = """
            insert into zhihu_answer(zhihu_id, url, question_id, author_id, content, praise_num, comments_num,
            create_time, update_time, crawl_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE content=VALUES(content), praise_num=VALUES(praise_num), comments_num=VALUES(comments_num),
            update_time=VALUES(update_time)
        """

        create_time = datetime.datetime.fromtimestamp(self["create_time"]).strftime(SQL_DATETIME_FORMAT) #将爬取到的int类型的create_time转换为datetime类型
        update_time = datetime.datetime.fromtimestamp(self["update_time"]).strftime(SQL_DATETIME_FORMAT)

        params = (
            self["zhihu_id"], self["url"], self["question_id"],
            self["author_id"], self["content"], self["praise_num"],
            self["comments_num"], create_time, update_time,
            self["crawl_time"].strftime(SQL_DATETIME_FORMAT)
        )

        return insert_sql, params

    def save_to_es(self):
        answer = AnswerType()
        answer.zhihu_id = self['zhihu_id']
        answer.url = self['url']
        answer.question_id = self['question_id']
        answer.author_id = self['author_id']
        answer.content = remove_tags(self['content'])
        answer.praise_num = self['praise_num']
        answer.comments_num = self['comments_num']
        answer.create_time = datetime.datetime.fromtimestamp(self["create_time"]).strftime(SQL_DATETIME_FORMAT)
        answer.update_time = datetime.datetime.fromtimestamp(self["update_time"]).strftime(SQL_DATETIME_FORMAT)
        answer.crawl_time = self["crawl_time"].strftime(SQL_DATETIME_FORMAT)

        answer.suggest = gen_suggests(AnswerType._doc_type.index, ((answer.zhihu_id, 10), (answer.url, 7))) #搜索建议的保存

        answer.save()

        return


def remove_splash(value):
    #去掉拉勾网工作城市的斜线(同样适用在工作年限、学历要求字段上)
    return value.replace("/","")


def handle_jobaddr(value):
    #处理拉勾网工作地址字段
    addr_list = value.split("\n") #去除换行符
    addr_list = [item.strip() for item in addr_list if item.strip()!="查看地图"] #列表每一项去除空格，除了“查看地图”
    return "".join(addr_list) #使用空字符串join起来


class LagouJobItemLoader(ItemLoader):
    # 自定义itemloader，即重载ItemLoader类
    default_output_processor = TakeFirst()


class LagouJobItem(scrapy.Item):
    #拉勾网职位信息
    title = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    salary = scrapy.Field()
    job_city = scrapy.Field(
        input_processor = MapCompose(remove_splash),
    )
    work_years = scrapy.Field(
        input_processor = MapCompose(remove_splash),
    )
    degree_need = scrapy.Field(
        input_processor = MapCompose(remove_splash),
    )
    job_type = scrapy.Field()
    publish_time = scrapy.Field()
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field()
    job_addr = scrapy.Field(
        input_processor = MapCompose(remove_tags, handle_jobaddr), #使用两个函数进行处理，第一个为scrapy自带的去除html标签函数
    )
    company_name = scrapy.Field()
    company_url = scrapy.Field()
    tags  = scrapy.Field(
        input_processor = Join(",")
    )
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into lagou_job(title, url, url_object_id, salary, job_city, work_years, degree_need,
            job_type, publish_time, job_advantage, job_desc, job_addr, company_name, company_url,
            tags, crawl_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE salary=VALUES(salary), job_desc=VALUES(job_desc)
        """
        params = (
            self["title"], self["url"], self["url_object_id"], self["salary"], self["job_city"],
            self["work_years"], self["degree_need"], self["job_type"],
            self["publish_time"], self["job_advantage"], self["job_desc"],
            self["job_addr"], self["company_name"], self["company_url"],
            self["tags"], self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
        )

        return insert_sql, params

    def save_to_es(self):
        job = JobType()  #实例化
        job.title = self['title']
        job.url = self['url']
        job.url_object_id = self['url_object_id']
        job.salary = self['salary']
        job.job_city = self['job_city']
        job.work_years = self['work_years']
        job.degree_need = self['degree_need']
        job.job_type = self['job_type']
        job.publish_time = self['publish_time']
        job.job_advantage = self['job_advantage']
        job.job_desc = remove_tags(self['job_desc'])
        job.job_addr = self['job_addr']
        job.company_name = self['company_name']
        job.company_url = self['company_url']
        job.tags = self['tags']
        job.crawl_time = self['crawl_time']

        job.suggest = gen_suggests(JobType._doc_type.index, ((job.title, 10), (job.tags, 7))) #搜索建议的保存

        job.save()

        return



