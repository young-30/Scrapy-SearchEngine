# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import codecs #代替open函数，避免很多编码问题
import json   #使用json处理爬取的数据
import MySQLdb #引入mysql模块
import MySQLdb.cursors #引入cursor，twisted异步处理时使用

from scrapy.pipelines.images import ImagesPipeline
from scrapy.exporters import JsonItemExporter #使用scrapy提供的json方法导出json文件
from twisted.enterprise import  adbapi #将mysqldb转换为异步化的操作
from models.es_types import ArticleType #引入es操作的类(jobbole)
from w3lib.html import remove_tags #处理content字段，可以去除html标签

class ArticlespiderPipeline(object):    #系统自动生成
    def process_item(self, item, spider):
        return item

class JsonWithEncodingPipeline(object): #使用json存储数据(自定义)
    def __init__(self):
        self.file = codecs.open('article.json','w',encoding="utf8")  #以写操作打开文件

    def process_item(self, item, spider):
        lines = json.dumps(dict(item),ensure_ascii=False) + '\n' #dumps函数将字典转换为字符串
        self.file.write(lines)
        return item

    def spider_closed(self, spider):  #信号量，当spider关闭时该函数会被调用
        self.file.close()

class JsonExporterPipeline(object):
    #调用scrapy提供的json export导出json文件
    def __init__(self):
        self.file = open('articleexport.json', 'wb') #以写二进制方式打开文件
        self.exporter = JsonItemExporter(self.file, encoding="uft-8", ensure_ascii=False) #使用JsonItemExporter实例化
        self.exporter.start_exporting()

    def close_spider(self, spider):  #关闭文件时会调用
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider): #处理item
        self.exporter.export_item(item)
        return item

class MysqlPipeline(object):
    # 采用同步的机制写入mysql
    def __init__(self):
        self.conn = MySQLdb.connect('localhost','root','root','article_spider',charset="utf8",use_unicode=True )
                    #MySQLdb.connect('host','user','password','dbname',charset="utf8",use_unicode=True )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):  #数据处理函数
        insert_sql ="""
            insert into jobbole_article(url_object_id, title, url, create_date, fav_nums)
            VALUES (%s, %s, %s, %s, %s)
            """
        self.cursor.execute(insert_sql, (item["url_object_id"], item["title"], item["url"],item["create_date"],item["fav_nums"]))
        self.conn.commit()  #调用execute以后需要调用commit()提交

class MysqlTwistedPipeline(object):
    #采用异步的机制写入mysql
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings): #从setting文件中读取值,该方法会自动被scrapy调用
        dbparms = dict(
            host = settings["MYSQL_HOST"],
            db = settings["MYSQL_DBNAME"],
            user = settings["MYSQL_USER"],
            passwd = settings["MYSQL_PASSWORD"],
            charset = 'utf8',
            cursorclass = MySQLdb.cursors.DictCursor,
            use_unicode = True
        )    #该dict参数名称为固定写法，要与connection方法里的名称一样

        dbpool = adbapi.ConnectionPool("MySQLdb", **dbparms)  #连接词实现异步,传入的dbparms表示是可变化的参数，就是上面的dict
        return cls(dbpool)  #调用的class就是MysqlTwistedPipeline，此处其实就是实例化

    def process_item(self, item, spider):
        #使用twisted将mysql插入变成异步执行
        query = self.dbpool.runInteraction(self.do_insert, item) #会把do_insert变为异步操作
        query.addErrback(self.handle_error, item, spider) #处理异常,这里传入item和spider,调试时方便查看

    def handle_error(self, failure, item, spider):
        #处理异步插入的异常
        print(failure)    #插入失败时找到原因的根本入口

    def do_insert(self, cursor, item):
        #执行具体的插入(拷贝上面的代码，但是要修改cursor，也不需要再commit了)
           #根据不同的item，构建不同的sql语句并插入到mysql中

        # if item.__class__.__name__ == "JobBoleArticleItem":
        #     insert_sql = """
        #                 insert into jobbole_article(url_object_id, title, url, create_date, fav_nums)
        #                 VALUES (%s, %s, %s, %s, %s)
        #                 """
        #     cursor.execute(insert_sql,(item["url_object_id"], item["title"], item["url"], item["create_date"], item["fav_nums"]))

        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)





class ArticleImagePipeline(ImagesPipeline):     #保存图片(ImagePipeline)并存储图片路径
    def item_completed(self, results, item, info): #results是列表，里面每个元素为元组，元组里包括一个flag和一个字典
        if "front_image_path" in item:   #有封面图片才调用，如爬取知乎网站则可能没有封面图片
            for ok, value in results:
                image_file_path = value["path"]
            item["front_image_path"] = image_file_path

        return item


class ElasticsearchPipeline(object):
    #将数据写入到es中

    def process_item(self, item, spider):
        #将item转换为es的数据
        item.save_to_es()  #item文件中的方法

        return item