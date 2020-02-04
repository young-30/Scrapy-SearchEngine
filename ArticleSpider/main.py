
from scrapy.cmdline import execute

import sys
import os

# print(os.path.abspath(__file__)) #获取脚本完整路径，以文件名结尾
# print(os.path.dirname(__file__)) #获取脚本路径
# print(os.path.dirname(os.path.abspath(__file__))) #获取工程目录
# print(os.path.abspath(os.path.dirname(__file__))) #获取工程目录

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
print(os.path.dirname(os.path.abspath(__file__)))
execute(["scrapy","crawl","jobbole"])
# execute(["scrapy","crawl","zhihu_sel"])
# execute(["scrapy","crawl","zhihu_sel"])
# execute(["scrapy","crawl","lagou"])


