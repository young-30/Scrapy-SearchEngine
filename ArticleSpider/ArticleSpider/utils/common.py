import hashlib
import re


def get_md5(url):
    if isinstance(url, str):     #判断传入参数的编码方式
        url = url.encode("utf-8")

    m = hashlib.md5()   #实例化对象
    m.update(url)       #调用update
    return m.hexdigest()  #抽取摘要


def extract_num(text):
    #从字符串中提取数字
    match_re = re.match(".*?(\d+).*", text)
    if match_re:
        nums = int(match_re.group(1))
    else:
        nums = 0

    return nums


if __name__ == "__main__":
    print(get_md5("htt://jobbole.com"))