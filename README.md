# Scrapy-SearchEngine
A vertical search engine integrated by `Scrapy`/`ElasticSearch`/`Django`
## Available function
* Crawl Cnlogs, Zhihu website and store in  MySQL & elasticsearch
* Full text search, search suggestions, search records, highlighted search terms and marked red
* Redis's real-time all station crawled number display(Under construction)
## Directory Structure
    ArticleSpider: The spider source  
    LcvSearch： SearchEngine system source  
    chrome60: Driver for Selenium  
## Requirements
* Python 3.5+
* Scrapy 2.0.0
* Django 2.1.5
* JDK 8+
* elasticsearch-rtf
## Usage
    git clone https://github.com/young-30/Scrapy-SearchEngine
    
    cd Scrapy-SearchEngine
    pip install -r requires.txt
    
    scrapy crawl cnblogs
    scrapy crawl zhihu
    scrapy crawl lagou


