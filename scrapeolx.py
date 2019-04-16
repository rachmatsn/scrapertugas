# -*- coding: utf-8 -*-
#rachmat_sn

import scrapy
import sqlite3

count = 0
page_url = []

#SQLITE3
conn = sqlite3.connect('scrapeolx.db')
c = conn.cursor()

def create_table():
    c.execute("CREATE TABLE IF NOT EXISTS motorBekas(img TEXT, txt TEXT, brand TEXT,city TEXT, year TEXT, price TEXT)")
    c.execute("DELETE FROM motorBekas")

#SCRAPY
def textBeautify(data):
    return list(map(lambda s: s.strip(), data))

def textBeautifyBrand(data):
    return list(map(lambda s: s.strip()[14:], data))

def rupiahToNumber(rupiah):
    noRp = rupiah[3:]
    noDot = noRp.replace(".", "")
    integer = int(noDot)
    return integer

def generate_page_url():
    numofpage = 500
    for i in range(1,numofpage):
        if i==1:
            page_url.append('https://www.olx.co.id/motor/bekas/')
        else:
            page_url.append('https://www.olx.co.id/motor/bekas/?page='+str(i))
    return page_url
        
class ScrapeolxSpider(scrapy.Spider):
    name = 'scrapeolx'
    create_table()
    page_url = generate_page_url()
    #print(page_url)
    
    def start_requests(self):
        for url in page_url:
            #print(url)
            yield scrapy.Request(url=url, callback=self.parse)
    
    def parse(self, response):
        print('test')
        img = textBeautify(response.css('td.offer>table>tbody>tr>td>span>a>img.fleft::attr(src)').extract())
        txt = textBeautify(response.css('td.offer>table>tbody>tr>td>h3>a>span::text').extract())
        
        #brand################################
        BR = response.css('td.offer>table>tbody>tr>td>p>small.breadcrumb::text').extract() #brand
        brand = []
        for i in range (0, len(BR),2):
            brand.append(BR[i])
        brand = textBeautifyBrand(brand)
        
        city = textBeautify(response.css('td.offer>table>tbody>tr>td>p>small.breadcrumb>span::text').extract())
        year = textBeautify(response.css('td.offer>table>tbody>tr>td>div>div.year::text').extract())
        price = textBeautify(response.css('td.offer>table>tbody>tr>td>div>p.price>strong::text').extract())
        
        for item in zip(img,txt,brand,city,year,price):
            scraped_info = {
                'img': item[0],
                'txt': item[1],
                'brand': item[2],
                'city': item[3],
                'year': item[4],
                'price': rupiahToNumber(item[5]), 
            }
            c.execute("INSERT INTO motorBekas (img, txt, brand, city, year, price) VALUES(?,?,?,?,?,?)",
                      (scraped_info['img'], scraped_info['txt'], scraped_info['brand'], scraped_info['city'], scraped_info['year'], scraped_info['price']))
            conn.commit()
            yield scraped_info
        
        
        
        
