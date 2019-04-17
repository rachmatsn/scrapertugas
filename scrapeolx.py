# -*- coding: utf-8 -*-
#rachmat_sn
# scrapy runspider scrapeolx.py

import scrapy
import sqlite3

count = 0
page_url = []

#SQLITE3
dbname = 'DBscraper.db'
conn = sqlite3.connect(dbname)
c = conn.cursor()

def create_table():
    c.execute("CREATE TABLE IF NOT EXISTS motorBekas(ad_id TEXT, img TEXT, txt TEXT, brand TEXT,city TEXT, year TEXT, price INTEGER, UNIQUE(ad_id))")
    c.execute("DELETE FROM motorBekas")

#SCRAPY
def textBeautify(data):
    return list(map(lambda s: s.strip(), data))

def textBeautifyBrand(data):
    return list(map(lambda s: s.strip()[14:], data))

def rupiahToNumber(rupiah):
    noRp = rupiah[3:]
    noDot = noRp.replace(".", "")
    if noDot == '':
        return ''
    else:
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
            yield scrapy.Request(url=url, callback=self.parse)
    
    def parse(self, response):        
        print('test')
        ad_id = textBeautify(response.css('td.offer>table>tbody>tr::attr(data-ad-id)').extract())
        img = textBeautify(response.css('td.offer>table>tbody>tr>td>span>a>img.fleft::attr(src)').extract())
        txt = textBeautify(response.css('td.offer>table>tbody>tr>td>h2>a::text').extract())
        
        #brand################################
        BR = response.css('td.offer>table>tbody>tr>td>p>small.breadcrumb::text').extract() #brand
        brand = []
        for i in range (0, len(BR),2):
            brand.append(BR[i])
        brand = textBeautifyBrand(brand)
        
        city = textBeautify(response.css('td.offer>table>tbody>tr>td>p>small.breadcrumb>span::text').extract())
        year = textBeautify(response.css('td.offer>table>tbody>tr>td>div>div.year::text').extract())
        price = textBeautify(response.css('td.offer>table>tbody>tr>td>div>p.price>strong::text').extract())
        
        #cari yg panjangnya paling kecil untuk acuan
        jum_data_per_iter = min([len(img), len(txt), len(brand), len(city), len(year), len(price)])
        for it in range(jum_data_per_iter):
            scraped_info = {
                'ad_id': ad_id[it],
                'img': img[it],
                'txt': txt[it],
                'brand': brand[it],
                'city': city[it],
                'year': year[it],
                'price': rupiahToNumber(price[it]), 
            }
            
            #commit jika semua data tidak kosong '' DAN brand bukan 'Lain-lain'
            if(scraped_info['ad_id']!='' and scraped_info['img']!='' and scraped_info['img']!='' and scraped_info['brand']!='' and scraped_info['city']!='' and scraped_info['year']!='' and scraped_info['price']!='' and scraped_info['brand']!='Lain-lain'):
                c.execute("INSERT OR IGNORE INTO motorBekas (ad_id, img, txt, brand, city, year, price) VALUES(?,?,?,?,?,?,?)",
                      (scraped_info['ad_id'],scraped_info['img'], scraped_info['txt'], scraped_info['brand'], scraped_info['city'], scraped_info['year'], scraped_info['price']))
                conn.commit()
                
            yield scraped_info