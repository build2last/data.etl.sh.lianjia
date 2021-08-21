# -*- coding: utf-8 -*-
import time
from datetime import datetime

import scrapy
from scrapy import Selector
from scrapy import Spider
import json
import re
from house_spider.items import LianjiaVillageItem, LianjiaHouseItem

class LianjiaSpider(Spider):
    name = 'lianjia'
    allowed_domains = ['sh.lianjia.com']
    start_urls = ['https://sh.lianjia.com/xiaoqu/?from=rec']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.count = 0
        self.base_url = 'https://sh.lianjia.com' #上海链家网址
        self.root_request_url = 'https://sh.lianjia.com/xiaoqu/?from=rec'

    def start_requests(self):
        yield scrapy.Request(url=self.root_request_url, callback=self.parse_district_links)

    def parse(self, response):
        yield scrapy.Request(url=self.root_request_url, callback=self.parse_district_links)

    def parse_district_links(self, response):
        """提取地区链接"""
        sel = Selector(response)
        links = sel.css("div[data-role='ershoufang'] div:first-child a::attr(href)").extract()
        for link in links:
            url = self.base_url + link
            yield scrapy.Request(url=url, callback=self.parse_bizcircle_links)

    def parse_bizcircle_links(self, response):
        """提取商圈链接"""
        sel = Selector(response)
        links = sel.css("div[data-role='ershoufang'] div:nth-child(2) a::attr(href)").extract()
        for link in links:
            url = self.base_url + link
            yield scrapy.Request(url=url, callback=self.parse_village_list, meta={"ref": url})

    def parse_village_list(self, response):
        """提取小区链接"""
        sel = Selector(response)
        links = sel.css(".listContent .xiaoquListItem .img::attr(href)").extract()
        for link in links:
            yield scrapy.Request(url=link, callback=self.parse_village_detail)

        # page
        page_data = sel.css(".house-lst-page-box::attr(page-data)").extract_first()
        page_data = json.loads(page_data)
        if page_data['curPage'] < page_data['totalPage']:
            url = response.meta["ref"] + 'pg' + str(page_data['curPage'] + 1)
            yield scrapy.Request(url=url, callback=self.parse_village_list, meta=response.meta)

    def parse_village_detail(self, response):
        """提取小区详情"""
        village_url = response.url
        sel = Selector(response)
        zone = sel.css('.xiaoquDetailbreadCrumbs .l-txt a::text').extract()
        latitude = 0
        longitude = 0
        try:
            html = response.body.decode().replace('\r', '')
            local = html[html.find('resblockPosition:'):html.find('resblockName') - 1]
            m = re.search('(\d.*\d),(\d.*\d)', local)
            longitude = m.group(1)
            latitude = m.group(2)
        except Exception:
            pass

        item = LianjiaVillageItem()
        item['id'] = village_url.replace(self.base_url + '/xiaoqu/', '').replace('/', '')
        item['name'] = sel.css('.detailHeader .detailTitle::text').extract_first()
        item['address'] = sel.css('.detailHeader .detailDesc::text').extract_first()
        item['latitude'] = latitude
        item['longitude'] = longitude
        item['zone'] = ','.join(zone)
        item['year'] = sel.css('.xiaoquInfo .xiaoquInfoItem:nth-child(1) .xiaoquInfoContent::text').extract_first()
        item['build_type'] = sel.css('.xiaoquInfo .xiaoquInfoItem:nth-child(2) .xiaoquInfoContent::text').extract_first()
        item['property_costs'] = sel.css('.xiaoquInfo .xiaoquInfoItem:nth-child(3) .xiaoquInfoContent::text').extract_first()
        item['property_company'] = sel.css('.xiaoquInfo .xiaoquInfoItem:nth-child(4) .xiaoquInfoContent::text').extract_first()
        item['developers'] = sel.css('.xiaoquInfo .xiaoquInfoItem:nth-child(5) .xiaoquInfoContent::text').extract_first()
        item['buildings'] = sel.css('.xiaoquInfo .xiaoquInfoItem:nth-child(6) .xiaoquInfoContent::text').extract_first()
        item['total_house'] = sel.css('.xiaoquInfo .xiaoquInfoItem:nth-child(7) .xiaoquInfoContent::text').extract_first()
        item['采集时间'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        print(item['name'])
        yield item

        # 小区房源 https://cq.lianjia.com/ershoufang/c3620038190566370/
        url = self.base_url + "/ershoufang/c" + item['id'] + "/"
        yield scrapy.Request(url=url, callback=self.parse_house_list, meta={"ref": url})
        # 成交房源
        url = self.base_url + "/chengjiao/c" + item['id'] + "/"
        yield scrapy.Request(url=url, callback=self.parse_chouse_list, meta={"ref": url})

    def parse_house_list(self, response):
        """提取房源链接"""
        sel = Selector(response)
        # 链家有时小区查询不到数据
        total = sel.css('.resultDes .total span::text').extract_first()
        total = int(total)
        if total > 0:
            # 提取房源链接
            links = sel.css(".sellListContent li .info .title a::attr(href)").extract()
            for link in links:
                yield scrapy.Request(url=link, callback=self.parse_house_detail)
            # 链接分页
            page_data = sel.css(".house-lst-page-box::attr(page-data)").extract_first()
            page_data = json.loads(page_data)
            if page_data['curPage'] == 1 and page_data['totalPage'] > 1:
                price = response.url.replace(self.base_url + '/ershoufang/', '  ')
                for x in range(2, page_data['totalPage'] + 1, 1):
                    url = self.base_url + '/ershoufang/' + 'pg' + str(x) + price
                    yield scrapy.Request(url=url, callback=self.parse_house_list)

    def parse_house_detail(self, response):
        """提取房源信息"""
        sel = Selector(response)

        item = LianjiaHouseItem()
        item['房屋Id'] = response.url.replace(self.base_url + '/ershoufang/', '').replace('.html', '')
        item['标题'] = sel.css('.title-wrapper .title .main::text').extract_first()
        item['售价'] = sel.css('.overview .content .price .total::text').extract_first()
        item['小区'] = sel.css('.overview .content .aroundInfo .communityName a.info::text').extract_first()
        item['小区ID'] = sel.css('.overview .content .aroundInfo .communityName a.info::attr(href)').extract_first().replace('/xiaoqu/', '').replace('/', '')
        item['房屋户型'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="房屋户型"]/text()').extract_first()
        item['所在楼层'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="所在楼层"]/text()').extract_first()
        item['建筑面积'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="建筑面积"]/text()').extract_first()
        item['户型结构'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="户型结构"]/text()').extract_first()
        item['套内面积'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="套内面积"]/text()').extract_first()
        item['建筑类型'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="建筑类型"]/text()').extract_first()
        item['房屋朝向'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="房屋朝向"]/text()').extract_first()
        item['建成年代'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="建成年代"]/text()').extract_first()
        item['装修情况'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="装修情况"]/text()').extract_first()
        item['建筑结构'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="建筑结构"]/text()').extract_first()
        item['供暖方式'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="供暖方式"]/text()').extract_first()
        item['梯户比例'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="梯户比例"]/text()').extract_first()
        item['配备电梯'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="配备电梯"]/text()').extract_first()
        item['挂牌时间'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="挂牌时间"]/text()').extract_first()
        item['交易权属'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="交易权属"]/text()').extract_first()
        item['上次交易'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="上次交易"]/text()').extract_first()
        item['房屋用途'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="房屋用途"]/text()').extract_first()
        item['房屋年限'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="房屋年限"]/text()').extract_first()
        item['产权所属'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="产权所属"]/text()').extract_first()
        item['抵押信息'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="抵押信息"]/text()').extract_first()
        item['房源核验码'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="房源核验码"]/text()').extract_first()
        item['房本备件'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="房本备件"]/text()').extract_first()
        item['链家编号'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="链家编号"]/text()').extract_first()
        item['关注人数'] = sel.css('#favCount::text').extract_first()
        item['状态'] = '在售'
        item['采集时间'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        self.count += 1
        print("number:" + str(self.count) + ":" + item['小区'] + ":" + response.url)
        yield item

    def parse_chouse_list(self, response):
        """提取成交房源链接"""
        sel = Selector(response)
        # 链家有时小区查询不到数据
        total = sel.css('.resultDes .total span::text').extract_first()
        total = int(total)
        if total > 0:
            # 提取房源链接
            links = sel.css(".listContent li .info .title a::attr(href)").extract()
            for link in links:
                yield scrapy.Request(url=link, callback=self.parse_chouse_detail)
            # 链接分页
            page_data = sel.css(".house-lst-page-box::attr(page-data)").extract_first()
            page_data = json.loads(page_data)
            if page_data['curPage'] == 1 and page_data['totalPage'] > 1:
                price = response.url.replace(self.base_url + '/chengjiao/', '')
                for x in range(2, page_data['totalPage'] + 1, 1):
                    url = self.base_url + '/chengjiao/' + 'pg' + str(x) + price
                    yield scrapy.Request(url=url, callback=self.parse_chouse_list)

    def parse_chouse_detail(self, response):
        """提取成交房源信息"""
        sel = Selector(response)
        house_id = response.url.replace(self.base_url + '/chengjiao/', '').replace('.html', '')
        item = LianjiaHouseItem()
        item['房屋Id'] = house_id
        item['售价'] = sel.css('.wrapper .overview .info.fr .msg span:nth-child(1) label::text').extract_first()
        item['成交价'] = sel.css('.wrapper .overview .info.fr .price .dealTotalPrice i::text').extract_first()
        item['标题'] = sel.css('.house-title .wrapper::text').extract_first()
        item['小区'] = sel.css('.wrapper .deal-bread a:nth-child(9)::text').extract_first().replace('二手房成交', '')
        item['小区ID'] = sel.css('.house-title::attr(data-lj_action_housedel_id)').extract_first()
        item['房屋户型'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="房屋户型"]/text()').extract_first()
        item['所在楼层'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="所在楼层"]/text()').extract_first()
        item['建筑面积'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="建筑面积"]/text()').extract_first()
        item['户型结构'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="户型结构"]/text()').extract_first()
        item['套内面积'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="套内面积"]/text()').extract_first()
        item['建筑类型'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="建筑类型"]/text()').extract_first()
        item['房屋朝向'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="房屋朝向"]/text()').extract_first()
        item['建成年代'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="建成年代"]/text()').extract_first()
        item['装修情况'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="装修情况"]/text()').extract_first()
        item['建筑结构'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="建筑结构"]/text()').extract_first()
        item['供暖方式'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="供暖方式"]/text()').extract_first()
        item['梯户比例'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="梯户比例"]/text()').extract_first()
        item['配备电梯'] = sel.xpath('//div[@class="base"]/div[@class="content"]/ul/li[span="配备电梯"]/text()').extract_first()
        item['挂牌时间'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="挂牌时间"]/text()').extract_first()
        item['交易权属'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="交易权属"]/text()').extract_first()
        item['上次交易'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="上次交易"]/text()').extract_first()
        item['房屋用途'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="房屋用途"]/text()').extract_first()
        item['房屋年限'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="房屋年限"]/text()').extract_first()
        item['产权所属'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="产权所属"]/text()').extract_first()
        item['房权所属'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="房权所属"]/text()').extract_first()
        item['抵押信息'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="抵押信息"]/text()').extract_first()
        item['房源核验码'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="房源核验码"]/text()').extract_first()
        item['房本备件'] = sel.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li[span="房本备件"]/text()').extract_first()
        item['状态'] = '成交'
        item['成交时间'] = datetime.strptime(sel.css('.house-title div span::text').extract_first().replace(' 成交', ''), '%Y.%m.%d').strftime('%Y-%m-%d')
        item['采集时间'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        yield item
