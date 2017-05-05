# -*- coding: utf-8 -*-
from scrapy import *
import requests, json, time, logging, threading

ids_url = 'https://api.weibo.com/2/statuses/user_timeline/ids.json?access_token=2.001zVo5G06XASO957ca448010wpKB8&count=100&uid={0}&page={1}'
info_url = 'https://api.weibo.com/2/users/show.json?access_token=2.001zVo5G06XASO957ca448010wpKB8&uid={0}'
follow_url = 'https://api.weibo.com/2/friendships/friends/ids.json?access_token=2.001zVo5G06XASO957ca448010wpKB8&uid={0}&count=5&cursor={1}'
fans_url = 'https://api.weibo.com/2/friendships/followers/ids.json?access_token=2.001zVo5G06XASO957ca448010wpKB8&uid={0}&count=5&cursor={1}'
f = open('ids.txt', 'w+')
uids = set()


class WSpider(Spider):
    name = 'S'
    count = 0

    def print_count(self):
        while (True):
            time.sleep(10)
            logging.info(self.count)

    def start_requests(self):
        threading._start_new_thread(self.print_count, tuple())
        yield Request(info_url.format(5863468422), self.parse_info)

    def parse_info(self, response):
        j = json.loads(response.body)
        if 'location' in j and j['location'] == u"湖北 武汉":
            self.count += 1
            f.write(j['idstr'] + '\n')
            f.flush()
            yield Request(follow_url.format(j['id'], 0), self.parse_follow, meta={'id': j['id']})
            yield Request(fans_url.format(j['id'], 0), self.parse_fans, meta={'id': j['id']})

    def parse_follow(self, response):
        j = json.loads(response.body)
        for uid in j['ids']:
            if uid not in uids:
                uids.add(uid)
                yield Request(info_url.format(uid), self.parse_info)
        if j['next_cursor'] is not 0:
            yield Request(follow_url.format(response.meta['id'], j['next_cursor']), self.parse_follow,
                          meta=response.meta)

    def parse_fans(self, response):
        j = json.loads(response.body)
        for uid in j['ids']:
            if uid not in uids:
                uids.add(uid)
                yield Request(info_url.format(uid), self.parse_info)
        if j['next_cursor'] is not 0:
            yield Request(fans_url.format(response.meta['id'], j['next_cursor']), self.parse_fans, meta=response.meta)
