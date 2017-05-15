# -*- coding: utf-8 -*-
from scrapy import *
from scrapy.exceptions import CloseSpider
import requests, json, time, logging, threading, psycopg2

status_url = 'https://api.weibo.com/2/statuses/user_timeline.json?' \
             'access_token=2.001zVo5G06XASO957ca448010wpKB8&uid={0}&count=200&max_id={1}&trim_user=1'
conn = psycopg2.connect("dbname=postgres user=postgres port=5439")


class WSpider(Spider):
    name = 'WW'
    i = 0
    count = [0, 0, 0]
    max = 0

    def print_count(self):
        while True:
            time.sleep(10)
            logging.info("user : %d  statuses: %d error %d" % tuple(self.count))
            logging.info('max : %d' % self.max)

    def start_requests(self):
        #threading._start_new_thread(self.print_count, tuple())
        f = open('weibo_user_id.txt', 'r')
        uids = f.readlines()
        f = open('cur.txt', 'r+')
        start = int(f.read())
        f.close()
        for i in range(start, uids.__len__()):
            uid = int(uids[i])
            yield Request(status_url.format(uid, 1), self.parse_status, meta={'id': uid})

    def handle_403(self):
        f = open('cur.txt', 'w+')
        f.write(str(self.i))
        f.flush()
        f.close()
        raise CloseSpider('403 happen')

    def parse_status(self, response):
        if response.status is 403:
            self.handle_403()
            return
        j = json.loads(response.body)
        if 'error' in j:
            print response.meta, j['error']
            self.count[2] += 1
            return
        meta = response.meta
        for status in j['statuses']:
            conn.cursor().execute(" insert into weibo.statuses "
                                  "values(%s,%s,%s) on conflict do nothing",
                                  (status['id'], meta['id'], str(status)))
            conn.commit()

        if j['statuses'].__len__() != 0:
            self.count[1] += j['statuses'].__len__()
            yield Request(status_url.format(meta['id'], j['statuses'][-1]['id'] - 1), self.parse_status, meta=meta)
        else:
            self.count[0] += 1
