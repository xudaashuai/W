# -*- coding: utf-8 -*-
from scrapy import *
from scrapy.exceptions import CloseSpider
import requests, json, time, logging, threading, psycopg2

status_url = 'https://api.weibo.com/2/statuses/user_timeline.json?' \
             'access_token=2.00rW3AhGfj3PXC4017a48ee5G6kHpD&uid={0}&count=200&max_id={1}&trim_user=1'
conn = psycopg2.connect("dbname=postgres user=postgres port=5439")

attrs = [
    'uid', 'id', 'created_at', 'text',  'reposts_count', 'attitudes_count', 'comments_count',
]



count = [0, 0, 0]

def print_count():
    while True:
        time.sleep(10)
        logging.info("user : %d  statuses: %d error %d" % tuple(count))


def get_tuple(j):
    if 'deleted' in j and j['deleted'] == '1':
        return
    for x in attrs:
        try:
            if type(j[x]) is list:
                yield ' '.join(j[x])
            else:
                yield j[x]
        except:
            print j
    if 'retweeted_status' in j:
        yield j['retweeted_status']['id']
    else:
        yield None
    if j['geo'] and 'coordinates' in j['geo']:
        yield ' '.join([str(x) for x in j['geo']['coordinates']])
    else:
        yield None
    pic_url = []
    for x in j['pic_urls']:
        pic_url.append(x['thumbnail_pic'])
    yield ' '.join(pic_url)


def add_status(status, id):
    t = tuple(x for x in get_tuple(status))
    if t[1]<3793991818200000:
        finish_id(t[1],id)
        return True
    conn.cursor().execute(" insert into weibo.status "
                          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict do nothing", t)
    conn.commit()
    count[1]+=1
    return False
def finish_id(last_id,id):
    conn.cursor().execute('update weibo.id_crawl set crawl=TRUE where id=%s', (id,))
    conn.cursor().execute( 'update weibo.id_crawl set last_id=%s where id=%s', (last_id, id,))
    conn.commit()
    count[0] += 1
class WSpider(Spider):
    name = 'WW'
    i = 0

    def start_requests(self):
        threading._start_new_thread(print_count, tuple())
        f = open('ali.txt', 'r')
        uid_s = f.readlines()
        uids=[]
        for x in uid_s:
            uids.append(int(x))
        cur=conn.cursor()
        cur.execute('select * from weibo.id_crawl')
        logging.info('start finish last work')
        k=set()

        for t in cur.fetchall():
            uid=t[0]
            k.add(uid)
                #uids.remove(uid)
                #yield Request(status_url.format(uid, 0), self.parse_status, meta={'id': uid})
                #    yield Request(status_url.format(t[0], t[2]), self.parse_status, meta={'id': t[0]})
        for uid in uids:
            if uid not in k:
                conn.cursor().execute(" insert into weibo.id_crawl "
                                  "values(%s,%s,%s) on conflict do nothing", (uid, False, 0))
                conn.commit()
                yield Request(status_url.format(uid, 0), self.parse_status, meta={'id': uid})

    def parse_status(self, response):
        if response.status!=200:
            return
        try:
            j = json.loads(response.body)
        except:
            count[2]+=1
            print response.status
            return
        if 'error' in j:
            count[2] += 1
            print j
            return
        meta = response.meta
        for status in j['statuses']:
            if add_status(status, meta['id']):
                return
        if j['statuses'].__len__() != 0:
            conn.cursor().execute('update weibo.id_crawl set last_id=%s where id=%s',
                                  (j['statuses'][-1]['id'], meta['id'],))
            conn.commit()
            yield Request(status_url.format(meta['id'], j['statuses'][-1]['id'] - 1), self.parse_status, meta=meta)
        else:
            conn.cursor().execute('update weibo.id_crawl set crawl=TRUE where id=%s', (meta['id'],))
            count[0]+=1
            conn.commit()
