# coding:utf-8
#!/usr/bin/env python

import signal
from multiprocessing import Pool, Event
import os
import sys
import time
import json
import sqlite3
import concurrent.futures

from google.cloud import datastore
import google.cloud.exceptions


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "./credential.json"
GCP_PROJECT = "your_gcp_project_id"

STOP_FLAG = Event()


class DatastoreService:
    def __init__(self):
        self.client = datastore.Client(GCP_PROJECT)

    # for AfterConvert
    def from_datastore(self, entity):
        if not entity:
            return None
        if isinstance(entity, list):
            entity = entity.pop()

        entity['id'] = entity.key.id
        return entity

    def get_acDictionary(self, cursor=None):
        query = self.client.query(kind='test', order=['f'])
        query_iterator = query.fetch(limit=None, start_cursor=cursor)
        page = next(query_iterator.pages)

        entities = list(map(self.from_datastore, page))
        next_cursor = (
            query_iterator.next_page_token.decode('utf-8')
            if query_iterator.next_page_token else None)

        return entities, next_cursor

    def set_datas(self, fid, tid):
        conn = sqlite3.connect("./wnjpn.db")
        try:
            for i in range(fid, tid):
                if STOP_FLAG.is_set():
                    print("STOP")
                    return
                cur = conn.execute(
                    "select * from word where lang = 'jpn' ORDER BY RANDOM() LIMIT 2;")
                wordlist = [record[2] for record in cur.fetchall()]
                key = self.client.key('test', i)
                entity = datastore.Entity(key=key)
                entity.update({
                    'f': wordlist[0],
                    't': wordlist[1],
                })
                self.client.put(entity)
                # Then get by key for this entity
                result = self.client.get(key)
                print(result)
        except:
            print("skip : " + str(i))
            # import traceback
            # traceback.print_exc()
        finally:
            time.sleep(0.1)


def set_datas(fid):
    print("co-exec : " + str(fid))
    datastore_service = DatastoreService()
    datastore_service.set_datas((fid*10)+1, (fid+1)*10)


if __name__ == '__main__':
    datastore_service = DatastoreService()

    # input datas

    def signalHandler(signal, handler):
        STOP_FLAG.set()
    signal.signal(signal.SIGINT,  signalHandler)
    signal.signal(signal.SIGTERM, signalHandler)

    process_pool = Pool(processes=4)
    process_pool.map(set_datas, range(0, 100))
    process_pool.close()
    process_pool.join()

    print("execute")
    if STOP_FLAG.is_set():
        exit()

    # export datas
    aftcnv_dict = {}
    cur = bytes()
    dicpg, cur = datastore_service.get_acDictionary()
    for w in dicpg:
        # print(w["f"] + "=>" + w["t"])
        aftcnv_dict[w["f"]] = w["t"]
    print(" loaded : " + str(len(aftcnv_dict)))
    while (not cur is None):
        dicpg, cur = datastore_service.get_acDictionary(cur.encode("ascii"))
        for w in dicpg:
            # print(w["f"] + "=>" + w["t"])
            aftcnv_dict[w["f"]] = w["t"]
        print(" loaded : " + str(len(aftcnv_dict)))
