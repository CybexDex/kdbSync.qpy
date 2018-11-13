from pymongo import MongoClient
import json
from bson import json_util
from bson.objectid import ObjectId
import config
from flatten_json import flatten
from collections import OrderedDict
import q, logging, time


logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log.log',
                filemode='w')
def Q_connection(sql = None):
    qconn = q.q(host = 'localhost', port = config.Q_port , user = 'sunqi')
    # qconn.k('\\l /home/sunqi/mudb/test1/test.q')
    logging.info("Loading " + config.Q_Script + "...")
    res = qconn.k('\\a')
    print list(res)
    res = qconn.k('\\v')
    print list(res)
    if sql != None:
        res = qconn.k(sql)
    else:
        res = []
    qconn.close()
    return res
def Mongo_connection(blk_num = 70000):
    client = MongoClient(config.MONGODB_DB_URL)
    db = client[config.MONGODB_DB_NAME]
    j = list(db.account_history.find({'bulk.block_data.block_num': blk_num}))
    print j

def get_lib():
    import os
    try:
        res = int(os.popen('/usr/bin/python3 bts_info.py').read())
    except:
        logging.error('lib script failed')
        exit(1)
    return res
def get_mongo_lib(db):
    init_size_limit_json = list(db.account_history.find().sort([("_id",-1)]).limit(1))[0]
    init_size_limit = init_size_limit_json['bulk']['block_data']['block_num']
    return init_size_limit

if __name__ == '__main__':
    print '----------------------------- Q_connection -----------------------------'
    Q_connection()
    print '----------------------------- Mongo_connection -----------------------------'
    Mongo_connection()
    
