from pymongo import MongoClient
import json
from bson import json_util
from bson.objectid import ObjectId
import config
from flatten_json import flatten
from collections import OrderedDict
import q, logging, time
# from bitshares import BitShares


logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log.log',
                filemode='w')

qconn = q.q(host = 'localhost', port = config.Q_port , user = 'sunqi')
# qconn.k('\\l /home/sunqi/mudb/test1/test.q')
logging.info("Loading " + config.Q_Script + "...")
qconn.k('\\l ' + config.Q_Script )

client = MongoClient(config.MONGODB_DB_URL)
db = client[config.MONGODB_DB_NAME]

def conn_reset(qconn):
    op4 = 'op4' 
    try:
        qconn.k('tbname:`%s ;tbupdate[%s]'%(op4,op4))
        qconn.k('mvcsv[`op4]')
        qconn.k('op4back:op4;delete %s from `.' % (op4))
    except:
        logging.error( op4 + ' not found!')
    qconn.k('\\l ' + config.Q_Script)



def get_lib():
    import os
    try:
        res = int(os.popen('/usr/bin/python3 ../bts_info.py').read())
    except:
        logging.error('lib script failed')
        exit(1)
    return res

# start_blk = config.START_BLK
start_blk = 450 * 24
blk_circle = start_blk
def get_mongo_lib():
    init_size_limit_json = list(db.account_history.find().sort([("_id",-1)]).limit(1))[0]
    init_size_limit = init_size_limit_json['bulk']['block_data']['block_num']
    return init_size_limit
# logging.info("=========== query blocks till -> "+ str(init_size_limit) +"===================\n")
def get_start(last):
    return last - blk_circle
lib = get_lib()
blk_num = get_start(lib)

while 1:
    if blk_num >= lib - 1:
        time.sleep(2)
        lib = get_lib()
        continue
    logging.info("----------deal block " + str(blk_num))
    j = list(db.account_history.find({'bulk.block_data.block_num': blk_num}))
    if len(j) == 0:
        mongo_blk_num = get_mongo_lib()
        if mongo_blk_num <= blk_num:
            logging.info('mongo blk num is '+ str(mongo_blk_num) )
            logging.erorr('Zero trx from mongo, please check mongodb!')
            time.sleep(10)
            continue
    for n in range(len(j)):#only deal op4
        if j[n]['bulk']['operation_type'] != 4:
            continue
        j[n]['id'] = str(j[n]['_id'] )
        j[n].pop('_id')
        content = flatten(j[n] , '__')
        json2k = json.dumps(json.dumps(content))
        sql = 'json2k : ' + json2k + ';ele: enlist .j.k  json2k ;ele: update bulk__block_data__block_time:"P"$bulk__block_data__block_time from ele;'
        qconn.k(sql)
        sql = 'op4,: ele;'
        try:
            qconn.k(sql)
        except:
            logging.error(sql)
            logging.error(j[n])
            # exit(-1)
    if blk_num >= blk_circle and blk_num % blk_circle == 0:
        # qconn = conn_reset(qconn)
        conn_reset(qconn)
    blk_num += 1

qconn.close()

def parse(j):
    from flatten_json import flatten
    import json
    content = flatten(j, '__')
    for _k in content.keys():
        if 'memo' in _k or 'extensions' in _k :
            content.pop(_k)
    # json2k = json.dumps(json.dumps(content))
    content = OrderedDict(sorted(content.items(), key=lambda t: t[0]))
    return content
