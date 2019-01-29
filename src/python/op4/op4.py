from pymongo import MongoClient
import json, datetime
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

qconn = q.q(host = 'localhost', port = config.Q_port , user = config.Passfile)
# qconn.k('\\l /home/sunqi/mudb/test1/test.q')
logging.info("Loading " + config.Q_Script + "...")
# qconn.k('\\l ' + config.Q_Script )
qconn.k('setDBEnv[`:%s;`%s];' % (config.Q_DBPATH, 'op4') )

client = MongoClient(config.MONGODB_DB_URL)
db = client[config.MONGODB_DB_NAME]


def conn_reset(qconn):
    logging.info( 'one circle!')
    return 
    try:
        qconn.k('op4:0#op4 ;' )
    except:
        logging.error( 'op4 not found!')
    qconn.close()
    try:
        qconn = q.q(host = 'localhost', port = config.Q_port , user = config.Passfile)
        qconn.k('\\l ' + config.Q_Script)
        qconn.k('setDBEnv[`:%s;`%s];' % (config.Q_DBPATH, 'op4') )
    except:
        logging.error('failed to open qconn with ' + str(config.Q_port))


def get_lib():
    import os
    try:
        # res = int(os.popen('/usr/bin/python3 ../bts_info.py').read())
        # res = int(os.popen3(config.cmd_lastblk)[1].read().strip())
        res = int(json.loads(os.popen3(config.cmd_lastblk)[1].read())['result']['head_block_number'])
    except:
        logging.error('lib config.cmd_lastblk script failed')
        try:
            res = int(json.loads(os.popen3(config.cmd_lastblk_bk1)[1].read()))
        except:
            logging.error('lib config.cmd_lastblk_bk1 script failed')
            return None
    if config.offset_date>0:
        return res - config.offset_date * 28800
    return res

# start_blk = config.START_BLK
blk_circle = 1200*24
def get_mongo_lib():
    try:
        init_size_limit_json = list(db.account_history.find().sort([("_id",-1)]).limit(1))[0]
        init_size_limit = init_size_limit_json['bulk']['block_data']['block_num']
    except:
        logging.error('failed to fetch data from get_mongo_lib!')
        init_size_limit = 0
    return init_size_limit
# logging.info("=========== query blocks till -> "+ str(init_size_limit) +"===================\n")
def get_start(last):
    return last - blk_circle
lib = get_lib()
blk_num = get_start(lib)

logging.info("=========== query blocks start -> "+ str(blk_num) +"===================\n")
while 1:
    if lib == None or blk_num >= lib - 1:
        time.sleep(2)
        lib = get_lib()
        continue
    logging.info("----------deal block " + str(blk_num))
    j = list(db.account_history.find({'bulk.block_data.block_num': blk_num, 'bulk.operation_type':4}))
    if len(j) == 0:
        mongo_blk_num = get_mongo_lib()
        if mongo_blk_num <= blk_num:
            logging.info('mongo blk num is '+ str(mongo_blk_num) )
            logging.error('Zero trx from mongo, please check mongodb!')
            time.sleep(10)
            continue
    for n in range(len(j)):#only deal op4
        j[n]['id'] = str(j[n]['_id'] )
        j[n].pop('_id')
        j[n].pop('result')
        content = flatten(j[n] , '__')
        if config.offset_date > 0:
            content['bulk__block_data__block_time'] = (datetime.datetime.strptime(content['bulk__block_data__block_time'],'%Y-%m-%dT%H:%M:%S') + datetime.timedelta(config.offset_date,0,0)).strftime('%Y-%m-%dT%H:%M:%S')
        for kk in ('op__fill_price__quote__amount','op__fill_price__base__amount','op__pays__amount','op__receives__amount'):
            content[kk] = float(content[kk] )
        json2k = json.dumps(json.dumps(content))
        sql = 'json2k : ' + json2k + ';eleUpdate[json2k];' # update on ele and op4
        try:
            qconn.k(sql)
        except:
            logging.error(sql)
            logging.error(content)
            logging.error(j[n])
            # exit(-1)
    if len(j) > 0:
        sql = 'expireDel[24];' # delete expilere blk on op4
        try:
            qconn.k(sql)
        except:
            logging.error('delete expiler blk on op4 failed on ' + str(blk_num))
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
