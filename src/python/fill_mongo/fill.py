from pymongo import MongoClient
import json
from bson import json_util
from bson.objectid import ObjectId
import config
from flatten_json import flatten
from collections import OrderedDict
import logging, time
# from bitshares import BitShares


logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='log.log',
                filemode='w')


rclient = MongoClient(config.READ_MONGODB_DB_URL)
rdb = rclient[config.READ_MONGODB_DB_NAME]

wclient = MongoClient(config.WRITE_MONGODB_DB_URL)
wdb = wclient[config.WRITE_MONGODB_DB_NAME]




start_blk = config.START_BLK
end_blk = config.END_BLK
blk_num = start_blk
wdb.account_history.delete_many({'bulk.block_data.block_num':start_blk})
while 1:
    if blk_num >= end_blk:
        break;
    logging.info("----------deal block " + str(blk_num))
    j = list(rdb.account_history.find({'bulk.block_data.block_num': blk_num}))
    wdb.account_history.insert_many(j)
    blk_num += 1


logging.info('---------- end -----------------\n')
