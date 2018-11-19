import os

# MONGODB_DB_URL = os.environ.get('MONGO_WRAPPER', "mongodb://yoyo:yoyo123@39.105.55.115:27017/cybex") # clockwork
# MONGODB_DB_NAME = os.environ.get('MONGO_DB_NAME', 'cybex')


MONGODB_DB_URL = os.environ.get('MONGO_WRAPPER', "mongodb://yoyo:yoyo123@39.105.55.115:27017/cybex") # clockwork
MONGODB_DB_NAME = os.environ.get('MONGO_DB_NAME', 'cybex')

Q_Script = "/home/sunqi/mysrc/kdbSync.qpy/src/qscript/store_op4.q"
Q_port = 9008
Q_DBPATH = '/data2/db/kdb/cybex2/'

START_BLK = 7430399


BTS_NODE = os.environ.get('BTS_NODE', 'ws://127.0.0.1:8090')


