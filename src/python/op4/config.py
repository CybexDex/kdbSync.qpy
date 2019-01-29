import os

# MONGODB_DB_URL = os.environ.get('MONGO_WRAPPER', "mongodb://yoyo:yoyo123@39.105.55.115:27017/cybex") # clockwork
# MONGODB_DB_NAME = os.environ.get('MONGO_DB_NAME', 'cybex')


MONGODB_DB_URL = os.environ.get('MONGO_WRAPPER', "mongodb://yoyo:yoyo123@39.105.55.115:27017/cybex") # clockwork
MONGODB_DB_NAME = os.environ.get('MONGO_DB_NAME', 'cybex')

# Q_Script = "/home/sunqi/mysrc/kdbSync.qpy/src/qscript/store_op4.q"
Q_Script = "/home/sunqi/mysrc/kdbSync.qpy/src/qscript/store_op4_withour_view.q"
Q_port = 9008
Q_DBPATH = '/data2/db/kdb/cybex2/'
Passfile = 'cybexdev:3ff625a14c8a3a6ddf3665c5b6c2798a'
START_BLK = -1


BTS_NODE = os.environ.get('BTS_NODE', 'ws://127.0.0.1:8090')

cmd_lastblk_bk1 = "curl -X GET --header 'Accept: application/json' 'http://localhost:8081/getlastblocknumbher'"
cmd_lastblk = 'curl --data \'{"jsonrpc":"2.0","method":"get_dynamic_global_properties","params":[],"id":1}\' https://apihk.cybex.io '

offset_date = 10

