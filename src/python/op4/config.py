import os



MONGODB_DB_URL = os.environ.get('MONGO_WRAPPER', "mongodb://cybex:Jsh64mcy1H9a@localhost:27017/cybex") # clockwork
MONGODB_DB_NAME = os.environ.get('MONGO_DB_NAME', 'cybex')

Q_Script = "/home/sunqi/kdbSync.qpy/src/qscript/store_op4_withour_view.q"
Q_port = 9008
Passfile = 'cybexdev:3ff625a14c8a3a6ddf3665c5b6c2798a'
START_BLK = -1

Q_DBPATH = '/tmp/cybex'

BTS_NODE = os.environ.get('BTS_NODE', 'ws://127.0.0.1:8090')

cmd_lastblk1 = "curl -X GET --header 'Accept: application/json' 'http://localhost:8081/getlastblocknumbher'"
cmd_lastblk = 'curl --data \'{"jsonrpc":"2.0","method":"get_dynamic_global_properties","params":[],"id":1}\' https://apihk.cybex.io '
offset_date = 0
