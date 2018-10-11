import os

# MONGODB_DB_URL = os.environ.get('MONGO_WRAPPER', "mongodb://yoyo:yoyo123@39.105.55.115:27017/cybex") # clockwork
# MONGODB_DB_NAME = os.environ.get('MONGO_DB_NAME', 'cybex')


MONGODB_DB_URL = os.environ.get('MONGO_WRAPPER', "mongodb://yoyo:yoyo123@39.105.55.115:27017/cybexops") # clockwork
MONGODB_DB_NAME = os.environ.get('MONGO_DB_NAME', 'cybexops')

Q_Script = "../qscript/store.q"
Q_port = 9009


START_BLK = 0


BTS_NODE = os.environ.get('BTS_NODE', 'ws://127.0.0.1:8090')


