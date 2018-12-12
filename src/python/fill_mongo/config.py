import os


READ_MONGODB_DB_URL = os.environ.get('MONGO_WRAPPER', "mongodb://yoyo:yoyo123@39.105.55.115:27017/cybex") # clockwork
READ_MONGODB_DB_NAME = os.environ.get('MONGO_DB_NAME', 'cybex')


WRITE_MONGODB_DB_URL = os.environ.get('MONGO_WRAPPER', "mongodb://cybex:Jsh64mcy1H9a@47.75.121.166:27017/cybex") # clockwork
WRITE_MONGODB_DB_NAME = os.environ.get('MONGO_DB_NAME', 'cybex')


START_BLK = 8214584
END_BLK = 8242271

