# kdbSync.qpy
## NOTE
1. The script should run under python2 as kdbq lib only support python2
2. You need to know the mongodb address, user, passwd, port and set into config.py
3. You need to connect a delayed node to keep up with chain blks

## Install Deps
After git clone this repo.
1. kdb py client
```
cd lib/kdbq/lib
pip install numpy
pip install pandas 
python setup.py /path/to/this/dir/as/full

```

2. install pymongo
```
pip install pymongo
```

3. install flatten json tool
```
pip install flatten
```

4. Set kdb/q
After fetch q bin (bin/home/q).
```
apt-get install rlwrap
```
Set below to your .bash_profile or .profile under home directory.
```
export PATH=$PATH:/path/to/q/l32:$BASE_DIR/bin
export QHOME=/path/to/q
alias q="rlwrap /path/to/q/l32/q"
```
Here is a structure example 
```
 # tree ~/q
/home/sunqi/q
├── l32
│   └── q
├── q.k
├── q.q
├── README.txt
├── s.k
├── sp.q
└── trade.q

```


## Config 

```
cd src/python
vi config.py

```
please config the right node, mongodb, q connection.

## Start q server/client
```
q -p 9009
```

## Database location
If you want to change kdb database location, need to change store.q code as:
```
dbpath:`:/your/database/location
```



