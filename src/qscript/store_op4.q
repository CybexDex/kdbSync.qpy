
/dbpath:`:/home/sunqi/mudb/cybex
setDBEnv:{[p;name] 
 dbpath::p ;
 tbname::name ;}

sympath::` sv dbpath,`$"/db"

eleUpdate:{[json2k]
 ele: enlist .j.k  json2k ;
 ele: update bulk__block_data__block_time:"P"$bulk__block_data__block_time from ele;
 ele: select bulk__block_data__block_time,bulk__operation_type,op__fill_price__quote__amount,op__fee__amount,op__fill_price__base__amount,bulk__block_data__block_num,op__pays__amount,op__is_maker,op__receives__amount,bulk__account_history__sequence,`$bulk__account_history__next,`$bulk__block_data__trx_id,`$op__fill_price__base__asset_id,`$id,`$op__fill_price__quote__asset_id,`$op__pays__asset_id,`$op__receives__asset_id,`$op__account_id,`$bulk__account_history__account,`$bulk__account_history__id,`$op__order_id,`$op__fee__asset_id,`$bulk__account_history__operation_id  from ele;op4,::ele }


tbstore:{[t;kk]
 a:flip t[kk];
 dbmonth: kk`month;
 segment_num:kk`seg;
 dps:` sv dbpath,`$string(segment_num),`$string(dbmonth),tbname,`;
 dps upsert .Q.en[sympath;a];}

tbupdate:{[x]
 t1:`seg`month xgroup (update seg:bulk__block_data__block_num mod 10, month:bulk__block_data__block_time.month from x);
 k1: key t1;
 if[(count k1) > 1;tbstore[t1] each k1;]}

/ prepare
lib::(last op4)`bulk__block_data__block_num
trade::op4,op4back 
/ non-net
v_24::select from trade where (lib - bulk__block_data__block_num) <= 28800
v_12::select from trade where (lib - bulk__block_data__block_num) <= 14400
v_1::select from trade where (lib - bulk__block_data__block_num) <= 1200

vpay_24::select acct:op__account_id, pamt:op__pays__amount, passet:op__pays__asset_id from v_24
vpay_12::select acct:op__account_id, pamt:op__pays__amount, passet:op__pays__asset_id from v_12
vpay_1::select acct:op__account_id, pamt:op__pays__amount, passet:op__pays__asset_id from v_1

vrcv_24::select acct:op__account_id, ramt:op__receives__amount, rasset:op__receives__asset_id from v_24
vrcv_12::select acct:op__account_id, ramt:op__receives__amount, rasset:op__receives__asset_id from v_12
vrcv_1::select acct:op__account_id, ramt:op__receives__amount, rasset:op__receives__asset_id from v_1

top_sell_24::raze {select [20] from flip x} each  select asset:passet,acct,pamt by passet from `passet`pamt xdesc (select sum pamt by acct,passet from vpay_24)
top_sell_12::raze {select [20] from flip x} each  select asset:passet,acct,pamt by passet from `passet`pamt xdesc (select sum pamt by acct,passet from vpay_12)
top_sell_1::raze {select [20] from flip x} each  select asset:passet,acct,pamt by passet from `passet`pamt xdesc (select sum pamt by acct,passet from vpay_1)

top_buy_24::raze {select [20] from flip x} each  select asset:rasset,acct,ramt by rasset from `rasset`ramt xdesc (select sum ramt by acct,rasset from vrcv_24)
top_buy_12::raze {select [20] from flip x} each  select asset:rasset,acct,ramt by rasset from `rasset`ramt xdesc (select sum ramt by acct,rasset from vrcv_12)
top_buy_1::raze {select [20] from flip x} each  select asset:rasset,acct,ramt by rasset from `rasset`ramt xdesc (select sum ramt by acct,rasset from vrcv_1)

/ net
p1_24::select acct,pamt,asset:passet from (select sum pamt by acct,passet from vpay_24)
p1_12::select acct,pamt,asset:passet from (select sum pamt by acct,passet from vpay_12)
p1_1::select acct,pamt,asset:passet from (select sum pamt by acct,passet from vpay_1)
r1_24::select acct,ramt,asset:rasset from (select sum ramt by acct,rasset from vrcv_24)
r1_12::select acct,ramt,asset:rasset from (select sum ramt by acct,rasset from vrcv_12)
r1_1::select acct,ramt,asset:rasset from (select sum ramt by acct,rasset from vrcv_1)

p2_24::select pamt by acct, asset from p1_24
p2_12::select pamt by acct, asset from p1_12
p2_1::select pamt by acct, asset from p1_1
r2_24::select ramt by acct, asset from r1_24
r2_12::select ramt by acct, asset from r1_12
r2_1::select ramt by acct, asset from r1_1

p3_24::select acct,pamt,asset,0^ {x@0} each ramt from p1_24 lj r2_24
p3_12::select acct,pamt,asset,0^ {x@0} each ramt from p1_24 lj r2_12
p3_1::select acct,pamt,asset,0^ {x@0} each ramt from p1_24 lj r2_1
r3_24::select acct,ramt,asset,0^ {x@0} each pamt from r1_24 lj p2_24
r3_12::select acct,ramt,asset,0^ {x@0} each pamt from r1_24 lj p2_12
r3_1::select acct,ramt,asset,0^ {x@0} each pamt from r1_24 lj p2_1

top_net_buy_24::raze {select [20] from flip x} each  select asset,acct,pamt by asset from `asset`pamt xdesc (select from (select sum (pamt - ramt ) by asset, acct from p3_24) where pamt >=0 )
top_net_buy_12::raze {select [20] from flip x} each  select asset,acct,pamt by asset from `asset`pamt xdesc (select from ( select sum (pamt - ramt ) by asset, acct from p3_12) where pamt >= 0)
top_net_buy_1::raze {select [20] from flip x} each  select asset,acct,pamt by asset from `asset`pamt xdesc (select from (select sum (pamt - ramt ) by asset, acct from p3_1) where pamt >=0 )
top_net_sell_24::raze {select [20] from flip x} each  select asset,acct,ramt by asset from `asset`ramt xdesc (select from (select sum (ramt - pamt ) by asset, acct from r3_24) where ramt >= 0)
top_net_sell_12::raze {select [20] from flip x} each  select asset,acct,ramt by asset from `asset`ramt xdesc (select from (select sum (ramt - pamt ) by asset, acct from r3_12) where ramt >= 0)
top_net_sell_1::raze {select [20] from flip x} each  select asset,acct,ramt by asset from `asset`ramt xdesc (select from (select sum (ramt - pamt ) by asset, acct from r3_1) where ramt >= 0)

/ pair
pv_24::select acct:op__account_id, pamt:op__pays__amount, passet:op__pays__asset_id, ramt:op__receives__amount, rasset:op__receives__asset_id, basset:(op__fill_price__base__asset_id),qasset:(op__fill_price__quote__asset_id) from v_24
pv_12::select acct:op__account_id, pamt:op__pays__amount, passet:op__pays__asset_id, ramt:op__receives__amount, rasset:op__receives__asset_id, basset:(op__fill_price__base__asset_id),qasset:(op__fill_price__quote__asset_id) from v_12
pv_1::select acct:op__account_id, pamt:op__pays__amount, passet:op__pays__asset_id, ramt:op__receives__amount, rasset:op__receives__asset_id, basset:(op__fill_price__base__asset_id),qasset:(op__fill_price__quote__asset_id) from v_1 

/ pair non-net
top_pair_buy_1::raze {select [20] from flip x} each select basset,qasset, acct,amt by basset,qasset from `basset`qasset`amt xdesc (select amt:sum ramt by basset,qasset,acct from pv_1 where passet = basset)
top_pair_buy_12::raze {select [20] from flip x} each select basset,qasset, acct,amt by basset,qasset from `basset`qasset`amt xdesc (select amt:sum ramt by basset,qasset,acct from pv_12 where passet = basset)
top_pair_buy_24::raze {select [20] from flip x} each select basset,qasset, acct,amt by basset,qasset from `basset`qasset`amt xdesc (select amt:sum ramt by basset,qasset,acct from pv_24 where passet = basset)
top_pair_sell_1::raze {select [20] from flip x} each select basset,qasset, acct,amt by basset,qasset from `basset`qasset`amt xdesc (select amt:sum pamt by basset,qasset,acct from pv_1 where passet = qasset)
top_pair_sell_12::raze {select [20] from flip x} each select basset,qasset, acct,amt by basset,qasset from `basset`qasset`amt xdesc (select amt:sum pamt by basset,qasset,acct from pv_12 where passet = qasset)
top_pair_sell_24::raze {select [20] from flip x} each select basset,qasset, acct,amt by basset,qasset from `basset`qasset`amt xdesc (select amt:sum pamt by basset,qasset,acct from pv_24 where passet = qasset)

/ pair net
top_pair_net_buy_1::raze {select [20] from flip x} each select basset,qasset, acct,amt by basset,qasset from (`basset`qasset`amt xdesc select basset,qasset,acct,amt : ramt- 0^pamt from ((select sum ramt by basset,qasset,acct from pv_1 where passet = basset) lj (select sum pamt by basset,qasset,acct from pv_1 where passet = qasset)) )where amt >=0
top_pair_net_buy_12::raze {select [20] from flip x} each select basset,qasset, acct,amt by basset,qasset  from (`basset`qasset`amt xdesc select basset,qasset,acct,amt : ramt- 0^pamt from ((select sum ramt by basset,qasset,acct from pv_12 where passet = basset) lj (select sum pamt by basset,qasset,acct from pv_12 where passet = qasset)) )where amt >=0
top_pair_net_buy_24::raze {select [20] from flip x} each select basset,qasset, acct,amt by basset,qasset from (`basset`qasset`amt xdesc select basset,qasset,acct,amt : ramt- 0^pamt from ((select sum ramt by basset,qasset,acct from pv_24 where passet = basset) lj (select sum pamt by basset,qasset,acct from pv_24 where passet = qasset)) )where amt >=0

top_pair_net_sell_1::raze {select [20] from flip x} each select basset,qasset, acct,amt by basset,qasset  from (`basset`qasset`amt xdesc select basset,qasset,acct,amt : pamt- 0^ramt from ((select sum pamt by basset,qasset,acct from pv_1 where passet = qasset) lj (select sum ramt by basset,qasset,acct from pv_1 where passet = basset)) )where amt >=0
top_pair_net_sell_12::raze {select [20] from flip x} each select basset,qasset, acct,amt by basset,qasset from (`basset`qasset`amt xdesc select basset,qasset,acct,amt : pamt- 0^ramt from ((select sum pamt by basset,qasset,acct from pv_12 where passet = qasset) lj (select sum ramt by basset,qasset,acct from pv_12 where passet = basset)) )where amt >=0
top_pair_net_sell_24::raze {select [20] from flip x} each select basset,qasset, acct,amt by basset,qasset from (`basset`qasset`amt xdesc select basset,qasset,acct,amt : pamt- 0^ramt from ((select sum pamt by basset,qasset,acct from pv_24 where passet = qasset) lj (select sum ramt by basset,qasset,acct from pv_24 where passet = basset)) )where amt >=0

/ return top_sell_24,top_sell_12,top_sell_1 top_buy_24,top_buy_12,top_buy_1 top_net_buy_24,top_net_buy_12,top_net_buy_1 top_net_sell_24,top_net_sell_12,top_net_sell_1

/ mv csv to new csv with timestamp
mvcsv:{ save `op4.csv; system "mv op4.csv /data2/db/tmp/op4.csv.`date +%Y%m%d.%H%M%S`";}


