
/ prepare
\p 9005
h:hopen `:localhost:9008:cybexdev:3ff625a14c8a3a6ddf3665c5b6c2798a;
N:10

trade:{[hour]
 dur:hour * 01:00:00;
 res:h"select from trade where (.z.P - bulk__block_data__block_time) <= ",string(dur);
 if[hour=24;v_24::res];
 if[hour=12;v_12::res];
 if[hour=1;v_1::res]; }

/ non-net

vpay_24::select acct:op__account_id, pamt:op__pays__amount, passet:op__pays__asset_id from v_24
vpay_12::select acct:op__account_id, pamt:op__pays__amount, passet:op__pays__asset_id from v_12
vpay_1::select acct:op__account_id, pamt:op__pays__amount, passet:op__pays__asset_id from v_1

vrcv_24::select acct:op__account_id, ramt:op__receives__amount, rasset:op__receives__asset_id from v_24
vrcv_12::select acct:op__account_id, ramt:op__receives__amount, rasset:op__receives__asset_id from v_12
vrcv_1::select acct:op__account_id, ramt:op__receives__amount, rasset:op__receives__asset_id from v_1

top_sell_24::raze {select [N] from flip x} each  select asset:passet,acct,pamt by passet from `passet`pamt xdesc (select sum pamt by acct,passet from vpay_24)
top_sell_12::raze {select [N] from flip x} each  select asset:passet,acct,pamt by passet from `passet`pamt xdesc (select sum pamt by acct,passet from vpay_12)
top_sell_1::raze {select [N] from flip x} each  select asset:passet,acct,pamt by passet from `passet`pamt xdesc (select sum pamt by acct,passet from vpay_1)

top_buy_24::raze {select [N] from flip x} each  select asset:rasset,acct,ramt by rasset from `rasset`ramt xdesc (select sum ramt by acct,rasset from vrcv_24)
top_buy_12::raze {select [N] from flip x} each  select asset:rasset,acct,ramt by rasset from `rasset`ramt xdesc (select sum ramt by acct,rasset from vrcv_12)
top_buy_1::raze {select [N] from flip x} each  select asset:rasset,acct,ramt by rasset from `rasset`ramt xdesc (select sum ramt by acct,rasset from vrcv_1)

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
p3_12::select acct,pamt,asset,0^ {x@0} each ramt from p1_12 lj r2_12
p3_1::select acct,pamt,asset,0^ {x@0} each ramt from p1_1 lj r2_1
r3_24::select acct,ramt,asset,0^ {x@0} each pamt from r1_24 lj p2_24
r3_12::select acct,ramt,asset,0^ {x@0} each pamt from r1_12 lj p2_12
r3_1::select acct,ramt,asset,0^ {x@0} each pamt from r1_1 lj p2_1

top_net_buy_24::raze {select [N] from flip x} each  select asset,acct,pamt by asset from `asset`pamt xdesc (select from (select sum (pamt - ramt ) by asset, acct from p3_24) where pamt >=0 )
top_net_buy_12::raze {select [N] from flip x} each  select asset,acct,pamt by asset from `asset`pamt xdesc (select from ( select sum (pamt - ramt ) by asset, acct from p3_12) where pamt >= 0)
top_net_buy_1::raze {select [N] from flip x} each  select asset,acct,pamt by asset from `asset`pamt xdesc (select from (select sum (pamt - ramt ) by asset, acct from p3_1) where pamt >=0 )
top_net_sell_24::raze {select [N] from flip x} each  select asset,acct,ramt by asset from `asset`ramt xdesc (select from (select sum (ramt - pamt ) by asset, acct from r3_24) where ramt >= 0)
top_net_sell_12::raze {select [N] from flip x} each  select asset,acct,ramt by asset from `asset`ramt xdesc (select from (select sum (ramt - pamt ) by asset, acct from r3_12) where ramt >= 0)
top_net_sell_1::raze {select [N] from flip x} each  select asset,acct,ramt by asset from `asset`ramt xdesc (select from (select sum (ramt - pamt ) by asset, acct from r3_1) where ramt >= 0)

/ pair
pv_24::select acct:op__account_id, pamt:op__pays__amount, passet:op__pays__asset_id, ramt:op__receives__amount, rasset:op__receives__asset_id, basset:(op__fill_price__base__asset_id),qasset:(op__fill_price__quote__asset_id) from v_24
pv_12::select acct:op__account_id, pamt:op__pays__amount, passet:op__pays__asset_id, ramt:op__receives__amount, rasset:op__receives__asset_id, basset:(op__fill_price__base__asset_id),qasset:(op__fill_price__quote__asset_id) from v_12
pv_1::select acct:op__account_id, pamt:op__pays__amount, passet:op__pays__asset_id, ramt:op__receives__amount, rasset:op__receives__asset_id, basset:(op__fill_price__base__asset_id),qasset:(op__fill_price__quote__asset_id) from v_1 

/ pair non-net
top_pair_buy_1::raze {select [N] from flip x} each select basset,qasset, acct,amt by basset,qasset from `basset`qasset`amt xdesc (select amt:sum ramt by basset,qasset,acct from pv_1 where passet = basset)
top_pair_buy_12::raze {select [N] from flip x} each select basset,qasset, acct,amt by basset,qasset from `basset`qasset`amt xdesc (select amt:sum ramt by basset,qasset,acct from pv_12 where passet = basset)
top_pair_buy_24::raze {select [N] from flip x} each select basset,qasset, acct,amt by basset,qasset from `basset`qasset`amt xdesc (select amt:sum ramt by basset,qasset,acct from pv_24 where passet = basset)
top_pair_sell_1::raze {select [N] from flip x} each select basset,qasset, acct,amt by basset,qasset from `basset`qasset`amt xdesc (select amt:sum pamt by basset,qasset,acct from pv_1 where passet = qasset)
top_pair_sell_12::raze {select [N] from flip x} each select basset,qasset, acct,amt by basset,qasset from `basset`qasset`amt xdesc (select amt:sum pamt by basset,qasset,acct from pv_12 where passet = qasset)
top_pair_sell_24::raze {select [N] from flip x} each select basset,qasset, acct,amt by basset,qasset from `basset`qasset`amt xdesc (select amt:sum pamt by basset,qasset,acct from pv_24 where passet = qasset)

/ pair net
top_pair_net_buy_1::raze {select [N] from flip x} each select basset,qasset, acct,amt by basset,qasset from (`basset`qasset`amt xdesc select basset,qasset,acct,amt : ramt- 0^pamt from ((select sum ramt by basset,qasset,acct from pv_1 where passet = basset) lj (select sum pamt by basset,qasset,acct from pv_1 where passet = qasset)) )where amt >=0
top_pair_net_buy_12::raze {select [N] from flip x} each select basset,qasset, acct,amt by basset,qasset  from (`basset`qasset`amt xdesc select basset,qasset,acct,amt : ramt- 0^pamt from ((select sum ramt by basset,qasset,acct from pv_12 where passet = basset) lj (select sum pamt by basset,qasset,acct from pv_12 where passet = qasset)) )where amt >=0
top_pair_net_buy_24::raze {select [N] from flip x} each select basset,qasset, acct,amt by basset,qasset from (`basset`qasset`amt xdesc select basset,qasset,acct,amt : ramt- 0^pamt from ((select sum ramt by basset,qasset,acct from pv_24 where passet = basset) lj (select sum pamt by basset,qasset,acct from pv_24 where passet = qasset)) )where amt >=0

top_pair_net_sell_1::raze {select [N] from flip x} each select basset,qasset, acct,amt by basset,qasset  from (`basset`qasset`amt xdesc select basset,qasset,acct,amt : pamt- 0^ramt from ((select sum pamt by basset,qasset,acct from pv_1 where passet = qasset) lj (select sum ramt by basset,qasset,acct from pv_1 where passet = basset)) )where amt >=0
top_pair_net_sell_12::raze {select [N] from flip x} each select basset,qasset, acct,amt by basset,qasset from (`basset`qasset`amt xdesc select basset,qasset,acct,amt : pamt- 0^ramt from ((select sum pamt by basset,qasset,acct from pv_12 where passet = qasset) lj (select sum ramt by basset,qasset,acct from pv_12 where passet = basset)) )where amt >=0
top_pair_net_sell_24::raze {select [N] from flip x} each select basset,qasset, acct,amt by basset,qasset from (`basset`qasset`amt xdesc select basset,qasset,acct,amt : pamt- 0^ramt from ((select sum pamt by basset,qasset,acct from pv_24 where passet = qasset) lj (select sum ramt by basset,qasset,acct from pv_24 where passet = basset)) )where amt >=0

/ return top_sell_24,top_sell_12,top_sell_1 top_buy_24,top_buy_12,top_buy_1 top_net_buy_24,top_net_buy_12,top_net_buy_1 top_net_sell_24,top_net_sell_12,top_net_sell_1

/ mv csv to new csv with timestamp
mvcsv:{ save `op4.csv; system "mv op4.csv /data2/db/tmp/op4.csv.`date +%Y%m%d.%H%M%S`";}

reset_conn:{ hclose h;h::hopen `:localhost:9008;}

turnover_24h:{[] a:select sum op__pays__amount by aid:op__pays__asset_id from v_24;b:select sum op__receives__amount by aid:op__receives__asset_id from v_24;c:select aid, v:(op__receives__amount+op__pays__amount)%2 from a lj b;c}
turnover_24h_s:{[asset_id] a:select sum op__pays__amount by aid:op__pays__asset_id from v_24 where op__pays__asset_id=asset_id;b:select sum op__receives__amount by aid:op__receives__asset_id from v_24 where op__receives__asset_id=asset_id;c:select aid, v:(op__receives__amount+op__pays__amount)%2 from a lj b;c}

fill_count:{[] (count v_24)%2 }
