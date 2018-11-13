
/dbpath:`:/home/sunqi/mudb/cybex
dbpath:`:/data2/db/kdb/cybex2
tbname:`testop0

sympath: ` sv dbpath,`$"/db";


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
/ non-jing
v_24::select from op4,op4back where (lib - bulk__block_data__block_num) <= 10800
v_12::select from op4,op4back where (lib - bulk__block_data__block_num) <= 5400
v_1::select from op4,op4back where (lib - bulk__block_data__block_num) <= 450

vpay_24::select acct:`$op__account_id, pamt:op__pays__amount, passet:`$op__pays__asset_id from v_24
vpay_12::select acct:`$op__account_id, pamt:op__pays__amount, passet:`$op__pays__asset_id from v_12
vpay_1::select acct:`$op__account_id, pamt:op__pays__amount, passet:`$op__pays__asset_id from v_1

vrcv_24::select acct:`$op__account_id, ramt:op__receives__amount, rasset:`$op__receives__asset_id from v_24
vrcv_12::select acct:`$op__account_id, ramt:op__receives__amount, rasset:`$op__receives__asset_id from v_12
vrcv_1::select acct:`$op__account_id, ramt:op__receives__amount, rasset:`$op__receives__asset_id from v_1

top_sell_24::raze {select [20] from flip x} each  select asset:passet,acct,pamt by passet from `passet`pamt xdesc (select sum pamt by acct,passet from vpay_24)
top_sell_12::raze {select [20] from flip x} each  select asset:passet,acct,pamt by passet from `passet`pamt xdesc (select sum pamt by acct,passet from vpay_12)
top_sell_1::raze {select [20] from flip x} each  select asset:passet,acct,pamt by passet from `passet`pamt xdesc (select sum pamt by acct,passet from vpay_1)

top_buy_24::raze {select [20] from flip x} each  select asset:rasset,acct,ramt by rasset from `rasset`ramt xdesc (select sum ramt by acct,rasset from vrcv_24)
top_buy_12::raze {select [20] from flip x} each  select asset:rasset,acct,ramt by rasset from `rasset`ramt xdesc (select sum ramt by acct,rasset from vrcv_12)
top_buy_1::raze {select [20] from flip x} each  select asset:rasset,acct,ramt by rasset from `rasset`ramt xdesc (select sum ramt by acct,rasset from vrcv_1)

/ jing
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

top_jing_buy_24::raze {select [20] from flip x} each  select asset,acct,pamt by asset from `asset`pamt xdesc (select from (select sum (pamt - ramt ) by asset, acct from p3_24) where pamt >=0 )
top_jing_buy_12::raze {select [20] from flip x} each  select asset,acct,pamt by asset from `asset`pamt xdesc (select from ( select sum (pamt - ramt ) by asset, acct from p3_12) where pamt >= 0)
top_jing_buy_1::raze {select [20] from flip x} each  select asset,acct,pamt by asset from `asset`pamt xdesc (select from (select sum (pamt - ramt ) by asset, acct from p3_1) where pamt >=0 )
top_jing_sell_24::raze {select [20] from flip x} each  select asset,acct,ramt by asset from `asset`ramt xdesc (select from (select sum (ramt - pamt ) by asset, acct from r3_24) where ramt >= 0)
top_jing_sell_12::raze {select [20] from flip x} each  select asset,acct,ramt by asset from `asset`ramt xdesc (select from (select sum (ramt - pamt ) by asset, acct from r3_12) where ramt >= 0)
top_jing_sell_1::raze {select [20] from flip x} each  select asset,acct,ramt by asset from `asset`ramt xdesc (select from (select sum (ramt - pamt ) by asset, acct from r3_1) where ramt >= 0)


/ return top_sell_24,top_sell_12,top_sell_1 top_buy_24,top_buy_12,top_buy_1 top_jing_buy_24,top_jing_buy_12,top_jing_buy_1 top_jing_sell_24,top_jing_sell_12,top_jing_sell_1

/ mv csv to new csv with timestamp
mvcsv:{ save `op4.csv; system "mv op4.csv /data2/db/tmp/op4.csv.`date +%Y%m%d.%H%M%S`";}


