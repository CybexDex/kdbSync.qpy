
/dbpath:`:/home/sunqi/mudb/cybex
setDBEnv:{[p;name] 
 dbpath::p ;
 tbname::name ;}


sympath::` sv dbpath,`$"/db"

eleUpdate:{[json2k]
 ele: enlist .j.k  json2k ;
 ele: update bulk__block_data__block_time:"P"$bulk__block_data__block_time from ele;
 ele: select bulk__block_data__block_time,bulk__operation_type,op__fill_price__quote__amount,op__fee__amount,op__fill_price__base__amount,bulk__block_data__block_num,op__pays__amount,op__is_maker,op__receives__amount,bulk__account_history__sequence,`$bulk__account_history__next,`$bulk__block_data__trx_id,`$op__fill_price__base__asset_id,`$id,`$op__fill_price__quote__asset_id,`$op__pays__asset_id,`$op__receives__asset_id,`$op__account_id,`$bulk__account_history__account,`$bulk__account_history__id,`$op__order_id,`$op__fee__asset_id,`$bulk__account_history__operation_id  from ele;
 op4,::ele}

/ N represents expire hour, here should be set as 24
expireDel:{[N]
 op4::delete from op4 where bulk__block_data__block_num < ((max bulk__block_data__block_num) - N * 1200) }


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
/ trade::op4,op4back 
trade::op4

/ mv csv to new csv with timestamp
mvcsv:{ save `op4.csv; system "mv op4.csv /data2/db/tmp/op4.csv.`date +%Y%m%d.%H%M%S`";}


