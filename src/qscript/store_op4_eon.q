
\p 9006
eleUpdate:{[json2k]
 ele: enlist .j.k  json2k ;
 ele: update bulk__block_data__block_time:"P"$bulk__block_data__block_time from ele;
 ele: select bulk__block_data__block_time,bulk__operation_type,op__fill_price__quote__amount,op__fee__amount,op__fill_price__base__amount,bulk__block_data__block_num,op__pays__amount,op__is_maker,op__receives__amount,bulk__account_history__sequence,`$bulk__account_history__next,`$bulk__block_data__trx_id,`$op__fill_price__base__asset_id,`$id,`$op__fill_price__quote__asset_id,`$op__pays__asset_id,`$op__receives__asset_id,`$op__account_id,`$bulk__account_history__account,`$bulk__account_history__id,`$op__order_id,`$op__fee__asset_id,`$bulk__account_history__operation_id  from ele;
 op4,::ele}

/ N represents expire hour, here should be set as 24
expireDel:{[N]
 op4::delete from op4 where bulk__block_data__block_time < ((max bulk__block_data__block_time) - N * 01:00:00 ) }


/ mv csv to new csv with timestamp
mvcsv:{ save `op4.csv; system "mv op4.csv /data2/db/tmp/op4.csv.`date +%Y%m%d.%H%M%S`";}



/ prepare
N:10
M:100

/ latest_time: "P"$(13# string ((last op4)`bulk__block_data__block_time) - 12:00:00)
latest_time:0Np



pair_sort_table : ([op__receives__asset_id:raze `$();op__pays__asset_id:raze `$();op__account_id:raze `$()]total_pay:raze "f"$();total_receive:raze "f"$())



init_pair_sort:{[start_s] start:"P"$start_s; now:"P"$(13#(string (last op4)`bulk__block_data__block_time)); pair_sort_table::update last_update:now from (select  total_receive: sum op__receives__amount,  total_pay: sum op__pays__amount by op__receives__asset_id,op__pays__asset_id,op__account_id from op4 where  (bulk__block_data__block_time>= start) and (bulk__block_data__block_time <now));latest_time:: now;}

pair_sort:{[timepoint]  start:"P"$timepoint;  end :"P"$(13#(string (last op4)`bulk__block_data__block_time)) ;  if[(start = latest_time) & (start < end);latest_time ::end; pair_sort_table::update last_update:latest_time from ((delete last_update from pair_sort_table) + (select  total_receive: sum op__receives__amount,  total_pay: sum op__pays__amount by op__receives__asset_id,op__pays__asset_id,op__account_id from op4 where  (bulk__block_data__block_time>= start) and (bulk__block_data__block_time <end)));]}

test_pair_sort:{[timepoint]  start:"P"$timepoint;  end :"P"$(13#(string (last op4)`bulk__block_data__block_time)) ;res : (select  total_receive: sum op__receives__amount,  total_pay: sum op__pays__amount by op__receives__asset_id,op__pays__asset_id,op__account_id from op4 where  (bulk__block_data__block_time>= start) and (bulk__block_data__block_time <end)); res }

/ get_table:{[k;passet;rasset] select [M] from k xdesc (select account:op__account_id,total_pay,total_receive,last_update from pair_sort_table where (op__pays__asset_id = passet) and (op__receives__asset_id = rasset)) }
get_ptable:{[passet;rasset] select  from `amt xdesc (select account:op__account_id,amt:total_pay,last_update from pair_sort_table where (op__pays__asset_id = passet) and (op__receives__asset_id = rasset)) }
get_rtable:{[passet;rasset] select  from `amt  xdesc (select account:op__account_id,amt:total_receive,last_update from pair_sort_table where (op__pays__asset_id = passet) and (op__receives__asset_id = rasset)) }
get_rank:{[base_asset;quote_asset]a:get_ptable[base_asset;quote_asset];b:get_rtable[quote_asset;base_asset];select [N] account, total:amt, pay, receive from `amt xdesc (update pay:amt from delete last_update from `account xkey a) + (update receive:amt from delete last_update from `account xkey b)}
Update:{[] pair_sort[string latest_time]; }
/ init_pair_sort[string latest_time]
.z.ts:{ Update[];}
/ \t 1800



