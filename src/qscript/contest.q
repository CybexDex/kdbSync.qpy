
reconnect:{[] hapi::hopen `$":210.3.74.58:6039:uatuser:u@T$Yb"}
closeconn:{[] hclose hapi;}


recoverPortfolio:{[timepoint] 
 eos:select account:accountName, asset_name:sym ,amount :amount + lockedAmount from  hapi"getBalanceByAsset[`JADE.EOS;", (string timepoint )," ; `JADE.USDT]";
 eth:select account:accountName, asset_name:sym ,amount :amount + lockedAmount from hapi"getBalanceByAsset[`JADE.ETH;", (string timepoint )," ; `JADE.USDT]";
 btc:select account:accountName, asset_name:sym ,amount :amount + lockedAmount from hapi"getBalanceByAsset[`JADE.BTC;", (string timepoint )," ; `JADE.USDT]"; 
 usdt:select account:accountName, asset_name:sym ,amount :amount + lockedAmount from hapi"getBalanceByAsset[`JADE.USDT;", (string timepoint )," ; `JADE.USDT]"; 
 t::(eos,eth,btc,usdt);
 bact:update time:timepoint from select  cap:sum amount by account from t;
 recov:`time`account xkey update eos:0^eos, eth:0^eth, usdt:0^usdt, btc:0^btc from  (((( bact lj (`account xkey select account, eos:amount from eos) ) lj (`account xkey select account, btc:amount from btc) ) lj (`account xkey select account, eth:amount from eth) ) lj (`account xkey select account,usdt:amount from usdt) );   portfolio,::recov;}


/accountName, asset_name, quantity
updateBalance:{[] now:: "P"$((string(.z.p))[til 13]);
 eos:select account:accountName, asset_name:sym ,amount :amount + lockedAmount from  hapi"getBalanceByAsset[`JADE.EOS;", (string now )," ; `JADE.USDT]";
 eth:select account:accountName, asset_name:sym ,amount :amount + lockedAmount from hapi"getBalanceByAsset[`JADE.ETH;", (string now )," ; `JADE.USDT]";
 btc:select account:accountName, asset_name:sym ,amount :amount + lockedAmount from hapi"getBalanceByAsset[`JADE.BTC;", (string now )," ; `JADE.USDT]"; 
 usdt:select account:accountName, asset_name:sym ,amount :amount + lockedAmount from hapi"getBalanceByAsset[`JADE.USDT;", (string now )," ; `JADE.USDT]"; 
 t:(eos,eth,btc,usdt);
 bact:update time:now from select  cap:sum amount by account from t;
 balance ::`time`account xkey update eos:0^eos, eth:0^eth, usdt:0^usdt, btc:0^btc from  (((( bact lj (`account xkey select account, eos:amount from eos) ) lj (`account xkey select account, btc:amount from btc) ) lj (`account xkey select account, eth:amount from eth) ) lj (`account xkey select account,usdt:amount from usdt) );   portfolio,::balance;}

portfolio_ind::(update sharp:0^ {if[x=0w;x:0];x} each (ret%risk) from select ncap:last cap ,ret:last(0 ^ -1 + cap % prev cap) ,total_ret :0^ -1 + (last cap)%(first cap),risk:0^ sdev (-1 + cap % prev cap) by account from `time xasc portfolio) lj (`account xkey to_ind)

SIZE::count portfolio
valid_SIZE::count valid_RV

RV::update rv_score:fills {if[x=0;x:0N];x} each rv_score from update rv_score:rv_score*(1-x) from select account,rv:total_ret,sharp,cap:ncap, rv_score:(SIZE - sums x) * 100 % SIZE, x:total_ret=(prev total_ret) from update x:1  from `total_ret xdesc portfolio_ind
Period_RV::select account,rv:ret,sharp,cap:ncap, score:(SIZE - sums x) * 100 % SIZE from update x:1 from `ret xdesc portfolio_ind
valid_RV::select from RV where rv_score > ((select [ (floor (count RV) * 0.3),1] from RV)`rv_score)@0 

SHARP::update sharp_score:fills {if[x=0;x:0N];x} each sharp_score from update sharp_score:sharp_score*(1-x) from  select account,rv,sharp,cap, sharp_score:(valid_SIZE - sums x) * 100 % valid_SIZE, x:sharp=(prev sharp) from update x:1  from `sharp xdesc valid_RV
COMP::`score xdesc (select account,cap, score: (rv_score*0.6)+(sharp_score*0.35)+ (is_to*0.05), rv,sharp, turnover from ej[`account;ej[`account;RV;SHARP];to_ind])

get_ranking:{[limit;algo]  select account,rv,sharp,cap,turnover from select [limit] from ej[`account;algo;to_ind] }

/ dumpfile:{[dir] save `$( "" sv (string(dir);"balance.csv" ) );  }
dumpfile:{[] save `balance.csv }

updateTurnover:{[]
 eth:select asset_name:`JADE.ETH, account:accountName , amount:convertedAmount from hapi"getTurnover[`$\"ETH/USDT\";.z.d - 1D; `JADE.USDT]";
 btc:select asset_name:`JADE.BTC, account:accountName , amount:convertedAmount from hapi"getTurnover[`$\"BTC/USDT\";.z.d - 1D; `JADE.USDT]";
 eos:select asset_name:`JADE.EOS, account:accountName , amount:convertedAmount from hapi"getTurnover[`$\"EOS/USDT\";.z.d - 1D; `JADE.USDT]";
 t:select distinct account from balance;
 to_ind::select account, turnover,is_to:turnover>=20000 from update turnover: 0^turnover from (t lj (select turnover:sum amount by account from (eth,btc,eos) ) );}

/ accounts_table:get_accounts_table[`:/home/sunqi/zmq_wit/accounts_table.csv];

getPortfolio:{[acct;slot] select [100] time.datetime, cap, eos,btc,eth, usdt from `time xdesc  portfolio where (account = `$acct) and ( ((floor ( 1e-09*((time - (min time) )%01:00:00) )) mod slot ) =0)}

/ mv csv to new csv with timestamp
/ mvcsv:{ save `trade.csv; system "mv trade.csv /data2/db/tmp/trade.csv.`date +%Y%m%d.%H%M%S`";}


/ define your timer
updateAll:{[] reconnect[]; updateBalance[];updateTurnover[];closeconn[];}

.z.ts:{updateAll[];}

/ 60 seconds set timer
/ \t 60000
/ 10*60 seconds set timer, 10minute
/ \t 600000
/ 10*60 seconds set timer, 10minute
\t 3600000

/ \t 0 to stop the timer
