
reconnect:{[] hbase::hopen `$":210.3.74.58:6020:uatuser:u@T$Yb";hrdb::hopen `$":210.3.74.58:6036:uatuser:u@T$Yb";hhdb::hopen `$":210.3.74.58:6037:uatuser:u@T$Yb";}
closeconn:{[] hclose hbase; hclose hrdb; hclose hhdb;}
/ prepare
get_accounts_table:{[inputfile]
 t:("SSF";enlist",")0:inputfile;update start_cap:1000000 from t}

eos:1;usdt:1;eth:1;btc:1;
yesterday: .z.p-1D
yesterdate:yesterday.date
updateTradesTable:{[] trades_table::select asset_name,account,amount from select sum amount by account,asset_name from ((hhdb"select side:`buy,asset_name:receiveAmount_symbol,account:accountName, amount:receiveAmount_amount from select sum receiveAmount_amount% (10 xexp receiveAmount_precision) by receiveAmount_symbol,accountName from  keyedTrade where (date=" , (string yesterdate) , ") and receiveAmount_symbol in (`JADE.ETH`JADE.USDT`JADE.EOS`JADE.BTC)"),(hhdb"select side:`sell,asset_name:soldAmount_symbol,account:accountName, amount:soldAmount_amount from select sum soldAmount_amount% (10 xexp soldAmount_precision) by soldAmount_symbol,accountName from  keyedTrade where (date=" , (string yesterdate) , ") and soldAmount_symbol in (`JADE.ETH`JADE.USDT`JADE.EOS`JADE.BTC)")) }


`HTTPS_PROXY setenv "socks5h://localhost:1080"
getEosRate:{[] t:hbase"select [-1] sym,exchange,lastPx,mp:(ap0+bp0)%2 from .emd.MarketDataTick where (sym=`EOSUSDT ) and (exchange=`BINANCE) " ; eos::(t`mp)@0; t }
getEthRate:{[] t:hbase"select [-1] sym,exchange,lastPx,mp:(ap0+bp0)%2 from .emd.MarketDataTick where (sym=`ETHUSDT ) and (exchange=`BINANCE) "  ; eth:: (t`mp)@0; t }
getBtcRate:{[] t:hbase"select [-1] sym,exchange,lastPx,mp:(ap0+bp0)%2 from .emd.MarketDataTick where (sym=`BTCUSDT ) and (exchange=`BINANCE) "  ; btc:: (t`mp)@0; t }

sympath::` sv dbpath,`$"/db"

/accountName, asset_name, quantity
updateBalance:{[] balance:: hrdb"select account:accountName,asset_name:symbol, amount:settled % (10 xexp precision)  from keyedPosition where symbol in (`JADE.EOS`JADE.ETH`JADE.USDT`JADE.BTC)" }

portfolio_ind::update sharp:0^ {if[x=0w;x:0];x} each (ret%risk) from select ncap:last cap ,ret:last(0 ^ -1 + cap % prev cap) ,total_ret :0^ -1 + (last cap)%(first cap),risk:0^ sdev (-1 + cap % prev cap) by account from `date xasc portfolio

SIZE::count portfolio
valid_SIZE::count valid_RV

RV::update rv_score:fills {if[x=0;x:0N];x} each rv_score from update rv_score:rv_score*(1-x) from select account,rv:total_ret,sharp,cap:ncap, rv_score:(SIZE - sums x) * 100 % SIZE, x:total_ret=(prev total_ret) from update x:1  from `total_ret xdesc portfolio_ind
Period_RV::select account,rv:ret,sharp,cap:ncap, score:(SIZE - sums x) * 100 % SIZE from update x:1 from `ret xdesc portfolio_ind
valid_RV::select from RV where rv_score > ((select [ (floor (count RV) * 0.3),1] from RV)`rv_score)@0 

SHARP::update sharp_score:fills {if[x=0;x:0N];x} each sharp_score from update sharp_score:sharp_score*(1-x) from  select account,rv,sharp,cap, sharp_score:(valid_SIZE - sums x) * 100 % valid_SIZE, x:sharp=(prev sharp) from update x:1  from `sharp xdesc valid_RV
COMP::`score xdesc (select account,cap, score: (rv_score*0.6)+(sharp_score*0.35)+ (is_to*0.05), rv,sharp, turnover from ej[`account;ej[`account;RV;SHARP];to_ind])

get_ranking:{[limit;algo]  select [limit] from ej[`account;algo;to_ind] }

/ dumpfile:{[dir] save `$( "" sv (string(dir);"balance.csv" ) );  }
dumpfile:{[] save `balance.csv }

updateTurnover:{[]
 v1:select as1:"f"$(first amount) by account from trades_table where asset_name = `JADE.USDT;
 v2:select as2:"f"$(first amount) by account from trades_table where asset_name = `JADE.ETH;
 v3:select as3:"f"$(first amount) by account from trades_table where asset_name = `JADE.EOS;
 v4:select as4:"f"$(first amount) by account from trades_table where asset_name = `JADE.BTC;
 t:select distinct account from balance;
 to_ind::select account, turnover,is_to:turnover>=20000 from update turnover:(as1*usdt) +(as2* eth) + (as3 * eos) + (as4 * btc) from `account xkey update 0^as1, 0^as2 ,0^as3, 0^as4 from (((t lj v1) lj v2) lj v3) lj v4; }

accounts_table:get_accounts_table[`:/home/sunqi/zmq_wit/accounts_table.csv];

updatePortfolio:{[]
 v1:select as1:"f"$(first amount) by account from balance where asset_name = `JADE.USDT;
 v2:select as2:"f"$(first amount) by account from balance where asset_name = `JADE.ETH;
 v3:select as3:"f"$(first amount) by account from balance where asset_name = `JADE.EOS;
 v4:select as4:"f"$(first amount) by account from balance where asset_name = `JADE.BTC;
 bact:select date:.z.p,start_cap:1000000,account from  select distinct account from balance;
 tmp:update account, cap:(as1*usdt) +(as2* eth) + (as3 * eos) + (as4 * btc) from `date`account xkey update 0^  as1, 0^ as2 ,0^ as3 ,0^ as4 from (((bact lj v1) lj v2) lj v3) lj v4;
 tmp:delete account_id from tmp;
 portfolio,::tmp}

getPortfolio:{[acct;slot] 
 /select [100]  time:date.datetime,as1,as2,as3,cap from `date xdesc portfolio where (account = `$acct) and ( ((floor ( 1e-09*((date - 2018.01.01D )%01:00) )) mod slot ) =0)}
 select [100]  time:date.datetime,as1,as2,as3,cap from `date xdesc portfolio where (account = `$acct) and ( ((floor ( 1e-09*((date - (min date) )%01:00:00) )) mod slot ) =0)}

/ mv csv to new csv with timestamp
/ mvcsv:{ save `trade.csv; system "mv trade.csv /data2/db/tmp/trade.csv.`date +%Y%m%d.%H%M%S`";}


/ define your timer
updateAll:{[] reconnect[]; updateBalance[];getEosRate[];getBtcRate[];getEthRate[];updatePortfolio[];updateTradesTable[];updateTurnover[];closeconn[];}
updateAll[];updateAll[];
.z.ts:{updateAll[];}

/ 60 seconds set timer
/ \t 60000
/ 10*60 seconds set timer, 10minute
/ \t 600000
/ 10*60 seconds set timer, 10minute
\t 3600000

/ \t 0 to stop the timer
