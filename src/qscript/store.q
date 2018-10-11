
/dbpath:`:/home/sunqi/mudb/cybex
dbpath:`:/data2/db/kdb/cybex
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
