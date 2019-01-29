cd <dir of kdbSync.qpy>
python src/python/op4/op4.py &
./bin/home/q/l32/q ./src/qscript/store_op4_withour_view.q -u ./pass  -p 9008 > 9008.log 2>&1 &
./bin/home/q/l32/q ./src/qscript/view_op4.q -u ./pass  > 9008.log 2>&1 &
