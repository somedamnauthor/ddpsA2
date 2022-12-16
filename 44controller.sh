rm output/*
rm input/*.ext
cp input/data/$2 input/file.ext
wait
 
ssh $3 "cd ddpsA2 && python2 $1 4 4 map n 0" 
wait
 
ssh $4 "cd ddpsA2 && python2 $1 4 1 reduce n 0" & 
ssh $5 "cd ddpsA2 && python2 $1 4 1 reduce n 1" & 
ssh $6 "cd ddpsA2 && python2 $1 4 1 reduce n 2"
wait

ssh $7 "cd ddpsA2 && python2 $1 4 1 reduce final 3"
wait

rm input/*.ext
