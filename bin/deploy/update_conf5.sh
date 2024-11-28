#!/usr/bin/env bash
start=`date +%s.%N`

VARIABLE=config.conf

K=1
while read line
do
    if [ ${K} -eq 1 ]; then
        SHARD=$(echo $line | cut -d ":" -f 2)
        SHARD=$(echo $SHARD | cut -d " " -f 2)
    elif [ ${K} -eq 2 ]; then
        COMMITTEE=$(echo $line | cut -d ":" -f 2)
        COMMITTEE=$(echo $COMMITTEE | cut -d " " -f 2)
    fi
    K=$((K+1))
done < $VARIABLE
echo "Shard: ${SHARD}, Committee: ${COMMITTEE}"
TOTALCOMMITTEE=$((SHARD*COMMITTEE))

distribute(){
    NODE_ADDR=(`cat ../../common/ips.txt`)
    for (( j=45; j<60; j++))
    do 
        echo "Update ${j}th ${NODE_ADDR[j]}"
       ./_update_cmd.sh ubuntu ${NODE_ADDR[j]}
       sleep 0.3s
    done
}

rm -rf log/deploy*
# distribute files
distribute

finish=`date +%s.%N`
diff=$( echo "$finish - $start" | bc -l )
echo 'start:' $start
echo 'finish:' $finish
echo 'diff:' $diff
