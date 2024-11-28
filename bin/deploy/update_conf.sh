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
    SERVER_ADDR=(`cat ../../common/base_ips.txt`)
    for (( j=0; j<=${SHARD}; j++))
    do 
        echo "Update Shard ${j} ${SERVER_ADDR[j]}"
       ./_update_cmd.sh ubuntu ${SERVER_ADDR[j]}
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
