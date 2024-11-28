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
        echo "Deploy Shard ${j} ${SERVER_ADDR[j]}"
       ./_deploy_cmd.sh ubuntu ${SERVER_ADDR[j]} ${j} 1>>../../log/deploy.log 2>>../../log/deploy_err.log &
       sleep 0.3s
    done
    NODE_ADDR=(`cat ../../common/ips.txt`)
    K=0
    for (( i=1; i<=${COMMITTEE}; i++)) # Node ID
    do
      for (( j=1; j<=${SHARD}; j++ )) # Shard
      do
        echo "Shard: ${j}, ID: ${i}, K: ${K} IP: ${NODE_ADDR[K]}"
        ./_deploy_cmd.sh ubuntu ${NODE_ADDR[K]} ${j} 1>>../../log/deploy.log 2>>../../log/deploy_err.log &
        K=$((K+1))
        sleep 0.1s
      done
    done
    # for (( j=0; j<${TOTALCOMMITTEE}; j++))
    # do 
    #     echo "Deploy ${j}th ${NODE_ADDR[j]}"
    #    ./_deploy_cmd.sh ubuntu ${NODE_ADDR[j]} ${j} 1>>../../log/deploy.log 2>>../../log/deploy_err.log &
    #    sleep 0.3s
    # done
}

rm -rf log/deploy*
# distribute files
distribute

finish=`date +%s.%N`
diff=$( echo "$finish - $start" | bc -l )
echo 'start:' $start
echo 'finish:' $finish
echo 'diff:' $diff
