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

getLog(){
    SERVER_ADDR=(`cat ../../common/base_ips.txt`)
    for (( j=0; j<=${SHARD}; j++))
    do 
        rm -rf ../../log/${j}
        mkdir ../../log/${j}
        mkdir ../../log/${j}/0
        if [ ${j} -eq 0 ]; then
            echo "Shard: ${j}, ID: 0, IP: ${SERVER_ADDR[j]}"
            ./_get_logs_cmd.sh ubuntu ${SERVER_ADDR[j]} ${j}
        fi
       sleep 0.05
    done

    NODE_ADDR=(`cat ../../common/ips.txt`)
    K=0
    for (( i=1; i<=${COMMITTEE}; i++)) # Node ID
    do
        for (( j=1; j<=${SHARD}; j++ )) # Shard
        do
            if [ ${i} -eq 1 ]; then
                echo "Shard: ${j}, ID: ${i}, K: ${K} IP: ${NODE_ADDR[K]}"
                # mkdir ../../log/${j}/${i}
                ./_get_logs_cmd.sh ubuntu ${NODE_ADDR[K]} ${j} ${i}
            fi
            K=$((K+1))
            sleep 0.05
        done
    done
}

# distribute files
getLog

# finish=`date +%s.%N`
# diff=$( echo "$finish - $start" | bc -l )
# echo 'start:' $start
# echo 'finish:' $finish
# echo 'diff:' $diff
