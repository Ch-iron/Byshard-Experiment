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


SERVER_ADDR=(`cat ../../common/base_ips.txt`)
for (( j=0; j<=${SHARD}; j++))
do 
    echo "Untar Shard ${j} ${SERVER_ADDR[j]}"
    ssh ubuntu@${SERVER_ADDR[j]} "mkdir bin; tar -zxvf bin.tar -C /home/ubuntu/bin; tar -zxvf common.tar" 1>>../../log/deploy.log 2>>../../log/deploy_err.log &
    # ssh ubuntu@${SERVER_ADDR[j]} rm -rf /home/ubuntu/bin /home/ubuntu/common /home/ubuntu/common.tar
    sleep 0.1s
done
NODE_ADDR=(`cat ../../common/ips.txt`)
K=0
for (( i=1; i<=${COMMITTEE}; i++)) # Node ID
do
    for (( j=1; j<=${SHARD}; j++ )) # Shard
    do
    echo "Shard: ${j}, ID: ${i}, K: ${K} IP: ${NODE_ADDR[K]}"
    ssh ubuntu@${NODE_ADDR[K]} "mkdir bin; tar -zxvf bin.tar -C /home/ubuntu/bin; tar -zxvf common${j}.tar" 1>>../../log/deploy.log 2>>../../log/deploy_err.log &
    # ssh ubuntu@${SERVER_ADDR[j]} rm -rf /home/ubuntu/bin /home/ubuntu/common /home/ubuntu/common${j}.tar
    K=$((K+1))
    sleep 0.1s
    done
done