tar -zcvf bin.tar ../server ../client ../start_protocol.sh ../stop_protocol.sh

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

tar -zcvf common.tar ../../common/config.json ../../common/base_ips.txt ../../common/ips.txt 1>>../../log/deploy.log 2>>../../log/deploy_err.log

for (( i=1; i<=${SHARD}; i++ ))
do
    tar -zcvf common${i}.tar ../../common/config.json ../../common/base_ips.txt ../../common/ips.txt ../../common/statedb/10/shard${i}_root.txt ../../common/statedb/10/${i} 1>>../../log/deploy.log 2>>../../log/deploy_err.log
done

mv ./*.tar ./tartar