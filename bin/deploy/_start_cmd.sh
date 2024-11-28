USERNAME=$1
SERVER_ADDR=$2
ISNODE=$3
SHARD=$4
INDEX=$5

if [ ${ISNODE} -eq 0 ]; then
    if [ ${SHARD} -eq 0 ]; then
        echo "Gateway Start"
        # ssh ${USERNAME}@${SERVER_ADDR} "cd bin; ./server -sim=false -mode=gateway -shard=0 -id=0 2> error.log &"
        ssh ${USERNAME}@${SERVER_ADDR} "cd bin; ./start_protocol.sh ${SHARD} ${INDEX} &"
    else
        echo "BlockBuilder ${SHARD} Start"
        # ssh ${USERNAME}@${SERVER_ADDR} "cd bin; ./server -sim=false -mode=blockbuilder -shard=${SHARD} -id=${INDEX} 2> error.log &"
        ssh ${USERNAME}@${SERVER_ADDR} "cd bin; ./start_protocol.sh ${SHARD} ${INDEX} &"
    fi
else
    echo "shard ${SHARD}, id ${INDEX}"
    echo "Node ${SHARD} ${INDEX} Start"
    # ssh ${USERNAME}@${SERVER_ADDR} "cd bin; ./server -sim=false -mode=blockbuilder -shard=${SHARD} -id=${INDEX} 2> error.log &"
    ssh ${USERNAME}@${SERVER_ADDR} "cd bin; ./start_protocol.sh ${SHARD} ${INDEX} &"
fi
