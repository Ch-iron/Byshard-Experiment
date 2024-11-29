SHARD=$1
ID=$2

SERVER_PID_FILE=server.pid
SERVER_PID=$(cat "${SERVER_PID_FILE}");
if [ -z "${SERVER_PID}" ]; then
    echo "Process id for servers is written to location: {$SERVER_PID_FILE}"
    rm *.log
    rm *.csv
    if [ ${SHARD} -eq 0 ]; then
        ./server -sim=false -mode=gateway -shard=${SHARD} -id=${ID} 2> error.log &
    else
        if [ ${ID} -eq 0 ]; then
            ./server -sim=false -mode=communicator -shard=${SHARD} -id=${ID} 2> error.log &
        else
            ./server -sim=false -mode=node -shard=${SHARD} -id=${ID} 2> error.log &
        fi
    fi
    echo $! >> ${SERVER_PID_FILE}
    echo "${alg} is running, on log severity: ${severity}"
    echo "=== if error occurs, plz check error.log ==="
else
    echo "Servers are already started in this folder."
    exit 0
fi