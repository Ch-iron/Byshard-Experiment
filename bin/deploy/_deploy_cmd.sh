USERNAME=$1
SERVER_ADDR=$2
SHARD=$3

ssh ${USERNAME}@${SERVER_ADDR} mkdir bin
ssh ${USERNAME}@${SERVER_ADDR} mkdir common
ssh ${USERNAME}@${SERVER_ADDR} mkdir common/statedb
ssh ${USERNAME}@${SERVER_ADDR} mkdir common/statedb/6

scp ../../common/statedb/6/shard${SHARD}_root.txt ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/common/statedb/6

echo "upload replica ${USERNAME}@${SERVER_ADDR}"
scp ../server ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/bin
scp ../client ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/bin
scp ../start_protocol.sh ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/bin
scp ../stop_protocol.sh ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/bin

scp ../../common/config.json ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/common
scp ../../common/base_ips.txt ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/common
scp ../../common/ips.txt ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/common
scp -r ../../common/statedb/6/${SHARD} ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/common/statedb/6

# scp -r ../../common/contracts ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/common
# scp ../../common/address.txt ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/common
# ssh ${USERNAME}@${SERVER_ADDR} "cd common; mkdir ca; mkdir statedb;"
# scp -r ../../common/ca/4 ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/common/ca
