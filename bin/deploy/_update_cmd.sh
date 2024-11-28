USERNAME=$1
SERVER_ADDR=$2

# scp -r ../../common/config.json ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/common
# scp -r ../../common/base_ips.txt ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/common
# scp -r ../../common/ips.txt ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/common
scp ../server ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/bin
# scp ../client ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/bin
# scp ../start_protocol.sh ${USERNAME}@${SERVER_ADDR}:/home/${USERNAME}/bin
# ssh ${USERNAME}@${SERVER_ADDR} rm -rf bin