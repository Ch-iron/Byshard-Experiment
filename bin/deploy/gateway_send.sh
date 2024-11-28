SERVER_ADDR=(`cat ../../common/base_ips.txt`)

scp -r ../../common/contracts ubuntu@${SERVER_ADDR[0]}:/home/ubuntu/common
scp ../../common/address.txt ubuntu@${SERVER_ADDR[0]}:/home/ubuntu/common
ssh ubuntu@${SERVER_ADDR[0]} "cd common; mkdir ca;"
scp -r ../../common/ca/10 ubuntu@${SERVER_ADDR[0]}:/home/ubuntu/common/ca