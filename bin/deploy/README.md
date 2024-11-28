# Deploy

Bamboo can be deployed in a real network.

1. `cd ./bin/deploy`.
2. Build `server` and `client`.

```
bash build.sh
```

3. ~~Specify external IPs and internal IPs of server nodes in `pub_ips.txt` and `ips.txt`, respectively.~~
4. IPs of machines running as clients are specified in `clients.txt`.
5. The type of the protocol is specified in `run.sh`.
6. Modify configuration parameters in `config.json`.
7. ~~Modify `deploy.sh` and `setup_cli.sh` to specify the username and password for logging onto the server and client machines.~~
8. Upload binaries and config files onto the remote machines.

```
bash deploy.sh
bash setup_cli.sh
```

9. Upload/Update config files onto the remote machines.

```
bash update_conf.sh
```

10. Start the server nodes.

```
bash start.sh
```

11. Log onto the client machine (assuming only one) via ssh and start the client.

```
bash ./runClient.sh
```

The number of concurrent clients can be specified in `runClient.sh`.

12. Log 가져오기

```
bash ./getLogs.sh
```

13. 원격에 Log 삭제하기

```
bash ./clear.sh
```

14. Stop the client and server.

```
bash ./closeClient.sh
bash ./pkill.sh
```
