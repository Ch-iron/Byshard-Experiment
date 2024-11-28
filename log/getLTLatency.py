import os
import re
import shutil

file_list = os.listdir('./0')
p = re.compile('.shard.')
for file in file_list:
    m = p.search(file)
    if m:
        os.remove('./0/'+file)

file_list = os.listdir('./0')
p = re.compile('.csv')
for file in file_list:
    m = p.search(file)
    if m:
        src = './0/'+file
        dst = './0/0.csv'
        os.rename(src, dst)
        break

for i in range(0, 5):
    file_list = os.listdir('./'+str(i))
    p = re.compile('.log')
    for file in file_list:
        m = p.search(file)
        if m:
            src = './'+str(i)+'/'+file
            dst = './'+str(i)+'/'+str(i)+'.log'
            os.rename(src, dst)
            break

path = "./skew_factor/9/4"
shutil.copy('./0/0.csv', path)
for i in range(0, 5):
    shutil.copy('./'+str(i)+'/'+str(i)+'.log', path)

file_list = os.listdir(path)
shard_latency = []
for file in file_list:
    if file != '0.log' and file != '0.csv':
        f = open(path+'/'+file)
        lines = f.readlines()
        seperate = lines[-2].split(': ')
        localLatency = seperate[-1]
        localLatency = localLatency.split('\n')[0]
        shard_latency.append(float(localLatency))
mean = sum(shard_latency) / len(shard_latency)
print("local: "+str(round(mean, 3)))
f = open(path+'/0.log')
lines = f.readlines()
seperate = lines[-1].split(': ')
crossLatency = seperate[2]
crossLatency = crossLatency.split(',')[0]
print('cross: '+crossLatency)
