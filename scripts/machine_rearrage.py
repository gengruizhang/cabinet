import subprocess as sp
import pandas as pd

cpus = []
newid = []
newport = []

df = pd.read_csv('../config/cluster_homo_100.conf', sep=' ', header=None, names=['id', 'ip', 'port', 'name'])

for i, items in df.iterrows():
    print(items[0], items[1], items[2])
    cpuout = sp.check_output("ssh " + items[1] + " \"lscpu | grep 'MHz' | awk '{print \$3}'\"", shell=True)
    cpumhz = float(cpuout.strip())
    cpus.append(cpumhz)
    newid.append(i)
    newport.append(10000+i)

df['cpu'] = cpus
df = df.sort_values('cpu', ascending=False)
df['id'] = newid
df['port'] = newport

df.to_csv('../config/new_cluster_homo_100.conf', sep=' ', header=False, index=False)
