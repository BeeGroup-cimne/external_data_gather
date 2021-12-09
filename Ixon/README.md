# Ixon Gathering BACnet data
The aim of this code is recover the data from the different buildings managed by Infrastructures.cat that use the protocol BACnet 
to share the devices data.

# Execution
## Connect to Hadoop Cluster
    ssh <user@master_domain>

## Active Python Environment
    source <path_from_env_folder>/bin/activate

## Run MapReduce
    hdfs dfs -rm -r ixon_output; python ixon_gather.py

