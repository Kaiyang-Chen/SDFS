# SDFS

## Design

Basically, we have imitate how google did GFS but in a much simpler way. Architecturally, we have developed a master-slave structured distributed file system based on tcp connections. The leader node does not perform file storage, but only task coordination and sending, as well as resource synchronization checks. Since our most important goal is to achieve strong consistency, the leader node runs three processes periodically in the background: 1. Check if there are enough file backups, and if not, select the storage node with the least current resources for extra file backups; 2. Check if there are enough backups of the current file mapping table of leader, if not, make data backups to more machines to cope with possible leader node crash; 3. Check if there are any files on the storage node that are out of date or do not match the leader information, and clean up the garbage. By running these three backends, the overall system consistency will be ensured.  Regarding the consistency of timing on different nodes, we use total ordering, when the leader accepts each operation request, it increases its own incarnation number and attaches it to the task's information, helping the storage node to order the events. We keep four replica for each files and the master table, and since we ensure strong consistency, we only read one replica oof data when needed. Next, I will give a separate description of each operation. For operations that require communication with other storage nodes, the nodes will first communicate with the leader node to obtain the corresponding information before transferring the file with the target node. When the leader failed, the new leader will be elected follow by a hard coded sequence, then the new leader will sent massage to update config files in all node, and the system go back to normal mode.

## Past MP Use

For membership protocal, we still use the SWIM like procotal we build for MP2, which enables us to detect the join and leave for new nodes and thus do the resource rebalancing. We use distributed-querier in MP1 to debug with our sdfs log.



## Getting started
```
git clone https://gitlab.engr.illinois.edu/kc68/SDFS.git
cd SDFS
go run main.go
# a node will join the system automatically, if you want to leave, type "leave"
```