# DottedDB

A prototype of a Dynamo-style distributed key-value database, implementing
[Global Logical Clocks](https://github.com/ricardobcl/GlobalLogicalClocks) as the main causality mechanism across the system.

## Advantages

* Smaller metadata per key for tracking causality:
    * The logical clock per key will be in most cases one pair (node ID, counter);
* Correct distributed deletes, without the need for GC "tombstone" metadata;
    * In most cases a delete immediately deletes **all** metadata from the node;
    * When metadata is still keep in disk to ensure that old values don't return,
    it is latter deleted automatically via node synchronization;
* Efficient node synchronization protocol, making the expensive merkle trees unnecessary:
    * no more false positives being transferred between nodes;
    * no need to recompute the hashes for every update;
    * no need to maintain and store one merkle tree for every node in the system;
* Scalable logical clocks in case of high rate of node churn (nodes retiring / leaving):
    * Causality per key is automatically reduced to only living nodes as they are updated;

## Benchmarks

TODO

## Building

```shell
rake all
make deps/ranch/Makefile
```

#### Normal release

```shell
> rake rel
```

#### 4 node dev cluster

```shell
> rake dev
```
