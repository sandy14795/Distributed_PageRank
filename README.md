# Distributed_PageRank

# Overview

The aim of the project is to get started with Apache Spark and Hadoop File Systems.

# Implementation

For this project, we have implemented PageRank algorithm on the Wikipedia Edge Relationship Dataset. Each line in the dataset is in the form 'A\tB', interpreted as a directed edge from A to B. We tried to compute the pageranks of all nodes with non-zero incoming edges in the Wikipedia dataset.

The project consists of the following different set-ups of Pagerank on WikiDataset:

    PageRank : Normal implementation.
    PageRank_partitioned : Involves custom data partitioning involved across 3 nodes.
    PageRank_persisted_cache: Involved data partitioning and enabling in-memory caching of most used RDDs.
    PageRank_FaultTolerance : Involves killing one machine at different timelines (25% and 75% lifetime)

# Metrics

We have drawn our observations from logging following metrics during execution times

    Job completion times
    Network traffic(recv/send data) across worker nodes.
    Disk read/write bandwidth(read/writ data) across worker nodes.
