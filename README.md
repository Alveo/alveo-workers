## Introduction

This repository contains the implementation of a set of "workers" to aid the ingestion of corpora into Alveo. The workers implement the producer-consumer pattern where work is either consumed off a work queue or produced by being placed on it. This pattern serves two purposes, firstly to act as a buffer for tasks that may take some time, and secondly to allow scalability through the increase or decrease in the number of wokers.

## System Overview

### Core System

![Core System](http://i.imgur.com/QIpTDDW.png)

The core of the system includes a server running the message queue, three servers running the data stores (Sesame, Solr, Postgres), and another server hosts the workers. There are four types of workers: Upload, Sesame, Solr, and Postgres. The 
Upload worker does the vast majority of the actual work: it takes messages containing JSON-LD formatted metadata off the upload queue and generates messages for the other workers and puts them on their queues. The workers named after their respective data stores are then responsible for taking messages off their queues and storing the data within in their stores.

### Greater System

![Greater System](http://i.imgur.com/QmCjMFv.png)

The greater system additionally includes the public facing Alveo server, which makes use of the data stores, and an Ingest server. There are two routes to getting jobs onto the work queue. The first is via the Ingester server, which has creates messages and places on the Upload queue (which would perhaps be more aptly named the 'Ingest' queue). It does this by reading RoboChef'd metadata stored on an RDSI mount. This is the private route for getting data into the system, and is analogous to the original ingest method.

The second route is via Alveo's public API. Instead of reading data/metadata off an RDSI mount and formatting it into JSON-LD, the user performs a request to ingest an item and provides the necessary metadata. Alveo performs the authentication/authorisation, and ultimately places the data within the request onto the Upload queue. Once the data has been received by the Upload queue, its flow into the rest of the system is the same regardless of the initial route into the system.

### Scaling Ingest

![Scaling Ingest](http://i.imgur.com/9Iqz7an.png)

This architecture affords scalability in each of its individual components. If a particular class of worker is processing slowly, more processes can launched within a Virtual Machine. If the Virtual Machine has its resource maxed out, more Virtual Machines can be launched with more Worker processes running on them. Theses can be transient in the cases where a large ingest task temporarily requires additional resources. If the data store services begin running slowly, they can be scaled into clusters.

## Worker Overview

The workers have some common functionality: they consume messages off a work queue, perform some sort of processing on this message then send the results off to server, either a data store or back on another work queue. The workers are designed with an interface for managing their lifecycle. Once they are created, the `connect` method connects to external service, `start` then causes the worker to subcribe to its work queue and begin processing messages, `stop` discontinues message processing and unsubscribes the worker, and `close` causes the worker to close its connections to external services.

### Launching Workers

Groups of workers are started via a worker launcher (see `script/launch_workers.rb` for options) which spawns a number of workers in separate processes and manages their lifecycle. These are meant to be run as daemons that endlessly await new messages to process.

### Batching

The workers can also perform operations in batches to speed up inseration into the data stores. The data stores typically accept some data before commiting this data and flushing it to disk. Batching reduces the number of disk flushes and the amount of network communication, but can consume more memory on the worker system. There are three parameters which control the batching process, `:enabled` specifies whether to use batching or not, `:size` controls the size of the batch to build before committing (number of items in Postgres, documents in Solr, or statements in Sesame), and `:timeout` specifies how long to wait in seconds before committing the batch. The `:timeout` parameter ensures that if a small number of items are left in the batch queue (i.e. smaller than what has been set for `:size`), that they will be committed instead of left to sit there indefinitely.

## Ingesters

Ingesters are a means to getting large quantiies of work onto the upload queues in the first place when going via the private route. Much like the worker launchers, Ingesters are spawned as groups of processes that divide up the work (e.g., a list of files or Trove chunks) before operating on them. Then Ingesters' task is convert any input data in a JSON-LD format that upload workers can consume.

## Procedure to ingest collection

### System check 

Run system_check.sh to check.

    [devel@alveo-app alveo-workers]$ . ./system_check.sh

    Checking HCS vLab environment
    Thu Jun 21 14:31:21 AEST 2018
    Rails env= nci
    Java Container url= http://localhost:8080/
    Web App url= https://localhost/version.json
    Attempt restart=
    Free disk space= 711M
    
    Checking ActiveMQ...
    + ActiveMQ is listening on port 8161 (status= 200)
    + ActiveMQ is listening on port 61616
    + ActiveMQ is listening on port 61613
    
    Checking the Java Container...
    + It looks like Sesame is available (status= 200)
    + It looks like Solr is available (status= 200)
    
    Checking A13g pollers...
    + It looks like the A13g pollers are running (processes= 5)
    
    Checking the web app...
    + The Web App is listening on port  (status= 200)
    
    Checking RabbitMQ...
    + RabbitMQ is listening on port 5673
    
    Done.

### Launch worker
    
    [devel@alveo-app alveo-workers]$ ruby script/launch_workers.rb -h
    Usage: launch_workers.rb [options]
    
    Specific options:
        -w (upload|solr|sesame|postgres)+,
            --workers                    Comma separated list of workers to launch (default=all)
        -d, --daemon                     Run as background daemon
        -h, --help                       Show this help message

**worker's log**

There are 3 log files:

- austalk_processed.log: contains all processed json file(s). If you want to re-ingest processed json file, just empty this log.
- ingester.log: ingester main log 
- worker.log: all workers(upload/solr/sesame/postgres) log.

Remember, while worker is running, don't remove any log file. If you wnat to empty log, use below command:

    truncate -s 0 *.log

### Launch ingester

Use below command to launch ingester:

    Usage: ingest_austalk.rb <collection> <dir>

In general, there would be a shell script to run the ingester program, e.g.,:

    [devel@alveo-app alveo-workers]$ cat ingest_CDUD.sh
    ruby script/ingest_austalk.rb austalk /local/austalk-postprocess-output/austalk/CDUD

### Process monitor

Use below command to create tunnel to monitor process:

** For nci sesame/solr/rabbitmq/activemq-web **

    ssh devel@app.alveo.edu.au -L 28080:10.0.0.11:8080 -L 28081:10.0.0.7:8080 -L 25673:10.0.0.14:15672 -L 28161:10.0.0.14:8161

- sesame: http://localhost:28080/openrdf-workbench/repositories/austalk/summary
- solr: http://localhost:28081/solr/#/hcsvlab/query
- RabbitMQ: http://localhost:25673/#/queues (guest/guest)
- ActiveMQ: http://localhost:28161/admin/queues.jsp (admin/admin)
