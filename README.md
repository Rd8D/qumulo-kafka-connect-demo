# Yet another Kafka Connect Pipeline with Qumulo Core API...

Why? To demonstrate how efficient and easy it is to integrate data from sources into targets, using QF2 as a huge buffer at scale.

## Getting Started with this demo

Prerequisites

You will need:

* Java 8, or better
* Qumulo Core 2.8.5, or better
* Imply 2.6.3, or better
* Linux, Mac OS X, or other Unix-like OS


## Pipeline overview

This demo code connects to Wikipedia IRC channels and turns them into a Kafka topic from which it can feed a distributed spool directory for persisting data using the Qumulo Core API. As a Connector component, it streams data to Qumulo for storing and caching messages at scale - just because we can!  


The source includes both a Qumulo RestApi Source component and a Qumulo RestApi Sink component to demonstrate an end-to-end data flow implemented through Kafka Connect.

The Qumulo demo Connector watches the distributed spool for appended updates. When ingesting data, a task spec is submitted to the Druid overlord to perform a batch file load via a middle manager component that provides an HTTP endpoint for data ingestion, and reads data from the distributed spool directory using the Qumulo Core API.

Data at-rest is encrypted at file level while stored in Qumulo.

![Pipeline overview](https://github.com/Rd8D/qumulo-kafka-connect-demo/blob/master/doc/pipeline-overview.png?raw=true)

![Demonstration overview](https://github.com/Rd8D/qumulo-kafka-connect-demo/blob/master/doc/demo-overview.png?raw=true)


## Getting Started

### Fire up the demo platform

Pull and compile from master branch, edit properties to your satisfaction.

In your favorite terminal, build the jar file with the './build-all.sh' command:

```
cd bin
./build-all.sh
```

If you are starting fresh and/or have no existing Kafka or ZooKeeper server available, you need to set up a multi-broker Kafka cluster as explained in this guide: https://kafka.apache.org/quickstart

Also you need to deploy one or multiple Qumulo clusters depending on your specific needs: https://try.qumulo.com.

After that, you’ll need to start up your Kafka & Qumulo clusters, and also Imply, which includes Druid (streaming analytics data store) and Pivot (data visualization app).

Start ZooKeeper on all nodes where it is installed, by issuing the following command:

```
/opt/qq/bin/zkServer.sh start 
```

Start Kafka on all nodes where it is installed:

```
JAVA_HOME=/opt/qq /opt/qq/bin/kafka-server-start.sh -daemon /opt/qq/conf/kafka.properties
```

Start Imply:

```
cd imply-2.6.3/
bin/supervise -c conf/supervise/quickstart.conf
```
 
### Stream data from Wikipedia to Qumulo through Kafka

Start all three connectors running in standalone mode:

```
cd bin
./kafka-connect-qf2-start.sh
```

Start a Wikipedia consumer, this process places the edit events in the inbox spool directory using the Qumulo Core API, where they will stay until they have been streamed out and in Kafka to topic qf2-connect-demo:

```
cd bin
./wikipedia-consumer-start.sh
```

### Stream data from Qumulo to Druid

Start the middle manager component that provides an HTTP endpoint for data ingestion, and reads data from the distributed spool directory using the Qumulo Core API:

```
cd bin
./middle-manager-start.sh
```

Write an ingestion spec that can load data from middle manager HTTP endpoint as-is: 

```
{
  "type": "index",
  "spec": {
    "dataSchema": {
      "dataSource": "Wikipedia",
      "parser": {
        "type": "string",
        "parseSpec": {
          "format": "json",
          "timestampSpec": {
            "column": "timestamp",
            "format": "iso"
          },
          "dimensionsSpec": {
            "dimensions": [
              "diffUrl",
              "isRobot",
              {
                "name": "added",
                "type": "long"
              },
              {
                "name": "delta",
                "type": "long"
              },
              "flags",
              "channel",
              "isUnpatrolled",
              "isNew",
              {
                "name": "deltaBucket",
                "type": "long"
              },
              "isMinor",
              "isAnonymous",
              {
                "name": "deleted",
                "type": "long"
              },
              "namespace",
              "comment",
              "page",
              {
                "name": "commentLength",
                "type": "long"
              },
              "user",
              "countryIsoCode",
              "countryName",
              "regionName",
              "cityName",
              "regionIsoCode"
            ]
          }
        }
      },
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "rollup": false,
        "queryGranularity": "none"
      },
      "metricsSpec": []
    },
    "ioConfig": {
      "type": "index",
      "firehose": {
        "fetchTimeout": 300000,
        "type": "http",
        "uris": [
          "http://ingest.qq.demo.local:9500/"
        ]
      },
      "appendToExisting": false
    },
    "tuningConfig": {
      "type": "index",
      "forceExtendableShardSpecs": true,
      "maxRowsInMemory": 75000,
      "reportParseExceptions": false
    }
  }
}
```

Run the following command to submit the task:

```
curl -X 'POST' -H 'Content-Type:application/json' -d @imply-2.6.3/ingestion-middle-manager-index.json http://localhost:8090/druid/indexer/v1/task
```

Because of the micro-batching model, it is recommended to schedule the re-indexing tasks to be executed periodically, which will merge data segments together as the data flows in. 

You also have the ability to load new data into Druid from Imply's home screen. 

You can now immediately begin visualizing data with Pivot at http://localhost:9095/pivot.

![Imply visuals](https://github.com/Rd8D/qumulo-kafka-connect-demo/blob/master/doc/imply-visuals.png?raw=true)

