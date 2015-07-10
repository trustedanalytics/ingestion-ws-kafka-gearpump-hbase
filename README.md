# ingestion-ws-kafka-gearpump-hbase

GearPump is inspired by recent advances in the Akka framework and a desire to improve on existing streaming frameworks. GearPump draws from a number of existing frameworks including MillWheel, Apache Storm, Spark Streaming, Apache Samza, Apache Tez and Hadoop YARN while leveraging Akka actors throughout its architecture.

User can submit to GearPump a computation DAG (Directed Acyclic Graph of tasks), which contains a list of nodes and edges, and each node can be parallelized to a set of tasks. GearPump will then schedule and distribute different tasks in the DAG to different machines automatically. Each task will be started as an actor, which is long running micro-service.

The example pipeline builds on ["websockets->kafka->hdfs" example](https://github.com/trustedanalytics/ingestion-ws-kafka-hdfs).

This approach is taken to emphasize that existing ingestions can be enhanced easily by dropping in GearPump computation DAG that could calculate, aggregate, filter data but also allow for parallel execution and stream forking.

![](https://github.com/trustedanalytics/ingestion-ws-kafka-gearpump-hbase/blob/master/docs/ingestion_ws2kafka2gearpump2hbase.png)

The key component of the pipeline is GearPump application (computation DAG) that:

1.	Reads from kafka topic
2.	Splits processing into two streams
3.	One stream just passes the messages to another kafka topic
4.	Second stream:
  1.	Processes the message (in this example the processing is simple string reversal) 
  2.	Persists messages to HBase

  
Processing is visualized in a dashboard that allows for tracking progress of messages, health of the app and the metrics (messages throughput, etc.):

![](https://github.com/trustedanalytics/ingestion-ws-kafka-gearpump-hbase/blob/master/docs/example-dag.png)


Please, visit [the project's wiki](https://github.com/trustedanalytics/ingestion-ws-kafka-gearpump-hbase/wiki) for more information.

