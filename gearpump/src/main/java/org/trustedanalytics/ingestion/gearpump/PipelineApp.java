/*
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.trustedanalytics.ingestion.gearpump;

import io.gearpump.cluster.ClusterConfigSource;
import io.gearpump.cluster.UserConfig;
import io.gearpump.cluster.client.ClientContext;
import io.gearpump.partitioner.HashPartitioner;
import io.gearpump.partitioner.Partitioner;
import io.gearpump.partitioner.ShufflePartitioner;
import io.gearpump.streaming.javaapi.Graph;
import io.gearpump.streaming.javaapi.Processor;
import io.gearpump.streaming.javaapi.StreamApplication;
import io.gearpump.streaming.kafka.KafkaSink;
import io.gearpump.streaming.kafka.KafkaSource;
import io.gearpump.streaming.kafka.KafkaStorageFactory;
import org.trustedanalytics.ingestion.gearpump.converters.ByteArray2StringTask;
import org.trustedanalytics.ingestion.gearpump.converters.String2Tuple2Task;
import org.trustedanalytics.ingestion.gearpump.processors.LogMessageTask;
import org.trustedanalytics.ingestion.gearpump.processors.ReverseStringTask;

public class PipelineApp {

    public static final String KAFKA_TOPIC_IN = "topic1";
    public static final String KAFKA_TOPIC_OUT = "topic2";
    public static final String KAFKA_SERVERS = "localhost:9092";
    public static final String ZOOKEEPER_QUORUM = "localhost:2181";
    public static final String TABLE_NAME = "pipeline";
    public static final String COLUMN_FAMILY = "message";

    public static void main(String[] args) {
        ClientContext context = ClientContext.apply();
        UserConfig appConfig = UserConfig.empty();

        int taskNumber = 1;

        // kafka source
        KafkaStorageFactory offsetStorageFactory = new KafkaStorageFactory(ZOOKEEPER_QUORUM, KAFKA_SERVERS);
        KafkaSource kafkaSource = new KafkaSource(KAFKA_TOPIC_IN, ZOOKEEPER_QUORUM, offsetStorageFactory);
        Processor sourceProcessor = Processor.source(kafkaSource, taskNumber, "kafkaSource", appConfig, context.system());

        // converter (converts byte[] message to String -- kafka produces byte[]
        Processor convert2string = new Processor(ByteArray2StringTask.class, taskNumber, "converter1", null);

        // converter (converts String message to byte[])
        Processor convert2tuple = new Processor(String2Tuple2Task.class, taskNumber, "converter2", null);

        // reverser
        Processor reverse  = new Processor(ReverseStringTask.class, taskNumber, "converter", null);

        // forwarder (you know, for forwarding)
        Processor forward = new Processor(LogMessageTask.class, taskNumber, "forwarder", null);

        // kafka sink (another kafka topic)
        KafkaSink kafkaSink = new KafkaSink(KAFKA_TOPIC_OUT, KAFKA_SERVERS);
        Processor kafkaSinkProcessor = Processor.sink(kafkaSink, taskNumber, "kafkaSink", appConfig, context.system());

        // hbase sink
        UserConfig config = UserConfig.empty()
            .withString(HBaseSinkTask.ZOOKEEPER_QUORUM, ZOOKEEPER_QUORUM)
            .withString(HBaseSinkTask.TABLE_NAME, TABLE_NAME)
            .withString(HBaseSinkTask.COLUMN_FAMILY, COLUMN_FAMILY);
        Processor hbaseSinkProcessor = new Processor(HBaseSinkTask.class, taskNumber, "hbaseSink", config);

        new ClusterConfigSource.ClusterConfigSourceImpl(null);
        Graph graph = new Graph();
        graph.addVertex(sourceProcessor);
        graph.addVertex(convert2string);
        graph.addVertex(forward);
        graph.addVertex(convert2tuple);
        graph.addVertex(reverse);
        graph.addVertex(kafkaSinkProcessor);

        Partitioner partitioner = new HashPartitioner();
        Partitioner shufflePartitioner = new ShufflePartitioner();

        graph.addEdge(sourceProcessor, shufflePartitioner, convert2string);
        graph.addEdge(convert2string, partitioner, forward);
        graph.addEdge(forward, partitioner, convert2tuple);
        graph.addEdge(forward, partitioner, reverse);

        graph.addEdge(reverse, partitioner, hbaseSinkProcessor);
        graph.addEdge(convert2tuple, partitioner, kafkaSinkProcessor);

        // submit
        StreamApplication app = new StreamApplication("kafka2hbase", appConfig, graph);
        context.submit(app);

        // clean resource
        context.close();
    }
}
