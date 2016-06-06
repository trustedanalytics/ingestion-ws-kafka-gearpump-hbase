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

import com.typesafe.config.Config;
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
import io.gearpump.cluster.ClusterConfig;
import org.trustedanalytics.ingestion.gearpump.converters.ByteArray2StringTask;
import org.trustedanalytics.ingestion.gearpump.converters.String2Tuple2Task;
import org.trustedanalytics.ingestion.gearpump.processors.LogMessageTask;
import org.trustedanalytics.ingestion.gearpump.processors.ReverseStringTask;

public class PipelineApp {

    private static String KAFKA_TOPIC_IN;
    private static String KAFKA_TOPIC_OUT;
    private static String KAFKA_SERVERS;
    private static String ZOOKEEPER_QUORUM;
    private static String KAFKA_ZOOKEEPER_QUORUM;
    private static String TABLE_NAME;
    private static String COLUMN_FAMILY;

    public static void main(String[] args) {
        main(ClusterConfig.defaultConfig(), args);
    }

    public static void main(Config akkaConf, String[] args) {
        ClientContext context = ClientContext.apply();
        UserConfig appConfig = UserConfig.empty();

        try {
            extractParameters(akkaConf);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println("Check input parameters.");
            throw new IllegalArgumentException("Check input parameters.", e);
        }

        int taskNumber = 1;

        // kafka source
        KafkaStorageFactory offsetStorageFactory = new KafkaStorageFactory(KAFKA_ZOOKEEPER_QUORUM, KAFKA_SERVERS);
        KafkaSource kafkaSource = new KafkaSource(KAFKA_TOPIC_IN, KAFKA_ZOOKEEPER_QUORUM, offsetStorageFactory);
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

    private static void extractParameters(Config akkaConf) {
        KAFKA_TOPIC_IN = akkaConf.getString("tap.usersArgs.inputTopic");
        KAFKA_TOPIC_OUT = akkaConf.getString("tap.usersArgs.outputTopic");
        KAFKA_SERVERS = akkaConf.getConfigList("tap.kafka").get(0).getString("credentials.uri");
        ZOOKEEPER_QUORUM = akkaConf.getConfigList("tap.hbase").get(0).getString("credentials.HADOOP_CONFIG_KEY.\"hbase.zookeeper.quorum\"");
        KAFKA_ZOOKEEPER_QUORUM = akkaConf.getConfigList("tap.kafka").get(0).getString("credentials.zookeeperUri");
        TABLE_NAME = akkaConf.getString("tap.usersArgs.tableName");
        COLUMN_FAMILY = akkaConf.getString("tap.usersArgs.columnFamily");

        System.out.println("KAFKA_TOPIC_IN: " + KAFKA_TOPIC_IN);
        System.out.println("KAFKA_TOPIC_OUT: " + KAFKA_TOPIC_OUT);
        System.out.println("KAFKA_SERVERS: " + KAFKA_SERVERS);
        System.out.println("ZOOKEEPER_QUORUM: " + ZOOKEEPER_QUORUM);
        System.out.println("KAFKA_ZOOKEEPER_QUORUM: " + KAFKA_ZOOKEEPER_QUORUM);
        System.out.println("TABLE_NAME: " + TABLE_NAME);
        System.out.println("COLUMN_FAMILY: " + COLUMN_FAMILY);
    }
}
