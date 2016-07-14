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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trustedanalytics.ingestion.gearpump.config.UserConfigMapper;
import org.trustedanalytics.ingestion.gearpump.hbase.HBaseSink;
import org.trustedanalytics.ingestion.gearpump.processors.ByteArray2StringTask;
import org.trustedanalytics.ingestion.gearpump.processors.String2Tuple2Task;
import org.trustedanalytics.ingestion.gearpump.processors.LogMessageTask;
import org.trustedanalytics.ingestion.gearpump.processors.ReverseStringTask;

public class PipelineApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineApp.class);

    public static void main(String[] args) throws Exception {
        Config akkaConf = ClusterConfig.defaultConfig();
        ClientContext context = ClientContext.apply();
        UserConfig userConfig;

        try {
            userConfig = UserConfigMapper.toUserConfig(akkaConf);
        } catch (Exception e) {
            LOGGER.error("Check input parameters. {}", e.getMessage());
            throw new IllegalArgumentException("Check input parameters.", e);
        }

        int taskNumber = 1;

        // kafka source
        KafkaStorageFactory offsetStorageFactory = new KafkaStorageFactory(userConfig.getString("KAFKA_ZOOKEEPER_QUORUM").get(), userConfig.getString("KAFKA_SERVERS").get());
        KafkaSource kafkaSource = new KafkaSource(userConfig.getString("KAFKA_TOPIC_IN").get(), userConfig.getString("KAFKA_ZOOKEEPER_QUORUM").get(), offsetStorageFactory);
        Processor sourceProcessor = Processor.source(kafkaSource, taskNumber, "kafkaSource", UserConfig.empty(), context.system());

        // converter (converts byte[] message to String -- kafka produces byte[]
        Processor convert2string = new Processor(ByteArray2StringTask.class, taskNumber, "converter1", null);

        // converter (converts String message to byte[])
        Processor convert2tuple = new Processor(String2Tuple2Task.class, taskNumber, "converter2", null);

        // reverser
        Processor reverse  = new Processor(ReverseStringTask.class, taskNumber, "converter", null);

        // forwarder (you know, for forwarding)
        Processor forward = new Processor(LogMessageTask.class, taskNumber, "forwarder", null);

        // kafka sink (another kafka topic)
        KafkaSink kafkaSink = new KafkaSink(userConfig.getString("KAFKA_TOPIC_OUT").get(), userConfig.getString("KAFKA_SERVERS").get());
        Processor kafkaSinkProcessor = Processor.sink(kafkaSink, taskNumber, "kafkaSink", UserConfig.empty(), context.system());

        HBaseSink hBaseSink;
        try {
            hBaseSink = new HBaseSink(userConfig);
        } catch (Exception e) {
            LOGGER.error("Exception during sink creation {}", e.getMessage());
            throw new Exception(e);
        }

        Processor hbaseSinkProcessor = Processor.sink(hBaseSink, taskNumber, "hbaseSink", UserConfig.empty(), context.system());

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
        StreamApplication app = new StreamApplication("kafka2hbase", UserConfig.empty(), graph);
        context.submit(app);

        // clean resource
        context.close();
    }


}
