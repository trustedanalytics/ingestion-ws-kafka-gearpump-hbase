package org.trustedanalytics.ingestion.gearpump.hbase;

/*
 * Copyright (c) 2016 Intel Corporation
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

import io.gearpump.Message;
import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.task.TaskContext;
import org.apache.commons.lang.RandomStringUtils;
import io.gearpump.streaming.sink.DataSink;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseSink implements DataSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSink.class);

    private UserConfig userConfig;
    private transient Connection connection;
    private transient Table table;

    public HBaseSink(UserConfig userConfig) throws IOException {
        this.userConfig = userConfig;
    }
    @Override
    public void open(TaskContext context) {
        try {
            if (connection == null || connection.isClosed()) {
                this.connection = HBaseConnManager.newInstance(this.userConfig).create();
                this.table = connection.getTable(TableName.valueOf(this.userConfig.getString("hbase.table.name").get()));
            }
        } catch (Exception e) {
            LOGGER.error("HBASE SINK OPEN EXCEPTION ....", e);
            throw new RuntimeException("HBase connection error occurred", e);
        }
    }

    @Override
    public void write(Message message) {
        String key = String.format("%d:%s", System.currentTimeMillis(), RandomStringUtils.randomAlphanumeric(8));
        String msg = message.msg().toString();

        byte[] value = Bytes.toBytes(msg);
        byte[] rowKey = Bytes.toBytes(key);
        byte[] columnFamily = Bytes.toBytes(this.userConfig.getString("hbase.table.column.family").get());
        byte[] columnName = Bytes.toBytes(this.userConfig.getString("hbase.table.column.name").get());

        Put put = new Put(rowKey);
        put.addColumn(columnFamily, columnName, value);
        try {
            table.put(put);
        } catch (Exception e) {
            LOGGER.error("HBASE SINK WRITE EXCEPTION ....", e);
            throw new RuntimeException("HBase write error occurred", e);
        }
    }

    @Override
    public void close() {
        try {
            table.close();
            connection.close();
        } catch (IOException e) {
            LOGGER.error("HBASE SINK CLOSE EXCEPTION ....", e);
            throw new RuntimeException("HBase close error occurred", e);
        }

    }
}