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

import io.gearpump.Message;
import io.gearpump.cluster.UserConfig;
import io.gearpump.external.hbase.HBaseSink;
import io.gearpump.streaming.task.StartTime;
import io.gearpump.streaming.task.Task;
import io.gearpump.streaming.task.TaskContext;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;

public class HBaseSinkTask extends Task {

    public static String TABLE_NAME = "hbase.table.name";
    public static String COLUMN_FAMILY = "hbase.table.column.family";
    public static String COLUMN_NAME = "hbase.table.column.name";
    public static String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static String HBASE_USER = "hbase.user";


    private HBaseSink sink;
    private String columnFamily;

    private Logger LOG = super.LOG();

    public HBaseSinkTask(TaskContext taskContext, UserConfig userConf) {
        super(taskContext, userConf);

        String tableName = userConf.getString(TABLE_NAME).get();
        String zkQuorum = userConf.getString(ZOOKEEPER_QUORUM).get();

        columnFamily = userConf.getString(COLUMN_FAMILY).get();

        Configuration hbaseConf = new Configuration();
        hbaseConf.set(ZOOKEEPER_QUORUM, zkQuorum);

        sink = new HBaseSink(userConf, tableName, hbaseConf);
    }

    @Override
    public void onStart(StartTime startTime) {
        LOG.info("HBaseSinkTask.onStart startTime [" + startTime + "]");
    }

    @Override
    public void onNext(Message message) {
        LOG.info("HBaseSinkTask.onNext [" + message.msg() + "]");
        String key = String.format("%d:%s", System.currentTimeMillis(), RandomStringUtils.randomAlphanumeric(8));
        sink.insert(key, columnFamily, "message", (String) message.msg());
    }
}
