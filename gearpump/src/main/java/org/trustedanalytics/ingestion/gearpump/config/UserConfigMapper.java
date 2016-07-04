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

package org.trustedanalytics.ingestion.gearpump.config;

import com.typesafe.config.Config;
import io.gearpump.cluster.UserConfig;
import java.util.Objects;

public class UserConfigMapper {

    public static UserConfig toUserConfig(Config appConfig) {
        String authMethod = appConfig.getConfigList("tap.hbase").get(0).getString("credentials.HADOOP_CONFIG_KEY.\"hbase.security.authentication\"");

        if (Objects.equals(authMethod, "kerberos")) {
            return withKrb(appConfig);
        }
        return noKrb(appConfig);
    }

    private static UserConfig withKrb(Config appConfig) {
        UserConfig userConfig = UserConfig.empty()
                .withBoolean("IS_KRB", true)
                .withString("KAFKA_TOPIC_IN", appConfig.getString("tap.usersArgs.inputTopic"))
                .withString("KAFKA_TOPIC_OUT", appConfig.getString("tap.usersArgs.outputTopic"))
                .withString("KAFKA_SERVERS", appConfig.getConfigList("tap.kafka").get(0).getString("credentials.uri"))
                .withString("KAFKA_ZOOKEEPER_QUORUM", appConfig.getConfigList("tap.kafka").get(0).getString("credentials.zookeeperUri"))

                .withString("hbase.zookeeper.quorum", appConfig.getConfigList("tap.hbase").get(0).getString("credentials.HADOOP_CONFIG_KEY.\"hbase.zookeeper.quorum\""))
                .withString("hbase.table.name", appConfig.getString("tap.usersArgs.tableName"))
                .withString("hbase.table.column.family", appConfig.getString("tap.usersArgs.columnFamily"))
                .withString("hbase.table.column.name", appConfig.getString("tap.usersArgs.columnName"))
                .withString("hbase.user", appConfig.getString("tap.usersArgs.hbaseUser"))
                .withString("hbase.security.authentication", appConfig.getConfigList("tap.hbase").get(0).getString("credentials.HADOOP_CONFIG_KEY.\"hbase.security.authentication\""))

                .withString("hbase.krb.user", appConfig.getString("tap.usersArgs.krbUser"))
                .withString("hbase.krb.password", appConfig.getString("tap.usersArgs.krbPasswd"))
                .withString("hbase.krb.realm", appConfig.getConfigList("tap.hbase").get(0).getString("credentials.kerberos.\"krealm\""))
                .withString("hbase.krb.kdc", appConfig.getConfigList("tap.hbase").get(0).getString("credentials.kerberos.\"kdc\""))
                .withString("hbase.master.kerberos.principal", appConfig.getConfigList("tap.hbase").get(0).getString("credentials.HADOOP_CONFIG_KEY.\"hbase.master.kerberos.principal\""))
                .withString("hbase.regionserver.kerberos.principal", appConfig.getConfigList("tap.hbase").get(0).getString("credentials.HADOOP_CONFIG_KEY.\"hbase.regionserver.kerberos.principal\""));

        return userConfig;
    }

    private static UserConfig noKrb(Config appConfig) {
        UserConfig userConfig = UserConfig.empty()
                .withBoolean("IS_KRB", false)
                .withString("KAFKA_TOPIC_IN", appConfig.getString("tap.usersArgs.inputTopic"))
                .withString("KAFKA_TOPIC_OUT", appConfig.getString("tap.usersArgs.outputTopic"))
                .withString("KAFKA_SERVERS", appConfig.getConfigList("tap.kafka").get(0).getString("credentials.uri"))
                .withString("KAFKA_ZOOKEEPER_QUORUM", appConfig.getConfigList("tap.kafka").get(0).getString("credentials.zookeeperUri"))

                .withString("hbase.zookeeper.quorum", appConfig.getConfigList("tap.hbase").get(0).getString("credentials.HADOOP_CONFIG_KEY.\"hbase.zookeeper.quorum\""))
                .withString("hbase.table.name", appConfig.getString("tap.usersArgs.tableName"))
                .withString("hbase.table.column.family", appConfig.getString("tap.usersArgs.columnFamily"))
                .withString("hbase.table.column.name", appConfig.getString("tap.usersArgs.columnName"))
                .withString("hbase.user", appConfig.getString("tap.usersArgs.hbaseUser"));

        return userConfig;
    }
}
