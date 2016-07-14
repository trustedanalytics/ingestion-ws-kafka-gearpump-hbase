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

package org.trustedanalytics.ingestion.gearpump.hbase;

import io.gearpump.cluster.UserConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManager;
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory;


import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.io.IOException;

public final class HBaseConnManager {

    private final Configuration hbaseConfiguration;
    private final UserConfig userConfig;
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConnManager.class);

    private HBaseConnManager(UserConfig userConfig) {
        this.hbaseConfiguration = createHbaseConfiguration(userConfig);
        this.userConfig = userConfig;
    }

    public static HBaseConnManager newInstance(UserConfig userConfig) {
        return new HBaseConnManager(userConfig);
    }

    public Connection create() throws IOException {
        if (userConfig.getBoolean("IS_KRB").get().equals(true)) {
            LOGGER.info("Creating hbase connection with kerberos auth");
            try {
                KrbLoginManager loginManager = KrbLoginManagerFactory.getInstance()
                        .getKrbLoginManagerInstance(userConfig.getString("hbase.krb.kdc").get(), userConfig.getString("hbase.krb.realm").get());
                Subject subject = loginManager.loginWithCredentials(userConfig.getString("hbase.krb.user").get(), userConfig.getString("hbase.krb.password").get().toCharArray());
                loginManager.loginInHadoop(subject, hbaseConfiguration);
                LOGGER.info("Creating hbase connection with kerberos auth - end");
                return ConnectionFactory.createConnection(hbaseConfiguration, getUserFromSubject(subject));
            } catch (LoginException e) {
                LOGGER.error("Create hbase connection failed. Unable to authorize with kerberos credentials: "
                        + "user - {}, realm - {}, kdc - {}", userConfig.getString("hbase.krb.user").get(), userConfig.getString("hbase.krb.kdc").get(),
                        userConfig.getString("hbase.krb.realm").get());
                throw new IOException(e);
            }
        } else {
            return ConnectionFactory.createConnection(hbaseConfiguration, getNoKrbUserFromSubject(hbaseConfiguration, userConfig.getString("hbase.krb.user").get()));
        }

    }

    private User getUserFromSubject(Subject subject) throws IOException {
        return UserProvider.instantiate(hbaseConfiguration)
                .create(UserGroupInformation.getUGIFromSubject(subject));
    }

    private User getNoKrbUserFromSubject(Configuration configuration, String krbUser) throws IOException {
        return UserProvider.instantiate(configuration)
                .create(UserGroupInformation.createRemoteUser(krbUser));
    }

    private static Configuration createHbaseConfiguration(UserConfig userConfig) {
        Configuration hbaseConfig = HBaseConfiguration.create();

        if (userConfig.getBoolean("IS_KRB").get().equals(true)) {
            hbaseConfig.set("hbase.security.authentication", "kerberos");
            hbaseConfig.set("hbase.master.kerberos.principal", userConfig.getString("hbase.master.kerberos.principal").get());
            hbaseConfig.set("hbase.regionserver.kerberos.principal", userConfig.getString("hbase.regionserver.kerberos.principal").get());
        }
        hbaseConfig.set("hbase.zookeeper.quorum", userConfig.getString("hbase.zookeeper.quorum").get());

        return hbaseConfig;
    }
}