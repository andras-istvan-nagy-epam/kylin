/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.stream.core.dict;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.kylin.stream.core.dict.StreamingDictionaryClient.ID_FOR_EMPTY_STR;
import static org.apache.kylin.stream.core.dict.StreamingDictionaryClient.ID_FOR_EXCEPTION;
import static org.apache.kylin.stream.core.dict.StreamingDictionaryClient.ID_NOT_FOUND;

/**
 * Used HBase as remote dictionary store
 */
public class RemoteDictionaryStore {
    private static Logger logger = LoggerFactory.getLogger(RemoteDictionaryStore.class);

    private final byte[] hbaseTableName;
    private final String tableName;
    private final byte[] encodeQualifierName = "encode_value".getBytes(StandardCharsets.UTF_8);
    private final byte[] tsQualifierName = "ts".getBytes(StandardCharsets.UTF_8);
    private Table table;

    public RemoteDictionaryStore(String cubeName) {
        hbaseTableName = cubeName.getBytes(StandardCharsets.UTF_8);
        tableName = cubeName;
    }

    public void init(String[] cfs) throws Exception {
        logger.debug("Checking streaming remote store for {} at {}.", tableName, String.join(", ", cfs));
        Connection conn = getConnection();
        Admin admin = conn.getAdmin();
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(hbaseTableName));
        for (String family : cfs) {
            HColumnDescriptor fd = new HColumnDescriptor(family);
            desc.addFamily(fd);
        }
        DistributedLock lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory().lockForCurrentProcess();
        try {
            boolean locked = lock.lock(lockPath());
            if (locked && !admin.tableExists(TableName.valueOf(hbaseTableName))) {
                admin.createTable(desc);
            }
        } finally {
            admin.close();
            if (lock != null && lock.isLockedByMe(lockPath())) {
                lock.unlock(lockPath());
            }
        }

        table = conn.getTable(TableName.valueOf(hbaseTableName));
    }

    /**
     * When checkPrevious set to true, check wether row which rowkey is *rowkeyStr* exists and its value equals to *expectedValue*,
     *  otherwise only check if row which rowkey is *rowkeyStr* is exists.
     * If check succeed, set *expectedValue* to specific row and return *expectedValue*; otherwise, return ID_NOT_FOUND.
     *
     * @param columnFamily Column Family of HBase
     * @param rowkeyStr rowkey in string re
     * @param expectedValue expectedValue integer
     */
    public int checkAndPut(ByteArray columnFamily, String rowkeyStr, int expectedValue, int putValue,
            boolean checkPrevious) {
        try {
            byte[] rowkey = rowkeyStr.getBytes(StandardCharsets.UTF_8);
            if (rowkey.length == 0) {
                return ID_FOR_EMPTY_STR;
            }
            byte[] valueByte = Integer.toString(putValue).getBytes(StandardCharsets.UTF_8);
            Put put = new Put(rowkey);
            put.addColumn(columnFamily.array(), encodeQualifierName, valueByte);
            put.addColumn(columnFamily.array(), tsQualifierName, Bytes.toBytes(System.currentTimeMillis()));
            boolean hasPut = table.checkAndPut(rowkey, columnFamily.array(), encodeQualifierName,
                    checkPrevious ? Integer.toString(expectedValue).getBytes(StandardCharsets.UTF_8) : null, put);

            if (hasPut) {
                return putValue;
            } else {
                return ID_NOT_FOUND;
            }
        } catch (Exception e) {
            logger.error("CheckAndPut failed at " + rowkeyStr + ", columnFamily "
                    + new String(columnFamily.array(), StandardCharsets.UTF_8), e);
            return ID_FOR_EXCEPTION;
        }
    }

    /**
     * Get encode integer from remote dictionary store.
     */
    public int encode(ByteArray column, String rowkeyStr) {
        try {
            byte[] rowkey = rowkeyStr.getBytes(StandardCharsets.UTF_8);
            if (rowkey.length == 0) {
                return ID_FOR_EMPTY_STR;
            }
            Get get = new Get(rowkey);
            Result res = table.get(get);
            byte[] resBytes = res.getValue(column.array(), encodeQualifierName);
            byte[] tsBytes = res.getValue(column.array(), tsQualifierName);
            String realId = new String(resBytes, StandardCharsets.UTF_8);
            String ts = new String(tsBytes, StandardCharsets.UTF_8);
            logger.debug("Encode {} to {} [{}]", rowkeyStr, realId, ts);
            return Integer.parseInt(realId);
        } catch (Exception ioe) {
            logger.error("Get encode rowkey failed at " + rowkeyStr + ", column "
                    + new String(column.array(), StandardCharsets.UTF_8), ioe);
        }
        return ID_FOR_EMPTY_STR;
    }

    public static Connection getConnection() {
        Configuration conf = HBaseConfiguration.create(HadoopUtil.getCurrentConfiguration());
        try {
            return ConnectionFactory.createConnection(conf);
        } catch (IOException ioe) {
            throw new IllegalStateException("Cannot connect to HBase.", ioe);
        }
    }

    private String lockPath() {
        return "/realtime/create_global_dict_table/" + tableName;
    }
}
