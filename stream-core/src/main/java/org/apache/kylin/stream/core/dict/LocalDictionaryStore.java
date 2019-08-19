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

import com.google.common.base.Preconditions;
import org.apache.kylin.common.util.ByteArray;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kylin.stream.core.dict.StreamingDictionaryClient.ID_FOR_EXCEPTION;
import static org.apache.kylin.stream.core.dict.StreamingDictionaryClient.ID_NOT_FOUND;

public class LocalDictionaryStore {
    private static Logger logger = LoggerFactory.getLogger(LocalDictionaryStore.class);
    private RocksDB db;
    private File dictPath;
    String baseStorePath = "DictCache";
    private Map<ByteArray, ColumnFamilyHandle> columnFamilyHandleMap = new HashMap<>();
    String cubeName;

    public LocalDictionaryStore(String tableColumn) {
        this.dictPath = new File(baseStorePath, tableColumn);
        this.cubeName = tableColumn;
        if (!dictPath.exists()) {
            dictPath.mkdirs();
        }
    }

    public void init(String[] cfs) throws Exception {
        logger.debug("Checking streaming dict local store for {} at {}.", cubeName, String.join(", ", cfs));

        try (DBOptions options = new DBOptions().setCreateIfMissing(true)
                .setWritableFileMaxBufferSize(400 * SizeUnit.KB).setCreateMissingColumnFamilies(true)) {
            String dataPath = dictPath.getAbsolutePath() + "/data";
            List<ColumnFamilyDescriptor> columnFamilyDescriptorList = new ArrayList<>();
            List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
            for (String family : cfs) {
                ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
                        family.getBytes(StandardCharsets.UTF_8));
                columnFamilyDescriptorList.add(columnFamilyDescriptor);
            }
            db = RocksDB.open(options, dataPath, columnFamilyDescriptorList, columnFamilyHandleList);// db should not be null
            Preconditions.checkNotNull(db, "RocksDB cannot created for some reasons.");
            for (int i = 0; i < columnFamilyHandleList.size(); i++) {
                columnFamilyHandleMap.put(new ByteArray(cfs[i].getBytes(StandardCharsets.UTF_8)),
                        columnFamilyHandleList.get(i));
            }
        } catch (Exception e) {
            logger.error("Init rocks db failed.", e);
            throw e;
        }
    }

    public boolean put(ByteArray column, String key, Integer value) {
        try {
            ColumnFamilyHandle handle = columnFamilyHandleMap.get(column);
            Preconditions.checkNotNull(handle,
                    new String(column.array(), StandardCharsets.UTF_8) + " cannot find matched handle.");
            db.put(handle, key.getBytes(StandardCharsets.UTF_8), value.toString().getBytes(StandardCharsets.UTF_8));
            return true;
        } catch (Exception rdbe) {
            logger.error("Put failed.", rdbe);
            return false;
        }
    }

    public int encode(ByteArray column, String value) {
        byte[] values;
        try {
            ColumnFamilyHandle handle = columnFamilyHandleMap.get(column);
            Preconditions.checkNotNull(handle,
                    new String(column.array(), StandardCharsets.UTF_8) + " cannot find matched handle.");
            values = db.get(handle, value.getBytes(StandardCharsets.UTF_8));
        } catch (Exception rdbe) {
            logger.error("Can not get from rocksDB.", rdbe);
            return ID_FOR_EXCEPTION;
        }
        if (values != null) {
            if (values.length == 0) {
                logger.warn("Empty values for {}", value);
                return ID_NOT_FOUND;
            } else {
                try {
                    return Integer.parseInt(new String(values, StandardCharsets.UTF_8));
                } catch (Exception e) {
                    logger.error("parseInt " + new ByteArray(values).toString(), e);
                    return ID_FOR_EXCEPTION;
                }
            }
        }
        return ID_NOT_FOUND;
    }
}
