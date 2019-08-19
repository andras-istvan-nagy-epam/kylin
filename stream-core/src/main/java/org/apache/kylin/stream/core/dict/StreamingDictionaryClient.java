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

import org.apache.kylin.common.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 *  <p>
 * Used to encode string into integer for each cube, it connect both local&remote dict store.
 * One table/dir for each cube, one column family for each column in cube.
 * Not thread-safe.
 *
 * For each receiver, it will ask allocation for a range of integer for String encode. Range will be [startDictId, startDictId + DICT_ID_RANGE_LEN),
 * and startDictId + offset is currently used as dictId.
 *
 * ID_FOR_EMPTY_STR is for empty string, MIN_ID_FOR_NO_EMPTY_STR to MAX_ID_FOR_NO_EMPTY_STR is for non-empty string.
 * </p>
 */
public class StreamingDictionaryClient {

    private static Logger logger = LoggerFactory.getLogger(StreamingDictionaryClient.class);

    private static final ByteArray defaultCf = new ByteArray("default".getBytes(StandardCharsets.UTF_8));
    private static final String GLOBAL_START_ID = "GLOBAL_START_ID";
    public static final String MSG_TEMPLATE = "Dictionary Client Info:  ExceptionLocal:%9d,  ExceptionRemote:%9d, HitLocal:%9d,  MissLocal:%9d,  ALL:%9d .";

    public static final int DICT_ID_RANGE_LEN = 30000;
    public static final int ID_FOR_EXCEPTION = -1;
    public static final int ID_FOR_EMPTY_STR = 0;
    public static final int MIN_ID_FOR_NO_EMPTY_STR = 1;
    public static final int MAX_ID_FOR_NO_EMPTY_STR = Integer.MAX_VALUE - DICT_ID_RANGE_LEN - 10;
    public static final int ID_NOT_FOUND = Integer.MIN_VALUE;

    private int startDictId = -1;
    private int offset = 0;

    /** Local stats for cache hit/miss/error */
    private AtomicLong encodeCounter = new AtomicLong();
    private AtomicLong hitLocal = new AtomicLong();
    private AtomicLong missLocal = new AtomicLong();
    private AtomicLong errorLocal = new AtomicLong();
    private AtomicLong errorRemote = new AtomicLong();
    private long lastCheck = System.currentTimeMillis();
    private long lastTotalError = 0;

    /** Local Storage Layer*/
    private LocalDictionaryStore localStore;

    /** Remote Storage Layer*/
    private RemoteDictionaryStore remoteStore;

    public StreamingDictionaryClient(String cubeName, String[] columns) {
        String[] columnFamily = getCf(columns);
        try {
            localStore = new LocalDictionaryStore(cubeName);
            remoteStore = new RemoteDictionaryStore(cubeName);
            remoteStore.init(columnFamily);
            localStore.init(columnFamily);
        } catch (Exception e) {
            throw new RuntimeException("Init dictionary failed.", e);
        }
    }

    public int encode(ByteArray column, String value) {
        checkDictIdRange();
        printStat();
        int localId = localStore.encode(column, value);
        int expected = startDictId + offset;
        if (localId >= ID_FOR_EMPTY_STR) {
            hitLocal.addAndGet(1);
            return localId;
        } else {
            if (localId == ID_FOR_EXCEPTION) {
                errorLocal.addAndGet(1);
            } else {
                missLocal.addAndGet(1);
            }
            int remoteId;
            remoteId = remoteStore.checkAndPut(column, value, ID_NOT_FOUND, expected, false);
            if (remoteId == ID_FOR_EXCEPTION) {
                errorRemote.addAndGet(1);
                return MIN_ID_FOR_NO_EMPTY_STR; // should be better here
            } else if (remoteId == ID_NOT_FOUND) { // remote exists, fetch from remote
                remoteId = remoteStore.encode(column, value);
            } else { // remote not exist, advance offset
                offset++;
            }
            if (!localStore.put(column, value, remoteId)) {
                errorLocal.addAndGet(1);
            }
            return remoteId;
        }
    }

    //=====================================================================================
    //================================= Internal method ===================================

    private void checkDictIdRange() {
        // first time
        if (startDictId == -1) {
            logger.debug("Init range.");
            int res = remoteStore.checkAndPut(defaultCf, GLOBAL_START_ID, MIN_ID_FOR_NO_EMPTY_STR,
                    MIN_ID_FOR_NO_EMPTY_STR, false);
            if (res != ID_NOT_FOUND) {
                startDictId = MIN_ID_FOR_NO_EMPTY_STR;
            } else {
                startDictId = findStartId();
                logger.debug("After allcated, current startDictId is {}.", startDictId);
            }
        }

        // need to ask for another range
        if (offset >= DICT_ID_RANGE_LEN - 1) {
            logger.debug("Ask for another dictId range. Current startDictId is {}.", startDictId);
            startDictId = findStartId();
            logger.debug("After allcated, current startDictId is {}.", startDictId);
            offset = 0;
        }

        if (startDictId >= MAX_ID_FOR_NO_EMPTY_STR) {
            // do something here to fix overflow
        }
    }

    private int findStartId() {
        int finalV = ID_NOT_FOUND;
        int oldV = remoteStore.encode(defaultCf, GLOBAL_START_ID);
        boolean hasPut = false;
        while (!hasPut) {
            int res = remoteStore.checkAndPut(defaultCf, GLOBAL_START_ID, oldV, oldV + DICT_ID_RANGE_LEN, true);
            if (res == ID_NOT_FOUND) { // put failed
                oldV = remoteStore.encode(defaultCf, GLOBAL_START_ID);
            } else { // put success
                finalV = res;
                hasPut = true;
            }
        }
        return finalV;
    }

    /**
     * Create column family for each column in one cube
     */
    private String[] getCf(String[] columns) {
        String[] cfs = new String[columns.length + 1];
        cfs[0] = "default"; // RocksDB need it
        int idx = 1;
        for (String col : columns) {
            cfs[idx++] = col;
        }
        return cfs;
    }

    private void printStat() {
        long curr = encodeCounter.addAndGet(1);
        if (System.currentTimeMillis() - lastCheck >= 10000) {
            long totalError = errorRemote.get() + errorLocal.get();
            String msg = String.format(Locale.ROOT, MSG_TEMPLATE, errorLocal.get(), errorRemote.get(), hitLocal.get(),
                    missLocal.get(), curr);
            if (totalError > lastTotalError) {
                logger.warn("Exception in dict\n {}", msg);
                lastTotalError = totalError;
            } else {
                logger.info(msg);
            }
            lastCheck = System.currentTimeMillis();
        }
    }
}
