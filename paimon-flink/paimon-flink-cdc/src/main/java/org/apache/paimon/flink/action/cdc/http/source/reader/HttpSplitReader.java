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

package org.apache.paimon.flink.action.cdc.http.source.reader;

import org.apache.paimon.flink.action.cdc.http.source.HttpSourceConfig;
import org.apache.paimon.flink.action.cdc.http.source.reader.strategy.HttpExecuteStrategy;
import org.apache.paimon.flink.action.cdc.http.source.split.HttpSplit;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** A {@link SplitReader} implementation that reads records from Http request. */
public class HttpSplitReader implements SplitReader<HttpRecord, HttpSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSplitReader.class);
    private final HttpExecuteStrategy httpExecuteStrategy;
    private final BlockingQueue<HttpRecord> queue = new LinkedBlockingDeque<>();
    private final transient ScheduledExecutorService httpExecutor;
    private List<HttpSplit> splits;

    public HttpSplitReader(HttpSourceConfig httpSourceConfig) {
        this.httpExecuteStrategy = httpSourceConfig.getHttpExecuteStrategy();
        this.httpExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("flink-executor-http-pool"));
        LOG.info("Schedule http task.");
        httpExecuteStrategy.open();
        httpExecutor.scheduleWithFixedDelay(
                () ->
                        splits.forEach(
                                split -> {
                                    try {
                                        HttpRecord httpRecord = httpExecuteStrategy.execute(split);
                                        if (httpRecord != null) {
                                            queue.put(httpRecord);
                                        }
                                    } catch (InterruptedException ex) {
                                        throw new RuntimeException("Unexpect failed:", ex);
                                    }
                                }),
                httpSourceConfig.getHttpExecuteInitialSeconds(),
                httpSourceConfig.getHttpExecuteIntervalSeconds(),
                TimeUnit.SECONDS);
    }

    @Override
    public RecordsWithSplitIds<HttpRecord> fetch() throws IOException {
        HttpRecord httpRecord = queue.poll();
        HttpSplitRecords httpSplitRecords;
        if (httpRecord != null) {
            httpSplitRecords = new HttpSplitRecords(httpRecord);
        } else {
            httpSplitRecords = new HttpSplitRecords();
        }
        return httpSplitRecords;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<HttpSplit> splitsChanges) {
        splits = splitsChanges.splits();
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (httpExecutor != null) {
            LOG.info("Shutdown schedule thread.");
            httpExecutor.shutdown();
        }
    }

    // ---------------- private helper class ------------------------

    private static class HttpSplitRecords implements RecordsWithSplitIds<HttpRecord> {

        private final Set<String> finishedSplits = new HashSet<>();
        private String splitId;
        private HttpRecord record;

        private HttpSplitRecords() {
            this(null);
        }

        private HttpSplitRecords(HttpRecord record) {
            this.splitId = record == null ? null : record.getHttpUrl();
            this.record = record;
        }

        @Nullable
        @Override
        public String nextSplit() {
            String nextSplit = splitId;
            if (splitId != null) {
                splitId = null;
            }
            return nextSplit;
        }

        @Nullable
        @Override
        public HttpRecord nextRecordFromSplit() {
            HttpRecord result = record;
            if (record != null) {
                record = null;
            }
            return result;
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }
}
