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

package org.apache.paimon.flink.action.cdc.http;

import org.apache.paimon.flink.action.cdc.CdcActionITCaseBase;
import org.apache.paimon.flink.action.cdc.http.source.HttpSource;
import org.apache.paimon.flink.action.cdc.http.source.HttpSourceConfig;
import org.apache.paimon.flink.action.cdc.http.source.reader.deserializer.PrometheusHttpRecord;
import org.apache.paimon.flink.action.cdc.http.source.reader.deserializer.PrometheusHttpRecordDeserializationSchema;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** IT cases for {@link PrometheusSyncTableAction}. */
public class PrometheusSyncTableActionITCase extends CdcActionITCaseBase {

    @Test
    public void testPrometheusTable() throws Exception {
        Map<String, String> prometheusConfig = new HashMap<>();
        prometheusConfig.put("http-urls", "http://10.59.54.214:7072/metrics");
        prometheusConfig.put("http-execute-mode", "prometheus");
        prometheusConfig.put(
                "include-metrics-regex",
                "^debezium_metrics_queueremainingcapacity\\{context=\"streaming\".*|^debezium_metrics_millisecondsbehindsource\\{context=\"streaming\".*");

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", "-1");
        tableConfig.put("sink.parallelism", "1");
        // tableConfig.put("file.format", "parquet");

        PrometheusSyncTableAction action =
                new PrometheusSyncTableAction(
                        "file:/Users/sh00704ml/Downloads/paimon",
                        "workdb",
                        "test_debezium_metrics",
                        tableConfig,
                        prometheusConfig);

        action.withTableConfig(tableConfig);

        List<String> computedColumnArgs = new ArrayList<>();
        computedColumnArgs.add("created_time=from_unixtime(timestamp)");
        action.withComputedColumnArgs(computedColumnArgs);

        action.run();
    }

    @Test
    public void testPrometheusHttpSource() throws Exception {
        Set<String> urls = new HashSet<>();
        urls.add("http://10.59.54.214:7072/metrics");
        urls.add("http://10.59.54.214:27072/metrics");
        HttpSourceConfig httpSourceConfig =
                HttpSourceConfig.builder()
                        .httpUrls(urls)
                        .httpExecuteMode("prometheus")
                        .includeMetricsRegex(
                                "^debezium_metrics_queueremainingcapacity\\{context=\"streaming\".*|^debezium_metrics_millisecondsbehindsource\\{context=\"streaming\".*")
                        .build();

        run(httpSourceConfig);
    }

    @Test
    public void testPrometheusHttpSourceWithoutIncludeMetricsRegex() throws Exception {
        Set<String> urls = new HashSet<>();
        urls.add("http://10.59.54.214:7072/metrics");
        HttpSourceConfig httpSourceConfig =
                HttpSourceConfig.builder().httpUrls(urls).httpExecuteMode("prometheus").build();

        run(httpSourceConfig);
    }

    private void run(HttpSourceConfig httpSourceConfig) throws Exception {
        HttpSource<PrometheusHttpRecord> httpSource =
                new HttpSource<>(httpSourceConfig, new PrometheusHttpRecordDeserializationSchema());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromSource(httpSource, WatermarkStrategy.noWatermarks(), "HttpSource")
                .setParallelism(2)
                .print()
                .setParallelism(1);

        env.execute("Print Http Response");
    }
}
