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

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.http.source.HttpSource;
import org.apache.paimon.flink.action.cdc.http.source.HttpSourceConfig;
import org.apache.paimon.flink.action.cdc.http.source.HttpSourceOptions;
import org.apache.paimon.flink.action.cdc.http.source.reader.deserializer.PrometheusStringDeserializationSchema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.http.source.util.HttpUtils.COMMA_PATTERN;

/** Utils for Http Action. */
public class HttpActionUtils {

    public static HttpSource<String> buildPrometheusHttpSource(Configuration httpConfig) {
        HttpSourceConfig.Builder sourceBuilder = HttpSourceConfig.builder();

        sourceBuilder.httpUrls(
                Arrays.stream(COMMA_PATTERN.split(httpConfig.get(HttpSourceOptions.HTTP_URLS)))
                        .collect(Collectors.toSet()));

        httpConfig
                .getOptional(HttpSourceOptions.HTTP_EXECUTE_MODE)
                .ifPresent(sourceBuilder::httpExecuteMode);
        httpConfig
                .getOptional(HttpSourceOptions.INCLUDE_METRICS_REGEX)
                .ifPresent(sourceBuilder::includeMetricsRegex);
        httpConfig
                .getOptional(HttpSourceOptions.HTTP_EXECUTE_INITIAL_SECONDS)
                .ifPresent(sourceBuilder::httpExecuteInitialSeconds);
        httpConfig
                .getOptional(HttpSourceOptions.HTTP_EXECUTE_INTERVAL_SECONDS)
                .ifPresent(sourceBuilder::httpExecuteIntervalSeconds);

        return new HttpSource<>(sourceBuilder.build(), new PrometheusStringDeserializationSchema());
    }

    public static LinkedHashMap<String, DataType> commonDataTypes(
            List<ComputedColumn> computedColumns, boolean caseSensitive) {
        LinkedHashMap<String, DataType> dataTypes = new LinkedHashMap<>();
        dataTypes.put("http_url", DataTypes.STRING());
        dataTypes.put("timestamp", DataTypes.BIGINT());
        dataTypes.put("metric_name", DataTypes.STRING());
        dataTypes.put("value", DataTypes.STRING());
        computedColumns.forEach(
                computedColumn ->
                        dataTypes.put(
                                keyCaseConvert(computedColumn.columnName(), caseSensitive),
                                computedColumn.columnType()));
        return dataTypes;
    }

    public static String keyCaseConvert(String key, boolean caseSensitive) {
        if (key == null) {
            return null;
        }
        return caseSensitive ? key : key.toLowerCase();
    }
}
