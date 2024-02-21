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
import org.apache.paimon.flink.action.cdc.http.source.reader.deserializer.PrometheusHttpRecord;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnCaseConvertAndDuplicateCheck;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnDuplicateErrMsg;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.mapKeyCaseConvert;
import static org.apache.paimon.flink.action.cdc.http.HttpActionUtils.commonDataTypes;
import static org.apache.paimon.flink.action.cdc.http.HttpActionUtils.keyCaseConvert;

/**
 * A parser for Prometheus http strings, converting them into a list of {@link
 * RichCdcMultiplexRecord}s.
 */
public class PrometheusRecordParser implements FlatMapFunction<String, RichCdcMultiplexRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusRecordParser.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final String databaseName;
    private final String tableName;
    private final boolean caseSensitive;
    private final List<ComputedColumn> computedColumns;
    private final Map<List<String>, LinkedHashMap<String, DataType>> schemaCache = new HashMap<>();

    public PrometheusRecordParser(
            String databaseName,
            String tableName,
            boolean caseSensitive,
            List<ComputedColumn> computedColumns) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.caseSensitive = false;
        this.computedColumns = computedColumns;
        objectMapper
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void flatMap(String value, Collector<RichCdcMultiplexRecord> out) throws Exception {
        PrometheusHttpRecord prometheusHttpRecord =
                objectMapper.readValue(value, PrometheusHttpRecord.class);
        List<String> labelKeys = prometheusHttpRecord.labelKeys();
        LinkedHashMap<String, DataType> dataTypes = schemaCache.get(labelKeys);
        if (dataTypes == null) {
            dataTypes = extractDataTypes(prometheusHttpRecord);
            // put paimon data types to cache
            schemaCache.put(labelKeys, dataTypes);

            out.collect(
                    new RichCdcMultiplexRecord(
                            databaseName,
                            tableName,
                            dataTypes,
                            Collections.emptyList(),
                            CdcRecord.emptyRecord()));
        }

        out.collect(
                new RichCdcMultiplexRecord(
                        databaseName,
                        tableName,
                        new LinkedHashMap<>(0),
                        Collections.emptyList(),
                        new CdcRecord(RowKind.INSERT, extractRow(prometheusHttpRecord))));
    }

    public Map<String, String> extractRow(PrometheusHttpRecord prometheusHttpRecord) {
        Map<String, String> data = new HashMap<>();
        data.put("http_url", prometheusHttpRecord.getHttpUrl());
        data.put("timestamp", String.valueOf(prometheusHttpRecord.getTimestamp()));
        data.put("metric_name", prometheusHttpRecord.getMetricName());
        data.put("value", prometheusHttpRecord.getValue());
        data.putAll(
                mapKeyCaseConvert(
                        prometheusHttpRecord.getLabels(),
                        caseSensitive,
                        columnDuplicateErrMsg(tableName)));
        // generate values of computed columns
        computedColumns.forEach(
                computedColumn ->
                        data.put(
                                keyCaseConvert(computedColumn.columnName(), caseSensitive),
                                computedColumn.eval(data.get(computedColumn.fieldReference()))));
        return data;
    }

    public LinkedHashMap<String, DataType> extractDataTypes(
            PrometheusHttpRecord prometheusHttpRecord) {
        LinkedHashMap<String, DataType> dataTypes = commonDataTypes(computedColumns, caseSensitive);
        Set<String> existedFields = new HashSet<>(dataTypes.keySet());
        Function<String, String> columnDuplicateErrMsg = columnDuplicateErrMsg(tableName);
        prometheusHttpRecord
                .labelKeys()
                .forEach(
                        label -> {
                            String columnName =
                                    columnCaseConvertAndDuplicateCheck(
                                            label,
                                            existedFields,
                                            caseSensitive,
                                            columnDuplicateErrMsg);
                            dataTypes.put(columnName, DataTypes.STRING());
                        });
        return dataTypes;
    }
}
