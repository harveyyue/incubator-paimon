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

import org.apache.paimon.flink.action.cdc.CdcActionCommonUtils;
import org.apache.paimon.flink.action.cdc.SyncJobHandler;
import org.apache.paimon.flink.action.cdc.SyncTableActionBase;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;

import org.apache.flink.api.common.functions.FlatMapFunction;

import java.util.Map;

import static org.apache.paimon.flink.action.cdc.http.HttpActionUtils.commonDataTypes;

/** Synchronize table from Prometheus. */
public class PrometheusSyncTableAction extends SyncTableActionBase {

    public PrometheusSyncTableAction(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> cdcSourceConfig) {
        super(
                warehouse,
                database,
                table,
                catalogConfig,
                cdcSourceConfig,
                SyncJobHandler.SourceType.HTTP);
    }

    @Override
    protected Schema buildPaimonSchema(Schema retrievedSchema) {
        return CdcActionCommonUtils.buildPaimonSchema(
                table,
                partitionKeys,
                primaryKeys,
                computedColumns,
                tableConfig,
                retrievedSchema,
                metadataConverters,
                caseSensitive,
                false);
    }

    @Override
    protected Schema retrieveSchema() throws Exception {
        Schema.Builder builder = Schema.newBuilder();
        commonDataTypes(computedColumns, caseSensitive).forEach(builder::column);
        return builder.build();
    }

    @Override
    protected FlatMapFunction<String, RichCdcMultiplexRecord> recordParse() {
        return new PrometheusRecordParser(database, table, caseSensitive, computedColumns);
    }

    @Override
    protected Object buildSource() {
        return HttpActionUtils.buildPrometheusHttpSource(cdcSourceConfig);
    }
}
