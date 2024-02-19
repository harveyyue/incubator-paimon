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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.flink.action.cdc.http.source.reader.HttpRecord;
import org.apache.paimon.flink.action.cdc.http.source.reader.deserializer.HttpRecordDeserializationSchema;
import org.apache.paimon.flink.action.cdc.http.source.reader.deserializer.PrometheusHttpRecord;
import org.apache.paimon.flink.action.cdc.http.source.util.HttpUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/** The {@link CdcSourceRecord} type implementation for {@link HttpRecordDeserializationSchema}. */
public class CdcPrometheusDeserializationSchema
        implements HttpRecordDeserializationSchema<CdcSourceRecord> {

    @Override
    public void deserialize(HttpRecord record, Collector<CdcSourceRecord> out) throws IOException {
        List<PrometheusHttpRecord> records = HttpUtils.parse(record);
        for (PrometheusHttpRecord prometheusHttpRecord : records) {
            out.collect(new CdcSourceRecord(prometheusHttpRecord));
        }
    }

    @Override
    public TypeInformation<CdcSourceRecord> getProducedType() {
        return getForClass(CdcSourceRecord.class);
    }
}
