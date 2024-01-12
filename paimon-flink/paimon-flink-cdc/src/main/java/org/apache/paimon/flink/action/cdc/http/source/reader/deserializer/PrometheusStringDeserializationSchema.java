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

package org.apache.paimon.flink.action.cdc.http.source.reader.deserializer;

import org.apache.paimon.flink.action.cdc.http.source.reader.HttpRecord;
import org.apache.paimon.flink.action.cdc.http.source.util.HttpUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;

/** The String type implementation for {@link HttpRecordDeserializationSchema}. */
public class PrometheusStringDeserializationSchema
        implements HttpRecordDeserializationSchema<String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void deserialize(HttpRecord record, Collector<String> out) throws IOException {
        List<PrometheusHttpRecord> records = HttpUtils.parse(record);
        for (PrometheusHttpRecord prometheusHttpRecord : records) {
            out.collect(objectMapper.writeValueAsString(prometheusHttpRecord));
        }
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
