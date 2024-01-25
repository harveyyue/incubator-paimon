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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * Implementation of {@link KafkaRecordDeserializationSchema} for the deserialization of Kafka
 * Debezium format records.
 */
public class CdcDebeziumAvroDeserializationSchema
        implements KafkaRecordDeserializationSchema<CdcSourceRecord> {

    public static final int SCHEMA_REGISTRY_CACHE_CAPACITY = 1000;
    private final String schemaRegistryUrl;
    private transient Deserializer deserializer;

    public CdcDebeziumAvroDeserializationSchema(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        deserializer = new Deserializer(schemaRegistryUrl);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CdcSourceRecord> out)
            throws IOException {
        String topic = record.topic();
        GenericContainerWithVersion keyContainerWithVersion =
                deserializer.deserialize(topic, true, record.key());
        GenericContainerWithVersion valueContainerWithVersion =
                deserializer.deserialize(topic, false, record.value());
        out.collect(new CdcSourceRecord(topic, keyContainerWithVersion, valueContainerWithVersion));
    }

    @Override
    public TypeInformation<CdcSourceRecord> getProducedType() {
        return getForClass(CdcSourceRecord.class);
    }

    /** An implementation of {@link AbstractKafkaAvroDeserializer}. */
    public static class Deserializer extends AbstractKafkaAvroDeserializer {

        public Deserializer(String schemaRegistryUrl) {
            this.schemaRegistry =
                    new CachedSchemaRegistryClient(
                            schemaRegistryUrl, SCHEMA_REGISTRY_CACHE_CAPACITY);
        }

        public GenericContainerWithVersion deserialize(
                String topic, boolean isKey, byte[] payload) {
            return deserializeWithSchemaAndVersion(topic, isKey, payload);
        }
    }
}
