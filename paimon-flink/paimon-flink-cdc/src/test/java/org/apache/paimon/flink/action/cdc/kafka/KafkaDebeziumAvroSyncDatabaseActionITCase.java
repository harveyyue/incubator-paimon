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

package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.sanitizedSchema;
import static org.apache.paimon.flink.action.cdc.kafka.KafkaActionUtils.SCHEMA_REGISTRY_URL;

/** IT cases for {@link KafkaSyncDatabaseAction}. */
public class KafkaDebeziumAvroSyncDatabaseActionITCase extends KafkaSyncDatabaseActionITCase {
    private static final String DEBEZIUM = "debezium";

    private static final String T1_KEY_SCHEMA_V1 =
            "{\"type\":\"record\",\"name\":\"Key\",\"namespace\":\"test_avro.workdb.t1\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}],\"connect.name\":\"test_avro.workdb.t1.Key\"}";
    private static final String T1_VALUE_SCHEMA_V1 =
            "{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"test_avro.workdb.t1\",\"fields\":[{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"id\"}}},{\"name\":\"name\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"255\",\"__debezium.source.column.name\":\"name\"}}],\"default\":null},{\"name\":\"description\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"255\",\"__debezium.source.column.name\":\"description\"}}],\"default\":null},{\"name\":\"weight\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DECIMAL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"weight\"}}],\"default\":null}],\"connect.name\":\"test_avro.workdb.t1.Value\"}],\"default\":null},{\"name\":\"after\",\"type\":[\"null\",\"Value\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"Source\",\"namespace\":\"io.debezium.connector.mysql\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"connector\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":\"long\"},{\"name\":\"snapshot\",\"type\":[{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"true,last,false,incremental\"},\"connect.default\":\"false\",\"connect.name\":\"io.debezium.data.Enum\"},\"null\"],\"default\":\"false\"},{\"name\":\"db\",\"type\":\"string\"},{\"name\":\"sequence\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"table\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_id\",\"type\":\"long\"},{\"name\":\"gtid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file\",\"type\":\"string\"},{\"name\":\"pos\",\"type\":\"long\"},{\"name\":\"row\",\"type\":\"int\"},{\"name\":\"thread\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"query\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"io.debezium.connector.mysql.Source\"}},{\"name\":\"op\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"transaction\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"block\",\"namespace\":\"event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"total_order\",\"type\":\"long\"},{\"name\":\"data_collection_order\",\"type\":\"long\"}],\"connect.version\":1,\"connect.name\":\"event.block\"}],\"default\":null}],\"connect.version\":1,\"connect.name\":\"test_avro.workdb.t1.Envelope\"}";
    private static final String T1_VALUE_SCHEMA_V2 =
            "{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"test_avro.workdb.t1\",\"fields\":[{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"id\"}}},{\"name\":\"name\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"255\",\"__debezium.source.column.name\":\"name\"}}],\"default\":null},{\"name\":\"description\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"255\",\"__debezium.source.column.name\":\"description\"}}],\"default\":null},{\"name\":\"weight\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DECIMAL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"weight\"}}],\"default\":null},{\"name\":\"age\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"age\"}}],\"default\":null}],\"connect.name\":\"test_avro.workdb.t1.Value\"}],\"default\":null},{\"name\":\"after\",\"type\":[\"null\",\"Value\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"Source\",\"namespace\":\"io.debezium.connector.mysql\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"connector\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":\"long\"},{\"name\":\"snapshot\",\"type\":[{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"true,last,false,incremental\"},\"connect.default\":\"false\",\"connect.name\":\"io.debezium.data.Enum\"},\"null\"],\"default\":\"false\"},{\"name\":\"db\",\"type\":\"string\"},{\"name\":\"sequence\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"table\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_id\",\"type\":\"long\"},{\"name\":\"gtid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file\",\"type\":\"string\"},{\"name\":\"pos\",\"type\":\"long\"},{\"name\":\"row\",\"type\":\"int\"},{\"name\":\"thread\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"query\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"io.debezium.connector.mysql.Source\"}},{\"name\":\"op\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"transaction\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"block\",\"namespace\":\"event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"total_order\",\"type\":\"long\"},{\"name\":\"data_collection_order\",\"type\":\"long\"}],\"connect.version\":1,\"connect.name\":\"event.block\"}],\"default\":null}],\"connect.version\":1,\"connect.name\":\"test_avro.workdb.t1.Envelope\"}";

    private static final String T2_KEY_SCHEMA_V1 =
            "{\"type\":\"record\",\"name\":\"Key\",\"namespace\":\"test_avro.workdb.t2\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}],\"connect.name\":\"test_avro.workdb.t2.Key\"}";
    private static final String T2_VALUE_SCHEMA_V1 =
            "{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"test_avro.workdb.t2\",\"fields\":[{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"id\"}}},{\"name\":\"name\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"255\",\"__debezium.source.column.name\":\"name\"}}],\"default\":null},{\"name\":\"description\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"255\",\"__debezium.source.column.name\":\"description\"}}],\"default\":null},{\"name\":\"weight\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DECIMAL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"weight\"}}],\"default\":null}],\"connect.name\":\"test_avro.workdb.t2.Value\"}],\"default\":null},{\"name\":\"after\",\"type\":[\"null\",\"Value\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"Source\",\"namespace\":\"io.debezium.connector.mysql\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"connector\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":\"long\"},{\"name\":\"snapshot\",\"type\":[{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"true,last,false,incremental\"},\"connect.default\":\"false\",\"connect.name\":\"io.debezium.data.Enum\"},\"null\"],\"default\":\"false\"},{\"name\":\"db\",\"type\":\"string\"},{\"name\":\"sequence\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"table\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_id\",\"type\":\"long\"},{\"name\":\"gtid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file\",\"type\":\"string\"},{\"name\":\"pos\",\"type\":\"long\"},{\"name\":\"row\",\"type\":\"int\"},{\"name\":\"thread\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"query\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"io.debezium.connector.mysql.Source\"}},{\"name\":\"op\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"transaction\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"block\",\"namespace\":\"event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"total_order\",\"type\":\"long\"},{\"name\":\"data_collection_order\",\"type\":\"long\"}],\"connect.version\":1,\"connect.name\":\"event.block\"}],\"default\":null}],\"connect.version\":1,\"connect.name\":\"test_avro.workdb.t2.Envelope\"}";
    private static final String T2_VALUE_SCHEMA_V2 =
            "{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"test_avro.workdb.t2\",\"fields\":[{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"id\"}}},{\"name\":\"name\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"255\",\"__debezium.source.column.name\":\"name\"}}],\"default\":null},{\"name\":\"description\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"255\",\"__debezium.source.column.name\":\"description\"}}],\"default\":null},{\"name\":\"weight\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DECIMAL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"weight\"}}],\"default\":null},{\"name\":\"address\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"255\",\"__debezium.source.column.name\":\"address\"}}],\"default\":null}],\"connect.name\":\"test_avro.workdb.t2.Value\"}],\"default\":null},{\"name\":\"after\",\"type\":[\"null\",\"Value\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"Source\",\"namespace\":\"io.debezium.connector.mysql\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"connector\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":\"long\"},{\"name\":\"snapshot\",\"type\":[{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"true,last,false,incremental\"},\"connect.default\":\"false\",\"connect.name\":\"io.debezium.data.Enum\"},\"null\"],\"default\":\"false\"},{\"name\":\"db\",\"type\":\"string\"},{\"name\":\"sequence\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"table\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_id\",\"type\":\"long\"},{\"name\":\"gtid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file\",\"type\":\"string\"},{\"name\":\"pos\",\"type\":\"long\"},{\"name\":\"row\",\"type\":\"int\"},{\"name\":\"thread\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"query\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"io.debezium.connector.mysql.Source\"}},{\"name\":\"op\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"transaction\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"block\",\"namespace\":\"event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"total_order\",\"type\":\"long\"},{\"name\":\"data_collection_order\",\"type\":\"long\"}],\"connect.version\":1,\"connect.name\":\"event.block\"}],\"default\":null}],\"connect.version\":1,\"connect.name\":\"test_avro.workdb.t2.Envelope\"}";

    private Schema t1KeySchema;
    private Schema t1ValueSchemaV1;
    private Schema t1ValueSchemaV2;
    private Schema t2KeySchema;
    private Schema t2ValueSchemaV1;
    private Schema t2ValueSchemaV2;
    private Schema debeziumSourceSchema;

    @BeforeEach
    public void setup() {
        super.setup();
        // Set database name from topic naming strategy or debezium's source property
        database = "workdb";
        // Init kafka key/value schema
        Parser parser = new Parser();
        t1KeySchema = parser.parse(T1_KEY_SCHEMA_V1);
        t1ValueSchemaV1 = parser.parse(T1_VALUE_SCHEMA_V1);
        t1ValueSchemaV2 = new Parser().parse(T1_VALUE_SCHEMA_V2);
        parser = new Parser();
        t2KeySchema = parser.parse(T2_KEY_SCHEMA_V1);
        t2ValueSchemaV1 = parser.parse(T2_VALUE_SCHEMA_V1);
        t2ValueSchemaV2 = new Parser().parse(T2_VALUE_SCHEMA_V2);
        debeziumSourceSchema = t1ValueSchemaV1.getField("source").schema();
    }

    @Test
    @Timeout(600)
    public void testSchemaEvolutionMultiTopic() throws Exception {
        testSchemaEvolutionMultiTopic(DEBEZIUM);
    }

    @Override
    protected void testSchemaEvolutionMultiTopic(String format) throws Exception {
        final String topic1 = "test_avro.workdb.t1";
        final String topic2 = "test_avro.workdb.t2";
        boolean writeOne = false;
        int fileCount = 2;
        List<String> topics = Arrays.asList(topic1, topic2);
        topics.forEach(topic -> createTestTopic(topic, 1, 1));

        // ---------- Write the data into Kafka -------------------

        for (int i = 0; i < fileCount; i++) {
            try {
                writeByteRecordsToKafka(
                        topics.get(i),
                        readLines(
                                String.format(
                                        "kafka/%s/database/schemaevolution/topic%s/%s-data-1.txt",
                                        format, i, format)),
                        convertFunction(1));
            } catch (Exception e) {
                throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
            }
        }

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), format + "-avro");
        kafkaConfig.put(TOPIC.key(), String.join(";", topics));
        kafkaConfig.put(SCHEMA_REGISTRY_URL.key(), getSchemaRegistryUrl());
        KafkaSyncDatabaseAction action =
                syncDatabaseActionBuilder(kafkaConfig)
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        testSchemaEvolutionImpl(topics, writeOne, fileCount, format);
    }

    private void testSchemaEvolutionImpl(
            List<String> topics, boolean writeOne, int fileCount, String format) throws Exception {
        waitingTables("t1", "t2");

        FileStoreTable table1 = getFileStoreTable("t1");
        FileStoreTable table2 = getFileStoreTable("t2");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.DECIMAL(8, 3)
                        },
                        new String[] {"id", "name", "description", "weight"});

        List<String> expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.140]",
                        "+I[102, car battery, 12V car battery, 8.100]");
        waitForResult(expected, table1, rowType1, getPrimaryKey());

        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.DECIMAL(8, 3)
                        },
                        new String[] {"id", "name", "description", "weight"});

        List<String> expected2 =
                Arrays.asList(
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.750]");
        waitForResult(expected2, table2, rowType2, getPrimaryKey());

        for (int i = 0; i < fileCount; i++) {
            try {
                writeByteRecordsToKafka(
                        writeOne ? topics.get(0) : topics.get(i),
                        readLines(
                                String.format(
                                        "kafka/%s/database/schemaevolution/topic%s/%s-data-2.txt",
                                        format, i, format)),
                        convertFunction(2));
            } catch (Exception e) {
                throw new Exception(String.format("Failed to write %s data to Kafka.", format), e);
            }
        }

        rowType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.DECIMAL(8, 3),
                            DataTypes.INT()
                        },
                        new String[] {"id", "name", "description", "weight", "age"});
        expected =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.140, NULL]",
                        "+I[102, car battery, 12V car battery, 8.100, NULL]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800, 19]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.750, 25]");
        waitForResult(expected, table1, rowType1, getPrimaryKey());

        rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.DECIMAL(8, 3),
                            DataTypes.STRING()
                        },
                        new String[] {"id", "name", "description", "weight", "address"});

        expected =
                Arrays.asList(
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800, Beijing]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.750, Shanghai]");
        waitForResult(expected, table2, rowType2, getPrimaryKey());
    }

    private BiFunction<String, String, ProducerRecord<byte[], byte[]>> convertFunction(
            int schemaVersion) {
        return (topic, line) -> {
            try {
                JsonNode payload = objectMapper.readTree(line);
                JsonNode source = payload.get("source");
                JsonNode after = payload.get("after");

                GenericRecord avroKey;
                GenericRecord avroValue;
                GenericRecord afterAvroValue;

                if (topic.equalsIgnoreCase("test_avro.workdb.t1")) {
                    avroKey = new GenericData.Record(t1KeySchema);
                    avroKey.put("id", after.get("id").asInt());

                    if (schemaVersion == 1) {
                        avroValue = new GenericData.Record(t1ValueSchemaV1);
                        afterAvroValue =
                                new GenericData.Record(
                                        sanitizedSchema(
                                                t1ValueSchemaV1.getField("before").schema()));
                        afterAvroValue.put("id", after.get("id").asInt());
                        afterAvroValue.put("name", after.get("name").asText());
                        afterAvroValue.put("description", after.get("description").asText());
                        afterAvroValue.put("weight", after.get("weight").asText());
                    } else {
                        avroValue = new GenericData.Record(t1ValueSchemaV2);
                        afterAvroValue =
                                new GenericData.Record(
                                        sanitizedSchema(
                                                t1ValueSchemaV2.getField("before").schema()));
                        afterAvroValue.put("id", after.get("id").asInt());
                        afterAvroValue.put("name", after.get("name").asText());
                        afterAvroValue.put("description", after.get("description").asText());
                        afterAvroValue.put("weight", after.get("weight").asText());
                        afterAvroValue.put("age", after.get("age").asInt());
                    }
                    avroValue.put("after", afterAvroValue);
                } else {
                    avroKey = new GenericData.Record(t2KeySchema);
                    avroKey.put("id", after.get("id").asInt());

                    if (schemaVersion == 1) {
                        avroValue = new GenericData.Record(t2ValueSchemaV1);
                        afterAvroValue =
                                new GenericData.Record(
                                        sanitizedSchema(
                                                t2ValueSchemaV1.getField("before").schema()));
                        afterAvroValue.put("id", after.get("id").asInt());
                        afterAvroValue.put("name", after.get("name").asText());
                        afterAvroValue.put("description", after.get("description").asText());
                        afterAvroValue.put("weight", after.get("weight").asText());
                    } else {
                        avroValue = new GenericData.Record(t2ValueSchemaV2);
                        afterAvroValue =
                                new GenericData.Record(
                                        sanitizedSchema(
                                                t2ValueSchemaV2.getField("before").schema()));
                        afterAvroValue.put("id", after.get("id").asInt());
                        afterAvroValue.put("name", after.get("name").asText());
                        afterAvroValue.put("description", after.get("description").asText());
                        afterAvroValue.put("weight", after.get("weight").asText());
                        afterAvroValue.put("address", after.get("address").asText());
                    }
                    avroValue.put("after", afterAvroValue);
                }
                // Common properties
                avroValue.put("source", buildDebeziumSourceRecord(debeziumSourceSchema, source));
                avroValue.put("op", payload.get("op").asText());
                avroValue.put("ts_ms", payload.get("ts_ms").asLong());

                return new ProducerRecord<>(
                        topic,
                        kafkaKeyAvroSerializer.serialize(topic, avroKey),
                        kafkaValueAvroSerializer.serialize(topic, avroValue));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private List<String> getPrimaryKey() {
        return Collections.singletonList("id");
    }
}
