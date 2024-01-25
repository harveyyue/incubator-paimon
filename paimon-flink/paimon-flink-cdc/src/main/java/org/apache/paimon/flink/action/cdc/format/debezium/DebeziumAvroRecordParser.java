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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.DataFormat;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;

import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.sanitizedSchema;

/**
 * Implementation of {@link DebeziumRecordParser} for parsing messages in the Debezium avro format.
 *
 * <p>This parser handles records in the Debezium avro format and extracts relevant information to
 * produce {@link RichCdcMultiplexRecord} objects.
 */
public class DebeziumAvroRecordParser extends DebeziumRecordParser {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumAvroRecordParser.class);

    private String topic;
    private GenericContainerWithVersion key;
    private GenericContainerWithVersion value;
    private Map<Schema, List<String>> paimonPrimaryKeyCache;
    private Map<Schema, LinkedHashMap<String, DataType>> paimonDataTypeCache;

    public DebeziumAvroRecordParser(
            boolean caseSensitive, TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(caseSensitive, typeMapping, computedColumns);
    }

    @Override
    protected void setRoot(CdcSourceRecord record) {
        topic = record.getTopic();
        key = (GenericContainerWithVersion) record.getKey();
        value = (GenericContainerWithVersion) record.getValue();

        if (paimonPrimaryKeyCache == null) {
            paimonPrimaryKeyCache = new BoundedConcurrentHashMap<>();
        }
        if (paimonDataTypeCache == null) {
            paimonDataTypeCache = new BoundedConcurrentHashMap<>();
        }
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        String operation = getAndCheckValue(FIELD_TYPE).toString();
        switch (operation) {
            case OP_INSERT:
            case OP_READE:
                processRecord(getAfter(operation), RowKind.INSERT, records);
                break;
            case OP_UPDATE:
                processRecord(getBefore(operation), RowKind.DELETE, records);
                processRecord(getAfter(operation), RowKind.INSERT, records);
                break;
            case OP_DELETE:
                processRecord(getBefore(operation), RowKind.DELETE, records);
                break;
            case OP_TRUNCATE:
            case OP_MESSAGE:
                LOG.warn("Skip record operation: {}", operation);
                break;
            default:
                throw new UnsupportedOperationException("Unknown record operation: " + operation);
        }
        return records;
    }

    private void processRecord(
            GenericRecord payload, RowKind rowKind, List<RichCdcMultiplexRecord> records) {
        Map<String, String> resultMap = new HashMap<>();
        LinkedHashMap<String, DataType> paimonFieldTypes =
                paimonDataTypeCache.get(value.container().getSchema());
        LinkedHashMap<String, DataType> newPaimonFieldTypes = new LinkedHashMap<>();

        for (Schema.Field field : payload.getSchema().getFields()) {
            Schema fieldSchema = sanitizedSchema(field.schema());
            Map<String, Object> props = fieldSchema.getObjectProps();
            Map<String, String> parameters = new HashMap<>();
            if (props.get(CONNECT_PARAMETERS) != null) {
                parameters = (Map<String, String>) props.get(CONNECT_PARAMETERS);
            }
            String className = (String) props.get(CONNECT_NAME);
            String fieldName = field.name();
            String typeName = fieldSchema.getType().name();
            // Will use original kafka connect type since avro will treat int16 as int
            String connectTypeName = (String) props.get(CONNECT_TYPE);

            resultMap.put(
                    fieldName,
                    DebeziumSchemaUtils.transformRawValue(
                            DataFormat.DEBEZIUM_AVRO,
                            payload.get(fieldName),
                            typeName.toLowerCase(),
                            className,
                            typeMapping,
                            parameters));

            if (paimonFieldTypes == null) {
                newPaimonFieldTypes.put(
                        fieldName,
                        DebeziumSchemaUtils.toDataType(
                                typeName,
                                className,
                                parameters,
                                DataFormat.DEBEZIUM_AVRO,
                                connectTypeName));
            }
        }

        // Put to cache
        if (paimonFieldTypes == null) {
            paimonFieldTypes =
                    paimonDataTypeCache.computeIfAbsent(
                            value.container().getSchema(), schema -> newPaimonFieldTypes);
        }

        evalComputedColumns(resultMap, paimonFieldTypes);

        records.add(createRecord(rowKind, resultMap, paimonFieldTypes));
    }

    @Override
    protected List<String> extractPrimaryKeys() {
        if (key == null) {
            return Collections.emptyList();
        }

        return paimonPrimaryKeyCache.computeIfAbsent(
                key.container().getSchema(),
                schema ->
                        schema.getFields().stream()
                                .map(Schema.Field::name)
                                .collect(Collectors.toList()));
    }

    @Override
    protected String getTableName() {
        // Set table name from topic naming strategy or debezium's source property if needed.
        String[] parts = splitTopicParts();
        return parts.length == 3 ? parts[2] : getFromSourceField(FIELD_TABLE);
    }

    @Override
    protected String getDatabaseName() {
        // Set database name from topic naming strategy or debezium's source property if needed.
        String[] parts = splitTopicParts();
        return parts.length == 3 ? parts[1] : getFromSourceField(FIELD_DB);
    }

    @Override
    protected String format() {
        return "debezium-avro";
    }

    @Nullable
    private String getFromSourceField(String key) {
        GenericRecord sourceRecord =
                (GenericRecord) ((GenericRecord) value.container()).get(FIELD_SOURCE);
        if (sourceRecord == null) {
            return null;
        }

        Object result = sourceRecord.get(key);
        return result == null ? null : result.toString();
    }

    private String[] splitTopicParts() {
        return DEFAULT_DOT_PATTERN.split(topic);
    }

    private Object getAndCheckValue(String key) {
        Object obj = ((GenericRecord) value.container()).get(key);
        checkNotNull(obj, key);
        return obj;
    }

    private GenericRecord getAndCheckRecord(
            String key, String conditionKey, String conditionValue) {
        GenericRecord genericRecord = (GenericRecord) ((GenericRecord) value.container()).get(key);
        checkNotNull(genericRecord, key, conditionKey, conditionValue);
        return genericRecord;
    }

    private GenericRecord getAfter(String op) {
        return getAndCheckRecord(FIELD_AFTER, FIELD_TYPE, op);
    }

    private GenericRecord getBefore(String op) {
        return getAndCheckRecord(FIELD_BEFORE, FIELD_TYPE, op);
    }
}
