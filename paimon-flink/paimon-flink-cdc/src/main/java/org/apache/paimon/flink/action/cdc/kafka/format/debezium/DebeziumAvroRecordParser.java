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

package org.apache.paimon.flink.action.cdc.kafka.format.debezium;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.kafka.format.AbstractRecordParser;
import org.apache.paimon.flink.action.cdc.kafka.format.RecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import com.esri.core.geometry.ogc.OGCGeometry;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnCaseConvertAndDuplicateCheck;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnDuplicateErrMsg;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.listCaseConvert;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Implementation of {@link RecordParser} for parsing messages in the Debezium avro format.
 *
 * <p>This parser handles records in the Debezium avro format and extracts relevant information to
 * produce {@link RichCdcMultiplexRecord} objects.
 */
public class DebeziumAvroRecordParser extends AbstractRecordParser {
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumAvroRecordParser.class);

    private static final String FIELD_OP = "op";
    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_SOURCE_DB = "db";
    private static final String FIELD_SOURCE_TABLE = "table";
    private static final String OP_READ = "r";
    private static final String OP_CREATE = "c";
    private static final String OP_UPDATE = "u";
    private static final String OP_DELETE = "d";
    private static final String OP_TRUNCATE = "t";
    private static final String OP_MESSAGE = "m";
    private static final Schema NULL_AVRO_SCHEMA = Schema.create(Schema.Type.NULL);
    private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

    private Deserializer deserializer;
    private GenericContainerWithVersion keyContainerWithVersion;
    private GenericContainerWithVersion valueContainerWithVersion;
    private List<DebeziumAvroFieldDescriptor> fieldDescriptors;

    public DebeziumAvroRecordParser(
            boolean caseSensitive,
            TypeMapping typeMapping,
            List<ComputedColumn> computedColumns,
            String schemaRegistryUrl) {
        super(caseSensitive, typeMapping, computedColumns, schemaRegistryUrl);
    }

    @Override
    public void open() {
        checkArgument(
                !StringUtils.isBlank(this.schemaRegistryUrl),
                "[schema-registry-url] must be specified.");
        this.deserializer =
                new Deserializer(
                        new CachedSchemaRegistryClient(
                                schemaRegistryUrl, DEFAULT_IDENTITY_MAP_CAPACITY));
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords(String topic, byte[] key, byte[] value)
            throws IOException {
        parseKeyValueRecord(topic, key, value);
        // Skip debezium tombstone event
        if (valueContainerWithVersion == null) {
            return Collections.emptyList();
        }
        validateFormat();
        extractPrimaryKeys();

        return doExtractRecords();
    }

    @Override
    protected List<RichCdcMultiplexRecord> doExtractRecords() {
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        GenericRecord valueRecord = (GenericRecord) valueContainerWithVersion.container();
        String op = valueRecord.get(FIELD_OP).toString();
        GenericRecord source = (GenericRecord) valueRecord.get(FIELD_SOURCE);
        databaseName = source.get(FIELD_SOURCE_DB).toString();
        tableName = source.get(FIELD_SOURCE_TABLE).toString();

        switch (op) {
            case OP_READ:
            case OP_CREATE:
                processRecord(
                        (GenericRecord) valueRecord.get(FIELD_AFTER), RowKind.INSERT, records);
                break;
            case OP_UPDATE:
                processRecord(
                        (GenericRecord) valueRecord.get(FIELD_BEFORE), RowKind.DELETE, records);
                processRecord(
                        (GenericRecord) valueRecord.get(FIELD_AFTER), RowKind.INSERT, records);
                break;
            case OP_DELETE:
                processRecord(
                        (GenericRecord) valueRecord.get(FIELD_BEFORE), RowKind.DELETE, records);
                break;
            case OP_TRUNCATE:
            case OP_MESSAGE:
                LOG.info("Skip record operation: {}", op);
                break;
            default:
                throw new UnsupportedOperationException("Unknown record operation: " + op);
        }

        return records;
    }

    private void processRecord(
            GenericRecord payload, RowKind kind, List<RichCdcMultiplexRecord> records) {
        Map<String, String> resultMap = new HashMap<>();
        LinkedHashMap<String, DataType> paimonFieldTypes = setPaimonFieldType();

        fieldDescriptors.forEach(
                field -> {
                    String key = field.getColumnName();
                    resultMap.put(
                            key,
                            payload.get(key) == null
                                    ? null
                                    : transformValue(payload.get(key), field));
                });

        // generate values for computed columns
        computedColumns.forEach(
                computedColumn -> {
                    resultMap.put(
                            computedColumn.columnName(),
                            computedColumn.eval(resultMap.get(computedColumn.fieldReference())));
                    paimonFieldTypes.put(computedColumn.columnName(), computedColumn.columnType());
                });

        records.add(createRecord(kind, resultMap, paimonFieldTypes));
    }

    @Override
    protected void extractPrimaryKeys() {
        if (keyContainerWithVersion != null) {
            GenericRecord key = (GenericRecord) keyContainerWithVersion.container();
            Schema keySchema = sanitizedSchema(key.getSchema());
            keySchema.getFields().stream().map(Schema.Field::name).forEach(primaryKeys::add);
        }
    }

    @Override
    protected LinkedHashMap<String, DataType> setPaimonFieldType() {
        LinkedHashMap<String, DataType> paimonFieldTypes = new LinkedHashMap<>();
        GenericRecord value = (GenericRecord) valueContainerWithVersion.container();
        Schema payloadSchema =
                sanitizedSchema(((GenericRecord) value.get(FIELD_AFTER)).getSchema());

        fieldDescriptors =
                payloadSchema.getFields().stream()
                        .map(
                                filed ->
                                        new DebeziumAvroFieldDescriptor(
                                                sanitizedSchema(filed.schema()),
                                                filed.name(),
                                                primaryKeys.stream()
                                                        .anyMatch(key -> key.equals(filed.name()))))
                        .collect(Collectors.toList());

        fieldDescriptors.forEach(
                field -> paimonFieldTypes.put(field.getColumnName(), field.getPaimonType()));

        return paimonFieldTypes;
    }

    @Override
    public void validateFormat() {
        String errorMessageTemplate =
                "Didn't find '%s' property in avro. Only supports debezium-avro format,"
                        + "please make sure your topic's format is correct.";
        GenericRecord value = (GenericRecord) valueContainerWithVersion.container();
        GenericRecord source = (GenericRecord) value.get(FIELD_SOURCE);
        checkNotNull(source, errorMessageTemplate, FIELD_SOURCE);
        checkNotNull(source.get(FIELD_SOURCE_DB), errorMessageTemplate, FIELD_SOURCE_DB);
        checkNotNull(source.get(FIELD_SOURCE_TABLE), errorMessageTemplate, FIELD_SOURCE_TABLE);
    }

    @Override
    public org.apache.paimon.schema.Schema getKafkaSchema(String topic, byte[] key, byte[] value) {
        parseKeyValueRecord(topic, key, value);
        // Skip debezium tombstone event
        if (valueContainerWithVersion == null) {
            return null;
        }
        validateFormat();
        extractPrimaryKeys();
        GenericRecord source =
                (GenericRecord)
                        ((GenericRecord) valueContainerWithVersion.container()).get(FIELD_SOURCE);
        databaseName = source.get(FIELD_SOURCE_DB).toString();
        tableName = source.get(FIELD_SOURCE_TABLE).toString();
        LinkedHashMap<String, DataType> paimonFieldTypes = setPaimonFieldType();

        org.apache.paimon.schema.Schema.Builder builder =
                org.apache.paimon.schema.Schema.newBuilder();
        Set<String> existedFields = new HashSet<>();
        Function<String, String> columnDuplicateErrMsg = columnDuplicateErrMsg(tableName);
        for (Map.Entry<String, DataType> entry : paimonFieldTypes.entrySet()) {
            builder.column(
                    columnCaseConvertAndDuplicateCheck(
                            entry.getKey(), existedFields, caseSensitive, columnDuplicateErrMsg),
                    entry.getValue());
        }
        builder.primaryKey(listCaseConvert(primaryKeys, caseSensitive));

        return builder.build();
    }

    private Schema sanitizedSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION
                && schema.getTypes().size() == 2
                && schema.getTypes().contains(NULL_AVRO_SCHEMA)) {
            for (Schema memberSchema : schema.getTypes()) {
                if (!memberSchema.equals(NULL_AVRO_SCHEMA)) {
                    return memberSchema;
                }
            }
        }
        return schema;
    }

    private void parseKeyValueRecord(String topic, byte[] key, byte[] value) {
        keyContainerWithVersion = deserializer.deserialize(topic, true, key);
        valueContainerWithVersion = deserializer.deserialize(topic, false, value);
    }

    private String transformValue(
            Object originalValue, DebeziumAvroFieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.isSetType()) {
            try {
                return OBJECT_MAPPER.writeValueAsString(originalValue);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Failed to convert %s to JSON.", originalValue), e);
            }
        } else if (fieldDescriptor.isGeoType()) {
            return convertWkbArray((GenericRecord) originalValue);
        } else if (originalValue instanceof ByteBuffer) {
            byte[] value = ((ByteBuffer) originalValue).array();
            if (fieldDescriptor.isDecimalPreciseType()) {
                return new BigDecimal(new BigInteger(value), fieldDescriptor.getScale())
                        .toPlainString();
            }
            return new String(value, StandardCharsets.UTF_8);
        } else {
            Object convertedObject = convertAvroObjectToJsonCompatible(originalValue);
            try {
                return OBJECT_MAPPER.writer().writeValueAsString(convertedObject);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Failed to convert %s to JSON.", originalValue), e);
            }
        }
    }

    public static String convertWkbArray(GenericRecord record) {
        try {
            String geoJson = OGCGeometry.fromBinary((ByteBuffer) record.get("wkb")).asGeoJson();
            JsonNode originGeoNode = OBJECT_MAPPER.readTree(geoJson);

            Map<String, Object> geometryInfo = new HashMap<>();
            String geometryType = originGeoNode.get("type").asText();
            geometryInfo.put("type", geometryType);
            if (geometryType.equalsIgnoreCase("GeometryCollection")) {
                geometryInfo.put("geometries", originGeoNode.get("geometries"));
            } else {
                geometryInfo.put("coordinates", originGeoNode.get("coordinates"));
            }
            int srid = record.get("srid") != null ? (int) record.get("srid") : 0;
            geometryInfo.put("srid", srid);

            return OBJECT_MAPPER.writer().writeValueAsString(geometryInfo);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to convert %s to geometry JSON.", record), e);
        }
    }

    public Object convertAvroObjectToJsonCompatible(Object avroObject) {
        if (avroObject instanceof GenericData.Record) {
            return convertRecord((GenericData.Record) avroObject);
        } else if (avroObject instanceof GenericData.Array) {
            return convertArray((GenericData.Array<?>) avroObject);
        } else {
            // For other types, return as is or TODO: apply specific conversion if needed
            return avroObject;
        }
    }

    private Map<String, Object> convertRecord(GenericData.Record record) {
        Map<String, Object> map = new HashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.pos());
            map.put(field.name(), convertAvroObjectToJsonCompatible(value));
        }
        return map;
    }

    private List<Object> convertArray(GenericData.Array<?> array) {
        List<Object> list = new ArrayList<>();
        for (Object element : array) {
            list.add(convertAvroObjectToJsonCompatible(element));
        }
        return list;
    }

    private static class Deserializer extends AbstractKafkaAvroDeserializer
            implements Serializable {

        public Deserializer(SchemaRegistryClient client) {
            this.schemaRegistry = client;
        }

        public GenericContainerWithVersion deserialize(
                String topic, boolean isKey, byte[] payload) {
            return deserializeWithSchemaAndVersion(topic, isKey, payload);
        }
    }
}
