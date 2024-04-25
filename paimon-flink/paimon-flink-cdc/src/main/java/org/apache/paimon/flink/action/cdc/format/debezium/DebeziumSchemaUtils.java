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

import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.DataFormat;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.esri.core.geometry.ogc.OGCGeometry;
import io.debezium.data.Bits;
import io.debezium.data.EnumSet;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.json.JsonConverterConfig;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumRecordParser.CONNECT_DECIMAL_PRECISION;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumRecordParser.CONNECT_LENGTH;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumRecordParser.CONNECT_SCALE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumRecordParser.DEBEZIUM_SOURCE_COLUMN_LENGTH;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumRecordParser.DEBEZIUM_SOURCE_COLUMN_SCALE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumRecordParser.DEBEZIUM_SOURCE_COLUMN_TYPE;

/**
 * Utils to handle 'schema' field in debezium Json. TODO: The methods have many duplicate codes with
 * MySqlRecordParser. Need refactor.
 */
public class DebeziumSchemaUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Schema NULL_AVRO_SCHEMA = Schema.create(Schema.Type.NULL);

    /** Transform raw string value according to schema. */
    public static String transformRawValue(
            @Nullable Object rawValue,
            String debeziumType,
            @Nullable String className,
            TypeMapping typeMapping,
            ZoneId serverTimeZone,
            Map<String, String> parameters) {
        if (rawValue == null) {
            return null;
        }

        String transformed;

        if (Bits.LOGICAL_NAME.equals(className)) {
            // transform little-endian form to normal order
            // https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-data-types
            byte[] littleEndian = getBytes(rawValue);
            byte[] bigEndian = new byte[littleEndian.length];
            for (int i = 0; i < littleEndian.length; i++) {
                bigEndian[i] = littleEndian[littleEndian.length - 1 - i];
            }
            if (typeMapping.containsMode(TO_STRING)) {
                transformed = StringUtils.bytesToBinaryString(bigEndian);
            } else {
                transformed = Base64.getEncoder().encodeToString(bigEndian);
            }
        } else if (("bytes".equals(debeziumType) && className == null)) {
            // MySQL binary, varbinary, blob
            transformed = new String(getBytes(rawValue));
        } else if ("bytes".equals(debeziumType) && decimalLogicalName().equals(className)) {
            // MySQL numeric, fixed, decimal
            try {
                if (rawValue instanceof BigDecimal
                        || rawValue instanceof Integer
                        || rawValue instanceof Long) {
                    transformed = new BigDecimal(rawValue.toString()).toPlainString();
                } else {
                    byte[] bytes = getBytes(rawValue);
                    transformed =
                            new BigDecimal(
                                            new BigInteger(bytes),
                                            Integer.parseInt(parameters.get(CONNECT_SCALE)))
                                    .toPlainString();
                }
            } catch (Exception e) {
                throw new RuntimeException(
                        "Invalid big decimal value "
                                + rawValue
                                + ". Make sure that in the `customConverterConfigs` "
                                + "of the JsonDebeziumDeserializationSchema you created, set '"
                                + JsonConverterConfig.DECIMAL_FORMAT_CONFIG
                                + "' to 'numeric'",
                        e);
            }
        }
        // pay attention to the temporal types
        // https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-temporal-types
        else if (Date.SCHEMA_NAME.equals(className)) {
            // MySQL date
            transformed = DateTimeUtils.toLocalDate((int) rawValue).toString();
        } else if (Timestamp.SCHEMA_NAME.equals(className)) {
            // MySQL datetime (precision 0-3)

            // display value of datetime is not affected by timezone, see
            // https://dev.mysql.com/doc/refman/8.0/en/datetime.html for standard, and
            // RowDataDebeziumDeserializeSchema#convertToTimestamp in flink-cdc-connector
            // for implementation
            LocalDateTime localDateTime =
                    DateTimeUtils.toLocalDateTime((long) rawValue, ZoneOffset.UTC);
            transformed = DateTimeUtils.formatLocalDateTime(localDateTime, 3);
        } else if (MicroTimestamp.SCHEMA_NAME.equals(className)) {
            // MySQL datetime (precision 4-6)
            long microseconds = (long) rawValue;
            long microsecondsPerSecond = 1_000_000;
            long nanosecondsPerMicros = 1_000;
            long seconds = microseconds / microsecondsPerSecond;
            long nanoAdjustment = (microseconds % microsecondsPerSecond) * nanosecondsPerMicros;

            // display value of datetime is not affected by timezone, see
            // https://dev.mysql.com/doc/refman/8.0/en/datetime.html for standard, and
            // RowDataDebeziumDeserializeSchema#convertToTimestamp in flink-cdc-connector
            // for implementation
            LocalDateTime localDateTime =
                    Instant.ofEpochSecond(seconds, nanoAdjustment)
                            .atZone(ZoneOffset.UTC)
                            .toLocalDateTime();
            transformed = DateTimeUtils.formatLocalDateTime(localDateTime, 6);
        } else if (ZonedTimestamp.SCHEMA_NAME.equals(className)) {
            // MySQL timestamp

            // display value of timestamp is affected by timezone, see
            // https://dev.mysql.com/doc/refman/8.0/en/datetime.html for standard, and
            // RowDataDebeziumDeserializeSchema#convertToTimestamp in flink-cdc-connector
            // for implementation
            LocalDateTime localDateTime =
                    Instant.parse(rawValue.toString()).atZone(serverTimeZone).toLocalDateTime();
            transformed = DateTimeUtils.formatLocalDateTime(localDateTime, 6);
        } else if (MicroTime.SCHEMA_NAME.equals(className)) {
            long microseconds = (long) rawValue;
            long microsecondsPerSecond = 1_000_000;
            long nanosecondsPerMicros = 1_000;
            long seconds = microseconds / microsecondsPerSecond;
            long nanoAdjustment = (microseconds % microsecondsPerSecond) * nanosecondsPerMicros;

            transformed =
                    Instant.ofEpochSecond(seconds, nanoAdjustment)
                            .atZone(ZoneOffset.UTC)
                            .toLocalTime()
                            .toString();
        } else if (Point.LOGICAL_NAME.equals(className)
                || Geometry.LOGICAL_NAME.equals(className)) {
            ByteBuffer wkb;
            int srid;

            if (rawValue instanceof GenericRecord) {
                GenericRecord record = (GenericRecord) rawValue;
                wkb = (ByteBuffer) record.get(Geometry.WKB_FIELD);
                srid =
                        record.get(Geometry.SRID_FIELD) != null
                                ? (int) record.get(Geometry.SRID_FIELD)
                                : 0;
            } else {
                Map<String, Object> record = (Map<String, Object>) rawValue;
                wkb =
                        ByteBuffer.wrap(
                                Base64.getDecoder()
                                        .decode(record.get(Geometry.WKB_FIELD).toString()));
                srid =
                        record.get(Geometry.SRID_FIELD) != null
                                ? (int) record.get(Geometry.SRID_FIELD)
                                : 0;
            }

            transformed = convertWkbArray(wkb, srid);
        } else {
            transformed = rawValue.toString();
        }

        return transformed;
    }

    private static byte[] getBytes(Object rawValue) {
        if (rawValue instanceof ByteBuffer) {
            return ((ByteBuffer) rawValue).array();
        }
        return Base64.getDecoder().decode(rawValue.toString());
    }

    private static String convertWkbArray(ByteBuffer wkb, int srid) {
        try {
            String geoJson = OGCGeometry.fromBinary(wkb).asGeoJson();
            JsonNode originGeoNode = objectMapper.readTree(geoJson);

            Map<String, Object> geometryInfo = new HashMap<>();
            String geometryType = originGeoNode.get("type").asText();
            geometryInfo.put("type", geometryType);
            if (geometryType.equalsIgnoreCase("GeometryCollection")) {
                geometryInfo.put("geometries", originGeoNode.get("geometries"));
            } else {
                geometryInfo.put("coordinates", originGeoNode.get("coordinates"));
            }
            geometryInfo.put("srid", srid);

            return objectMapper.writer().writeValueAsString(geometryInfo);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Failed to convert %s to geometry JSON.", wkb), e);
        }
    }

    public static DataType toDataType(
            String debeziumType, @Nullable String className, Map<String, String> parameters) {
        return toDataType(debeziumType, className, parameters, DataFormat.DEBEZIUM_JSON, null);
    }

    public static DataType toDataType(
            String debeziumType,
            @Nullable String className,
            Map<String, String> parameters,
            DataFormat dataFormat,
            @Nullable String connectType) {
        DataType dataType = null;
        if (className != null) {
            dataType = fromDebeziumClassName(className, parameters);
        }

        if (dataType == null) {
            if (dataFormat.equals(DataFormat.DEBEZIUM_AVRO)) {
                Schema.Type avroType = Schema.Type.valueOf(debeziumType);
                if (connectType != null && connectType.equals("int16")) {
                    dataType = DataTypes.SMALLINT();
                } else if (avroType.equals(Schema.Type.STRING)
                        && parameters
                                .get(DEBEZIUM_SOURCE_COLUMN_TYPE)
                                .equalsIgnoreCase("DECIMAL")) {
                    dataType =
                            getDecimalType(
                                    parameters.get(DEBEZIUM_SOURCE_COLUMN_LENGTH),
                                    parameters.get(DEBEZIUM_SOURCE_COLUMN_SCALE));
                } else {
                    dataType = fromDebeziumAvroType(avroType);
                }
            } else {
                dataType = fromDebeziumType(debeziumType);
            }
        }

        return dataType;
    }

    private static DataType fromDebeziumClassName(
            String className, Map<String, String> parameters) {
        if (Bits.LOGICAL_NAME.equals(className)) {
            int length = Integer.parseInt(parameters.get(CONNECT_LENGTH));
            return DataTypes.BINARY((length + 7) / 8);
        }

        if (decimalLogicalName().equals(className)) {
            return getDecimalType(
                    parameters.get(CONNECT_DECIMAL_PRECISION), parameters.get(CONNECT_SCALE));
        }

        if (Date.SCHEMA_NAME.equals(className)) {
            return DataTypes.DATE();
        }

        if (Timestamp.SCHEMA_NAME.equals(className)) {
            return DataTypes.TIMESTAMP(3);
        }

        if (MicroTimestamp.SCHEMA_NAME.equals(className)
                || ZonedTimestamp.SCHEMA_NAME.equals(className)) {
            return DataTypes.TIMESTAMP(6);
        }

        if (MicroTime.SCHEMA_NAME.equals(className)) {
            return DataTypes.TIME();
        }

        if (EnumSet.LOGICAL_NAME.equals(className)) {
            return DataTypes.ARRAY(DataTypes.STRING());
        }

        return null;
    }

    private static DataType getDecimalType(String precision, String scale) {
        if (precision == null) {
            return DataTypes.DECIMAL(20, 0);
        }

        int p = Integer.parseInt(precision);
        if (p > DecimalType.MAX_PRECISION) {
            return DataTypes.STRING();
        }
        return DataTypes.DECIMAL(p, scale == null ? 0 : Integer.parseInt(scale));
    }

    private static DataType fromDebeziumType(String dbzType) {
        switch (dbzType) {
            case "int8":
                return DataTypes.TINYINT();
            case "int16":
                return DataTypes.SMALLINT();
            case "int32":
                return DataTypes.INT();
            case "int64":
                return DataTypes.BIGINT();
            case "float32":
            case "float64":
                return DataTypes.FLOAT();
            case "double":
                return DataTypes.DOUBLE();
            case "boolean":
                return DataTypes.BOOLEAN();
            case "bytes":
                return DataTypes.BYTES();
            case "string":
            default:
                return DataTypes.STRING();
        }
    }

    /**
     * get decimal logical name.
     *
     * <p>Using the maven shade plugin will shade the constant value. see <a
     * href="https://issues.apache.org/jira/browse/MSHADE-156">...</a> so the string
     * org.apache.kafka.connect.data.Decimal is shaded to org.apache.flink.kafka.shaded
     * .org.apache.kafka.connect.data.Decimal.
     */
    public static String decimalLogicalName() {
        return "org.apache.#.connect.data.Decimal".replace("#", "kafka");
    }

    public static DataType fromDebeziumAvroType(Schema.Type avroType) {
        switch (avroType) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case BYTES:
            case FIXED:
                return DataTypes.BYTES();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case FLOAT:
                // https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
                // MySql engine treat float(p) as following when execute created table statement:
                // A precision from 0 to 23 results in a 4-byte single-precision FLOAT column.
                // A precision from 24 to 53 results in an 8-byte double-precision DOUBLE column.
                // Debezium fix float type mapping issue in
                // https://issues.redhat.com/browse/DBZ-5843
                return DataTypes.FLOAT();
            case INT:
                return DataTypes.INT();
            case LONG:
                return DataTypes.BIGINT();
            case STRING:
            case RECORD:
            default:
                return DataTypes.STRING();
        }
    }

    public static Schema sanitizedSchema(Schema schema) {
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
}
