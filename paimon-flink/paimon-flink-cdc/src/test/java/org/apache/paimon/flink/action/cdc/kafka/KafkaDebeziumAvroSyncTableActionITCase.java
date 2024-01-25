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

import org.apache.paimon.flink.action.cdc.format.DataFormat;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.Base64Variants;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.sanitizedSchema;
import static org.apache.paimon.flink.action.cdc.kafka.KafkaActionUtils.SCHEMA_REGISTRY_URL;

/** IT cases for {@link KafkaSyncTableAction}. */
public class KafkaDebeziumAvroSyncTableActionITCase extends KafkaSyncTableActionITCase {

    private static final String ALL_TYPES_TABLE_KEY_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Key\",\"namespace\":\"test_avro_precise.workdb.all_types_table\",\"fields\":[{\"name\":\"_id\",\"type\":\"int\"}],\"connect.name\":\"test_avro_precise.workdb.all_types_table.Key\"}";
    private static final String ALL_TYPES_TABLE_VALUE_SCHEMA =
            "{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"test_avro_precise.workdb.all_types_table\",\"fields\":[{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"_id\",\"type\":{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"_id\"}}},{\"name\":\"pt\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":1,\"precision\":2,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"1\",\"connect.decimal.precision\":\"2\",\"__debezium.source.column.type\":\"DECIMAL\",\"__debezium.source.column.length\":\"2\",\"__debezium.source.column.scale\":\"1\",\"__debezium.source.column.name\":\"pt\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_bit1\",\"type\":[\"null\",{\"type\":\"boolean\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BIT\",\"__debezium.source.column.length\":\"1\",\"__debezium.source.column.name\":\"_bit1\"}}],\"default\":null},{\"name\":\"_bit\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.version\":1,\"connect.parameters\":{\"length\":\"64\",\"__debezium.source.column.type\":\"BIT\",\"__debezium.source.column.length\":\"64\",\"__debezium.source.column.name\":\"_bit\"},\"connect.name\":\"io.debezium.data.Bits\"}],\"default\":null},{\"name\":\"_tinyint1\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYINT\",\"__debezium.source.column.length\":\"1\",\"__debezium.source.column.name\":\"_tinyint1\"},\"connect.type\":\"int16\"}],\"default\":null},{\"name\":\"_boolean\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYINT\",\"__debezium.source.column.length\":\"1\",\"__debezium.source.column.name\":\"_boolean\"},\"connect.type\":\"int16\"}],\"default\":null},{\"name\":\"_bool\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYINT\",\"__debezium.source.column.length\":\"1\",\"__debezium.source.column.name\":\"_bool\"},\"connect.type\":\"int16\"}],\"default\":null},{\"name\":\"_tinyint\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYINT\",\"__debezium.source.column.name\":\"_tinyint\"},\"connect.type\":\"int16\"}],\"default\":null},{\"name\":\"_tinyint_unsigned\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYINT UNSIGNED\",\"__debezium.source.column.name\":\"_tinyint_unsigned\"},\"connect.type\":\"int16\"}],\"default\":null},{\"name\":\"_tinyint_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYINT UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"2\",\"__debezium.source.column.name\":\"_tinyint_unsigned_zerofill\"},\"connect.type\":\"int16\"}],\"default\":null},{\"name\":\"_smallint\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"SMALLINT\",\"__debezium.source.column.name\":\"_smallint\"},\"connect.type\":\"int16\"}],\"default\":null},{\"name\":\"_smallint_unsigned\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"SMALLINT UNSIGNED\",\"__debezium.source.column.name\":\"_smallint_unsigned\"}}],\"default\":null},{\"name\":\"_smallint_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"SMALLINT UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"4\",\"__debezium.source.column.name\":\"_smallint_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_mediumint\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"MEDIUMINT\",\"__debezium.source.column.name\":\"_mediumint\"}}],\"default\":null},{\"name\":\"_mediumint_unsigned\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"MEDIUMINT UNSIGNED\",\"__debezium.source.column.name\":\"_mediumint_unsigned\"}}],\"default\":null},{\"name\":\"_mediumint_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"MEDIUMINT UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.name\":\"_mediumint_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_int\",\"type\":[\"null\",{\"type\":\"int\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT\",\"__debezium.source.column.name\":\"_int\"}}],\"default\":null},{\"name\":\"_int_unsigned\",\"type\":[\"null\",{\"type\":\"long\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT UNSIGNED\",\"__debezium.source.column.name\":\"_int_unsigned\"}}],\"default\":null},{\"name\":\"_int_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"long\",\"connect.parameters\":{\"__debezium.source.column.type\":\"INT UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.name\":\"_int_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_bigint\",\"type\":[\"null\",{\"type\":\"long\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BIGINT\",\"__debezium.source.column.name\":\"_bigint\"}}],\"default\":null},{\"name\":\"_bigint_unsigned\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":0,\"precision\":20,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"0\",\"connect.decimal.precision\":\"20\",\"__debezium.source.column.type\":\"BIGINT UNSIGNED\",\"__debezium.source.column.name\":\"_bigint_unsigned\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_bigint_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":0,\"precision\":20,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"0\",\"connect.decimal.precision\":\"20\",\"__debezium.source.column.type\":\"BIGINT UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"16\",\"__debezium.source.column.name\":\"_bigint_unsigned_zerofill\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_serial\",\"type\":{\"type\":\"bytes\",\"scale\":0,\"precision\":20,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"0\",\"connect.decimal.precision\":\"20\",\"__debezium.source.column.type\":\"BIGINT UNSIGNED\",\"__debezium.source.column.name\":\"_serial\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}},{\"name\":\"_float\",\"type\":[\"null\",{\"type\":\"float\",\"connect.parameters\":{\"__debezium.source.column.type\":\"FLOAT\",\"__debezium.source.column.name\":\"_float\"}}],\"default\":null},{\"name\":\"_float_unsigned\",\"type\":[\"null\",{\"type\":\"float\",\"connect.parameters\":{\"__debezium.source.column.type\":\"FLOAT UNSIGNED\",\"__debezium.source.column.name\":\"_float_unsigned\"}}],\"default\":null},{\"name\":\"_float_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"float\",\"connect.parameters\":{\"__debezium.source.column.type\":\"FLOAT UNSIGNED ZEROFILL\",\"__debezium.source.column.name\":\"_float_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_real\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE\",\"__debezium.source.column.name\":\"_real\"}}],\"default\":null},{\"name\":\"_real_unsigned\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE UNSIGNED\",\"__debezium.source.column.name\":\"_real_unsigned\"}}],\"default\":null},{\"name\":\"_real_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.scale\":\"7\",\"__debezium.source.column.name\":\"_real_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_double\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE\",\"__debezium.source.column.name\":\"_double\"}}],\"default\":null},{\"name\":\"_double_unsigned\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE UNSIGNED\",\"__debezium.source.column.name\":\"_double_unsigned\"}}],\"default\":null},{\"name\":\"_double_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.scale\":\"7\",\"__debezium.source.column.name\":\"_double_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_double_precision\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE\",\"__debezium.source.column.name\":\"_double_precision\"}}],\"default\":null},{\"name\":\"_double_precision_unsigned\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE UNSIGNED\",\"__debezium.source.column.name\":\"_double_precision_unsigned\"}}],\"default\":null},{\"name\":\"_double_precision_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"double\",\"connect.parameters\":{\"__debezium.source.column.type\":\"DOUBLE UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.scale\":\"7\",\"__debezium.source.column.name\":\"_double_precision_unsigned_zerofill\"}}],\"default\":null},{\"name\":\"_numeric\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":3,\"precision\":8,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"3\",\"connect.decimal.precision\":\"8\",\"__debezium.source.column.type\":\"DECIMAL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"_numeric\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_numeric_unsigned\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":3,\"precision\":8,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"3\",\"connect.decimal.precision\":\"8\",\"__debezium.source.column.type\":\"DECIMAL UNSIGNED\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"_numeric_unsigned\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_numeric_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":3,\"precision\":8,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"3\",\"connect.decimal.precision\":\"8\",\"__debezium.source.column.type\":\"DECIMAL UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"_numeric_unsigned_zerofill\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_fixed\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":3,\"precision\":40,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"3\",\"connect.decimal.precision\":\"40\",\"__debezium.source.column.type\":\"DECIMAL\",\"__debezium.source.column.length\":\"40\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"_fixed\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_fixed_unsigned\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":3,\"precision\":40,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"3\",\"connect.decimal.precision\":\"40\",\"__debezium.source.column.type\":\"DECIMAL UNSIGNED\",\"__debezium.source.column.length\":\"40\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"_fixed_unsigned\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_fixed_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":3,\"precision\":40,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"3\",\"connect.decimal.precision\":\"40\",\"__debezium.source.column.type\":\"DECIMAL UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"40\",\"__debezium.source.column.scale\":\"3\",\"__debezium.source.column.name\":\"_fixed_unsigned_zerofill\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_decimal\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":0,\"precision\":8,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"0\",\"connect.decimal.precision\":\"8\",\"__debezium.source.column.type\":\"DECIMAL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"0\",\"__debezium.source.column.name\":\"_decimal\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_decimal_unsigned\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":0,\"precision\":8,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"0\",\"connect.decimal.precision\":\"8\",\"__debezium.source.column.type\":\"DECIMAL UNSIGNED\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"0\",\"__debezium.source.column.name\":\"_decimal_unsigned\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_decimal_unsigned_zerofill\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":0,\"precision\":8,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"0\",\"connect.decimal.precision\":\"8\",\"__debezium.source.column.type\":\"DECIMAL UNSIGNED ZEROFILL\",\"__debezium.source.column.length\":\"8\",\"__debezium.source.column.scale\":\"0\",\"__debezium.source.column.name\":\"_decimal_unsigned_zerofill\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_big_decimal\",\"type\":[\"null\",{\"type\":\"bytes\",\"scale\":10,\"precision\":38,\"connect.version\":1,\"connect.parameters\":{\"scale\":\"10\",\"connect.decimal.precision\":\"38\",\"__debezium.source.column.type\":\"DECIMAL\",\"__debezium.source.column.length\":\"38\",\"__debezium.source.column.scale\":\"10\",\"__debezium.source.column.name\":\"_big_decimal\"},\"connect.name\":\"org.apache.kafka.connect.data.Decimal\",\"logicalType\":\"decimal\"}],\"default\":null},{\"name\":\"_date\",\"type\":[\"null\",{\"type\":\"int\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"DATE\",\"__debezium.source.column.name\":\"_date\"},\"connect.name\":\"io.debezium.time.Date\"}],\"default\":null},{\"name\":\"_datetime\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"DATETIME\",\"__debezium.source.column.name\":\"_datetime\"},\"connect.name\":\"io.debezium.time.Timestamp\"}],\"default\":null},{\"name\":\"_datetime3\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"DATETIME\",\"__debezium.source.column.length\":\"3\",\"__debezium.source.column.name\":\"_datetime3\"},\"connect.name\":\"io.debezium.time.Timestamp\"}],\"default\":null},{\"name\":\"_datetime6\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"DATETIME\",\"__debezium.source.column.length\":\"6\",\"__debezium.source.column.name\":\"_datetime6\"},\"connect.name\":\"io.debezium.time.MicroTimestamp\"}],\"default\":null},{\"name\":\"_datetime_p\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"DATETIME\",\"__debezium.source.column.name\":\"_datetime_p\"},\"connect.name\":\"io.debezium.time.Timestamp\"}],\"default\":null},{\"name\":\"_datetime_p2\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"DATETIME\",\"__debezium.source.column.length\":\"2\",\"__debezium.source.column.name\":\"_datetime_p2\"},\"connect.name\":\"io.debezium.time.Timestamp\"}],\"default\":null},{\"name\":\"_timestamp\",\"type\":[\"null\",{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"TIMESTAMP\",\"__debezium.source.column.length\":\"6\",\"__debezium.source.column.name\":\"_timestamp\"},\"connect.name\":\"io.debezium.time.ZonedTimestamp\"}],\"default\":null},{\"name\":\"_timestamp0\",\"type\":[\"null\",{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"TIMESTAMP\",\"__debezium.source.column.name\":\"_timestamp0\"},\"connect.name\":\"io.debezium.time.ZonedTimestamp\"}],\"default\":null},{\"name\":\"_char\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"CHAR\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.name\":\"_char\"}}],\"default\":null},{\"name\":\"_varchar\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARCHAR\",\"__debezium.source.column.length\":\"20\",\"__debezium.source.column.name\":\"_varchar\"}}],\"default\":null},{\"name\":\"_tinytext\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYTEXT\",\"__debezium.source.column.name\":\"_tinytext\"}}],\"default\":null},{\"name\":\"_text\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TEXT\",\"__debezium.source.column.name\":\"_text\"}}],\"default\":null},{\"name\":\"_mediumtext\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"MEDIUMTEXT\",\"__debezium.source.column.name\":\"_mediumtext\"}}],\"default\":null},{\"name\":\"_longtext\",\"type\":[\"null\",{\"type\":\"string\",\"connect.parameters\":{\"__debezium.source.column.type\":\"LONGTEXT\",\"__debezium.source.column.name\":\"_longtext\"}}],\"default\":null},{\"name\":\"_bin\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BINARY\",\"__debezium.source.column.length\":\"10\",\"__debezium.source.column.name\":\"_bin\"}}],\"default\":null},{\"name\":\"_varbin\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.parameters\":{\"__debezium.source.column.type\":\"VARBINARY\",\"__debezium.source.column.length\":\"20\",\"__debezium.source.column.name\":\"_varbin\"}}],\"default\":null},{\"name\":\"_tinyblob\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.parameters\":{\"__debezium.source.column.type\":\"TINYBLOB\",\"__debezium.source.column.name\":\"_tinyblob\"}}],\"default\":null},{\"name\":\"_blob\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.parameters\":{\"__debezium.source.column.type\":\"BLOB\",\"__debezium.source.column.name\":\"_blob\"}}],\"default\":null},{\"name\":\"_mediumblob\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.parameters\":{\"__debezium.source.column.type\":\"MEDIUMBLOB\",\"__debezium.source.column.name\":\"_mediumblob\"}}],\"default\":null},{\"name\":\"_longblob\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.parameters\":{\"__debezium.source.column.type\":\"LONGBLOB\",\"__debezium.source.column.name\":\"_longblob\"}}],\"default\":null},{\"name\":\"_json\",\"type\":[\"null\",{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"JSON\",\"__debezium.source.column.name\":\"_json\"},\"connect.name\":\"io.debezium.data.Json\"}],\"default\":null},{\"name\":\"_enum\",\"type\":[\"null\",{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"value1,value2,value3\",\"__debezium.source.column.type\":\"ENUM\",\"__debezium.source.column.length\":\"1\",\"__debezium.source.column.name\":\"_enum\"},\"connect.name\":\"io.debezium.data.Enum\"}],\"default\":null},{\"name\":\"_year\",\"type\":[\"null\",{\"type\":\"int\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"YEAR\",\"__debezium.source.column.name\":\"_year\"},\"connect.name\":\"io.debezium.time.Year\"}],\"default\":null},{\"name\":\"_time\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"TIME\",\"__debezium.source.column.name\":\"_time\"},\"connect.name\":\"io.debezium.time.MicroTime\"}],\"default\":null},{\"name\":\"_point\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Point\",\"namespace\":\"io.debezium.data.geometry\",\"fields\":[{\"name\":\"x\",\"type\":\"double\"},{\"name\":\"y\",\"type\":\"double\"},{\"name\":\"wkb\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"srid\",\"type\":[\"null\",\"int\"],\"default\":null}],\"connect.doc\":\"Geometry (POINT)\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"POINT\",\"__debezium.source.column.name\":\"_point\"},\"connect.name\":\"io.debezium.data.geometry.Point\"}],\"default\":null},{\"name\":\"_geometry\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Geometry\",\"namespace\":\"io.debezium.data.geometry\",\"fields\":[{\"name\":\"wkb\",\"type\":\"bytes\"},{\"name\":\"srid\",\"type\":[\"null\",\"int\"],\"default\":null}],\"connect.doc\":\"Geometry\",\"connect.version\":1,\"connect.parameters\":{\"__debezium.source.column.type\":\"GEOMETRY\",\"__debezium.source.column.name\":\"_geometry\"},\"connect.name\":\"io.debezium.data.geometry.Geometry\"}],\"default\":null},{\"name\":\"_linestring\",\"type\":[\"null\",\"io.debezium.data.geometry.Geometry\"],\"default\":null},{\"name\":\"_polygon\",\"type\":[\"null\",\"io.debezium.data.geometry.Geometry\"],\"default\":null},{\"name\":\"_multipoint\",\"type\":[\"null\",\"io.debezium.data.geometry.Geometry\"],\"default\":null},{\"name\":\"_multiline\",\"type\":[\"null\",\"io.debezium.data.geometry.Geometry\"],\"default\":null},{\"name\":\"_multipolygon\",\"type\":[\"null\",\"io.debezium.data.geometry.Geometry\"],\"default\":null},{\"name\":\"_geometrycollection\",\"type\":[\"null\",\"io.debezium.data.geometry.Geometry\"],\"default\":null},{\"name\":\"_set\",\"type\":[\"null\",{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"a,b,c,d\",\"__debezium.source.column.type\":\"SET\",\"__debezium.source.column.length\":\"7\",\"__debezium.source.column.name\":\"_set\"},\"connect.name\":\"io.debezium.data.EnumSet\"}],\"default\":null}],\"connect.name\":\"test_avro_precise.workdb.all_types_table.Value\"}],\"default\":null},{\"name\":\"after\",\"type\":[\"null\",\"Value\"],\"default\":null},{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"Source\",\"namespace\":\"io.debezium.connector.mysql\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"connector\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":\"long\"},{\"name\":\"snapshot\",\"type\":[{\"type\":\"string\",\"connect.version\":1,\"connect.parameters\":{\"allowed\":\"true,last,false,incremental\"},\"connect.default\":\"false\",\"connect.name\":\"io.debezium.data.Enum\"},\"null\"],\"default\":\"false\"},{\"name\":\"db\",\"type\":\"string\"},{\"name\":\"sequence\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"table\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_id\",\"type\":\"long\"},{\"name\":\"gtid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file\",\"type\":\"string\"},{\"name\":\"pos\",\"type\":\"long\"},{\"name\":\"row\",\"type\":\"int\"},{\"name\":\"thread\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"query\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"io.debezium.connector.mysql.Source\"}},{\"name\":\"op\",\"type\":\"string\"},{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"transaction\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"block\",\"namespace\":\"event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"total_order\",\"type\":\"long\"},{\"name\":\"data_collection_order\",\"type\":\"long\"}],\"connect.version\":1,\"connect.name\":\"event.block\"}],\"default\":null}],\"connect.version\":1,\"connect.name\":\"test_avro_precise.workdb.all_types_table.Envelope\"}";

    private static final String DEBEZIUM_AVRO = "debezium-avro";

    private Schema allTypesTableKeySchema;
    private Schema allTypesTableValueSchema;
    private Schema debeziumSourceSchema;

    @BeforeEach
    public void setup() {
        super.setup();
        // Init kafka key/value schema
        Schema.Parser parser = new Schema.Parser();
        allTypesTableKeySchema = parser.parse(ALL_TYPES_TABLE_KEY_SCHEMA);
        allTypesTableValueSchema = parser.parse(ALL_TYPES_TABLE_VALUE_SCHEMA);
        debeziumSourceSchema = allTypesTableValueSchema.getField("source").schema();
    }

    @Test
    @Timeout(60)
    public void testAllTypes() throws Exception {
        String topic = "test_avro_precise.workdb.all_types_table";
        createTestTopic(topic, 1, 1);

        // ---------- Write the Debezium avro into Kafka -------------------
        List<String> lines = readLines("kafka/debezium/table/schema/alltype/debezium-data-1.txt");
        writeByteRecordsToKafka(topic, lines, convertFunction());

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), DEBEZIUM_AVRO);
        kafkaConfig.put(TOPIC.key(), topic);
        kafkaConfig.put(SCHEMA_REGISTRY_URL.key(), getSchemaRegistryUrl());
        KafkaSyncTableAction action =
                syncTableActionBuilder(kafkaConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pt", "_id")
                        .withTableConfig(getBasicTableConfig())
                        .build();
        runActionWithDefaultEnv(action);

        waitingTables(tableName);
        FileStoreTable table = getFileStoreTable(tableName);

        RowType rowType = allTypesRowType(DataFormat.DEBEZIUM_AVRO);

        List<String> expected = expectedAllTypesResult();

        List<String> primaryKeys = Arrays.asList("pt", "_id");

        waitForResult(expected, table, rowType, primaryKeys);
    }

    private BiFunction<String, String, ProducerRecord<byte[], byte[]>> convertFunction() {
        return (topic, line) -> {
            try {
                JsonNode value = objectMapper.readTree(line);
                JsonNode valuePayload = value.get("payload");
                JsonNode source = valuePayload.get("source");
                JsonNode after = valuePayload.get("after");

                GenericRecord avroKey = new GenericData.Record(allTypesTableKeySchema);
                avroKey.put("_id", after.get("_id").asInt());

                GenericRecord avroValue = new GenericData.Record(allTypesTableValueSchema);
                Schema beforeSchema = allTypesTableValueSchema.getField("before").schema();
                GenericRecord afterAvroValue =
                        new GenericData.Record(sanitizedSchema(beforeSchema));
                afterAvroValue.put("_id", after.get("_id").asInt());
                afterAvroValue.put(
                        "pt",
                        ByteBuffer.wrap(
                                Base64Variants.getDefaultVariant()
                                        .decode(after.get("pt").asText())));
                if (nonNullNode(after.get("_bit1"))) {
                    afterAvroValue.put("_bit1", after.get("_bit1").asBoolean());
                }
                if (nonNullNode(after.get("_bit"))) {
                    afterAvroValue.put(
                            "_bit",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_bit").asText())));
                }
                if (nonNullNode(after.get("_tinyint1"))) {
                    afterAvroValue.put("_tinyint1", after.get("_tinyint1").asInt());
                }
                if (nonNullNode(after.get("_boolean"))) {
                    afterAvroValue.put("_boolean", after.get("_boolean").asInt());
                }
                if (nonNullNode(after.get("_bool"))) {
                    afterAvroValue.put("_bool", after.get("_bool").asInt());
                }
                if (nonNullNode(after.get("_tinyint"))) {
                    afterAvroValue.put("_tinyint", after.get("_tinyint").asInt());
                }
                if (nonNullNode(after.get("_tinyint_unsigned"))) {
                    afterAvroValue.put("_tinyint_unsigned", after.get("_tinyint_unsigned").asInt());
                }
                if (nonNullNode(after.get("_tinyint_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_tinyint_unsigned_zerofill",
                            after.get("_tinyint_unsigned_zerofill").asInt());
                }
                if (nonNullNode(after.get("_smallint"))) {
                    afterAvroValue.put("_smallint", after.get("_smallint").asInt());
                }
                if (nonNullNode(after.get("_smallint_unsigned"))) {
                    afterAvroValue.put(
                            "_smallint_unsigned", after.get("_smallint_unsigned").asInt());
                }
                if (nonNullNode(after.get("_smallint_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_smallint_unsigned_zerofill",
                            after.get("_smallint_unsigned_zerofill").asInt());
                }
                if (nonNullNode(after.get("_mediumint"))) {
                    afterAvroValue.put("_mediumint", after.get("_mediumint").asInt());
                }
                if (nonNullNode(after.get("_mediumint_unsigned"))) {
                    afterAvroValue.put(
                            "_mediumint_unsigned", after.get("_mediumint_unsigned").asInt());
                }
                if (nonNullNode(after.get("_mediumint_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_mediumint_unsigned_zerofill",
                            after.get("_mediumint_unsigned_zerofill").asInt());
                }
                if (nonNullNode(after.get("_int"))) {
                    afterAvroValue.put("_int", after.get("_int").asInt());
                }
                if (nonNullNode(after.get("_int_unsigned"))) {
                    afterAvroValue.put("_int_unsigned", after.get("_int_unsigned").asLong());
                }
                if (nonNullNode(after.get("_int_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_int_unsigned_zerofill", after.get("_int_unsigned_zerofill").asLong());
                }
                if (nonNullNode(after.get("_bigint"))) {
                    afterAvroValue.put("_bigint", after.get("_bigint").asLong());
                }
                if (nonNullNode(after.get("_bigint_unsigned"))) {
                    afterAvroValue.put(
                            "_bigint_unsigned",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_bigint_unsigned").asText())));
                }
                if (nonNullNode(after.get("_bigint_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_bigint_unsigned_zerofill",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(
                                                    after.get("_bigint_unsigned_zerofill")
                                                            .asText())));
                }
                afterAvroValue.put(
                        "_serial",
                        ByteBuffer.wrap(
                                Base64Variants.getDefaultVariant()
                                        .decode(after.get("_serial").asText())));
                if (nonNullNode(after.get("_float"))) {
                    afterAvroValue.put("_float", after.get("_float").floatValue());
                }
                if (nonNullNode(after.get("_float_unsigned"))) {
                    afterAvroValue.put(
                            "_float_unsigned", after.get("_float_unsigned").floatValue());
                }
                if (nonNullNode(after.get("_float_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_float_unsigned_zerofill",
                            after.get("_float_unsigned_zerofill").floatValue());
                }
                if (nonNullNode(after.get("_real"))) {
                    afterAvroValue.put("_real", after.get("_real").doubleValue());
                }
                if (nonNullNode(after.get("_real_unsigned"))) {
                    afterAvroValue.put("_real_unsigned", after.get("_real_unsigned").doubleValue());
                }
                if (nonNullNode(after.get("_real_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_real_unsigned_zerofill",
                            after.get("_real_unsigned_zerofill").doubleValue());
                }
                if (nonNullNode(after.get("_double"))) {
                    afterAvroValue.put("_double", after.get("_double").asDouble());
                }
                if (nonNullNode(after.get("_double_unsigned"))) {
                    afterAvroValue.put(
                            "_double_unsigned", after.get("_double_unsigned").asDouble());
                }
                if (nonNullNode(after.get("_double_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_double_unsigned_zerofill",
                            after.get("_double_unsigned_zerofill").asDouble());
                }
                if (nonNullNode(after.get("_double_precision"))) {
                    afterAvroValue.put(
                            "_double_precision", after.get("_double_precision").asDouble());
                }
                if (nonNullNode(after.get("_double_precision_unsigned"))) {
                    afterAvroValue.put(
                            "_double_precision_unsigned",
                            after.get("_double_precision_unsigned").asDouble());
                }
                if (nonNullNode(after.get("_double_precision_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_double_precision_unsigned_zerofill",
                            after.get("_double_precision_unsigned_zerofill").asDouble());
                }
                // Decimal types
                if (nonNullNode(after.get("_numeric"))) {
                    afterAvroValue.put(
                            "_numeric",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_numeric").asText())));
                }
                if (nonNullNode(after.get("_numeric_unsigned"))) {
                    afterAvroValue.put(
                            "_numeric_unsigned",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_numeric_unsigned").asText())));
                }
                if (nonNullNode(after.get("_numeric_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_numeric_unsigned_zerofill",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(
                                                    after.get("_numeric_unsigned_zerofill")
                                                            .asText())));
                }
                if (nonNullNode(after.get("_fixed"))) {
                    afterAvroValue.put(
                            "_fixed",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_fixed").asText())));
                }
                if (nonNullNode(after.get("_fixed_unsigned"))) {
                    afterAvroValue.put(
                            "_fixed_unsigned",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_fixed_unsigned").asText())));
                }
                if (nonNullNode(after.get("_fixed_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_fixed_unsigned_zerofill",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(
                                                    after.get("_fixed_unsigned_zerofill")
                                                            .asText())));
                }
                if (nonNullNode(after.get("_decimal"))) {
                    afterAvroValue.put(
                            "_decimal",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_decimal").asText())));
                }
                if (nonNullNode(after.get("_decimal_unsigned"))) {
                    afterAvroValue.put(
                            "_decimal_unsigned",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_decimal_unsigned").asText())));
                }
                if (nonNullNode(after.get("_decimal_unsigned_zerofill"))) {
                    afterAvroValue.put(
                            "_decimal_unsigned_zerofill",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(
                                                    after.get("_decimal_unsigned_zerofill")
                                                            .asText())));
                }
                if (nonNullNode(after.get("_big_decimal"))) {
                    afterAvroValue.put(
                            "_big_decimal",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_big_decimal").asText())));
                }
                // Date types
                if (nonNullNode(after.get("_date"))) {
                    afterAvroValue.put("_date", after.get("_date").asInt());
                }
                if (nonNullNode(after.get("_datetime"))) {
                    afterAvroValue.put("_datetime", after.get("_datetime").asLong());
                }
                if (nonNullNode(after.get("_datetime3"))) {
                    afterAvroValue.put("_datetime3", after.get("_datetime3").asLong());
                }
                if (nonNullNode(after.get("_datetime6"))) {
                    afterAvroValue.put("_datetime6", after.get("_datetime6").asLong());
                }
                if (nonNullNode(after.get("_datetime_p"))) {
                    afterAvroValue.put("_datetime_p", after.get("_datetime_p").asLong());
                }
                if (nonNullNode(after.get("_datetime_p2"))) {
                    afterAvroValue.put("_datetime_p2", after.get("_datetime_p2").asLong());
                }
                if (nonNullNode(after.get("_timestamp"))) {
                    afterAvroValue.put("_timestamp", after.get("_timestamp").asText());
                }
                if (nonNullNode(after.get("_timestamp0"))) {
                    afterAvroValue.put("_timestamp0", after.get("_timestamp0").asText());
                }
                // String types
                if (nonNullNode(after.get("_char"))) {
                    afterAvroValue.put("_char", after.get("_char").asText());
                }
                if (nonNullNode(after.get("_varchar"))) {
                    afterAvroValue.put("_varchar", after.get("_varchar").asText());
                }
                if (nonNullNode(after.get("_tinytext"))) {
                    afterAvroValue.put("_tinytext", after.get("_tinytext").asText());
                }
                if (nonNullNode(after.get("_text"))) {
                    afterAvroValue.put("_text", after.get("_text").asText());
                }
                if (nonNullNode(after.get("_mediumtext"))) {
                    afterAvroValue.put("_mediumtext", after.get("_mediumtext").asText());
                }
                if (nonNullNode(after.get("_longtext"))) {
                    afterAvroValue.put("_longtext", after.get("_longtext").asText());
                }
                // Bytes
                if (nonNullNode(after.get("_bin"))) {
                    afterAvroValue.put(
                            "_bin",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_bin").asText())));
                }
                if (nonNullNode(after.get("_varbin"))) {
                    afterAvroValue.put(
                            "_varbin",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_varbin").asText())));
                }
                if (nonNullNode(after.get("_tinyblob"))) {
                    afterAvroValue.put(
                            "_tinyblob",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_tinyblob").asText())));
                }
                if (nonNullNode(after.get("_blob"))) {
                    afterAvroValue.put(
                            "_blob",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_blob").asText())));
                }
                if (nonNullNode(after.get("_mediumblob"))) {
                    afterAvroValue.put(
                            "_mediumblob",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_mediumblob").asText())));
                }
                if (nonNullNode(after.get("_longblob"))) {
                    afterAvroValue.put(
                            "_longblob",
                            ByteBuffer.wrap(
                                    Base64Variants.getDefaultVariant()
                                            .decode(after.get("_longblob").asText())));
                }
                // Json
                if (nonNullNode(after.get("_json"))) {
                    afterAvroValue.put("_json", after.get("_json").asText());
                }
                // Enum
                if (nonNullNode(after.get("_enum"))) {
                    afterAvroValue.put("_enum", after.get("_enum").asText());
                }

                if (nonNullNode(after.get("_year"))) {
                    afterAvroValue.put("_year", after.get("_year").asInt());
                }
                if (nonNullNode(after.get("_time"))) {
                    afterAvroValue.put("_time", after.get("_time").asLong());
                }
                // Point
                JsonNode pointJsonValue = after.get("_point");
                if (nonNullNode(pointJsonValue)) {
                    Schema pointSchema =
                            sanitizedSchema(
                                    sanitizedSchema(beforeSchema).getField("_point").schema());
                    afterAvroValue.put("_point", buildPointRecord(pointJsonValue, pointSchema));
                }
                // Geometry
                JsonNode geometryJsonValue = after.get("_geometry");
                Schema geometrySchema =
                        sanitizedSchema(
                                sanitizedSchema(beforeSchema).getField("_geometry").schema());
                if (nonNullNode(geometryJsonValue)) {
                    afterAvroValue.put(
                            "_geometry", buildGeometryRecord(geometryJsonValue, geometrySchema));
                }

                JsonNode linestringJsonNode = after.get("_linestring");
                if (nonNullNode(linestringJsonNode)) {
                    afterAvroValue.put(
                            "_linestring", buildGeometryRecord(linestringJsonNode, geometrySchema));
                }

                JsonNode polygonJsonNode = after.get("_polygon");
                if (nonNullNode(polygonJsonNode)) {
                    afterAvroValue.put(
                            "_polygon", buildGeometryRecord(polygonJsonNode, geometrySchema));
                }

                JsonNode multipointJsonNode = after.get("_multipoint");
                if (nonNullNode(multipointJsonNode)) {
                    afterAvroValue.put(
                            "_multipoint", buildGeometryRecord(multipointJsonNode, geometrySchema));
                }

                JsonNode multilineJsonNode = after.get("_multiline");
                if (nonNullNode(multilineJsonNode)) {
                    afterAvroValue.put(
                            "_multiline", buildGeometryRecord(multilineJsonNode, geometrySchema));
                }

                JsonNode multipolygonJsonNode = after.get("_multipolygon");
                if (nonNullNode(multipolygonJsonNode)) {
                    afterAvroValue.put(
                            "_multipolygon",
                            buildGeometryRecord(multipolygonJsonNode, geometrySchema));
                }

                JsonNode geometrycollectionJsonNode = after.get("_geometrycollection");
                if (nonNullNode(geometrycollectionJsonNode)) {
                    afterAvroValue.put(
                            "_geometrycollection",
                            buildGeometryRecord(geometrycollectionJsonNode, geometrySchema));
                }
                // Set
                if (nonNullNode(after.get("_set"))) {
                    afterAvroValue.put("_set", after.get("_set").asText());
                }

                avroValue.put("after", afterAvroValue);
                // Common properties
                avroValue.put("source", buildDebeziumSourceRecord(debeziumSourceSchema, source));
                avroValue.put("op", valuePayload.get("op").asText());
                avroValue.put("ts_ms", valuePayload.get("ts_ms").asLong());

                return new ProducerRecord<>(
                        topic,
                        kafkaKeyAvroSerializer.serialize(topic, avroKey),
                        kafkaValueAvroSerializer.serialize(topic, avroValue));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private boolean nonNullNode(JsonNode jsonNode) {
        return jsonNode != null && jsonNode.getNodeType() != JsonNodeType.NULL;
    }

    private GenericRecord buildPointRecord(JsonNode pointJsonValue, Schema debeziumPointSchema) {
        GenericRecord pointAvroValue = new GenericData.Record(debeziumPointSchema);
        pointAvroValue.put("x", pointJsonValue.get("x").asDouble());
        pointAvroValue.put("y", pointJsonValue.get("y").asDouble());
        pointAvroValue.put(
                "wkb",
                ByteBuffer.wrap(
                        Base64Variants.getDefaultVariant()
                                .decode(pointJsonValue.get("wkb").asText())));
        pointAvroValue.put(
                "srid",
                pointJsonValue.get("srid") != null ? pointJsonValue.get("srid").asInt() : null);
        return pointAvroValue;
    }

    private GenericRecord buildGeometryRecord(
            JsonNode geometryJsonValue, Schema debeziumGeometrySchema) {
        GenericRecord geometryAvroValue = new GenericData.Record(debeziumGeometrySchema);
        geometryAvroValue.put(
                "wkb",
                ByteBuffer.wrap(
                        Base64Variants.getDefaultVariant()
                                .decode(geometryJsonValue.get("wkb").asText())));
        geometryAvroValue.put(
                "srid",
                geometryJsonValue.get("srid") != null
                        ? geometryJsonValue.get("srid").asInt()
                        : null);
        return geometryAvroValue;
    }
}
