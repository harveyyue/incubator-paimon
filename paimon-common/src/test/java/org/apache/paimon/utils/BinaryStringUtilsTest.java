/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.TimestampType;

import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Test of {@link BinaryStringUtils}. */
class BinaryStringUtilsTest {

    @Test
    void toTimestampValidUnixEpochTimestamp() {
        String s = "1596240000000";
        BinaryString str = BinaryString.fromString(s);
        TimestampType timestampType = new TimestampType(true, 3);
        long timestampMillis = Long.parseLong(s); // Unix Epoch timestamp in milliseconds
        Timestamp expectedTimestamp = Timestamp.fromEpochMillis(timestampMillis);
        assertEquals(
                expectedTimestamp,
                BinaryStringUtils.toTimestamp(str, timestampType.getPrecision()));
    }

    @Test
    void toTimestampInvalidUnixEpochTimestamp() {
        String s = "invalid_epoch";
        BinaryString str = BinaryString.fromString(s);
        TimestampType timestampType = new TimestampType(true, 3);
        assertThrows(
                DateTimeException.class,
                () -> BinaryStringUtils.toTimestamp(str, timestampType.getPrecision()));
    }

    @Test
    void toTimestampValidTimestampString() {
        String s = "2020-08-01 00:00:00";
        BinaryString str = BinaryString.fromString(s);
        TimestampType timestampType = new TimestampType(true, 3);
        // Convert java.sql.Timestamp to your custom Timestamp type
        java.sql.Timestamp sqlTimestamp =
                java.sql.Timestamp.valueOf(LocalDateTime.of(2020, 8, 1, 0, 0, 0));
        Timestamp expectedTimestampValue = Timestamp.fromSQLTimestamp(sqlTimestamp);
        assertEquals(
                expectedTimestampValue,
                BinaryStringUtils.toTimestamp(str, timestampType.getPrecision()));
    }

    @Test
    void toTimestampInvalidTimestampString() {
        String s = "01-08-2020 00:00";
        BinaryString str = BinaryString.fromString(s);
        TimestampType timestampType = new TimestampType(true, 3);
        assertThrows(
                DateTimeException.class,
                () -> BinaryStringUtils.toTimestamp(str, timestampType.getPrecision()));
    }

    @Test
    void toTimestampUnixEpochTimestamp() {
        String s = "1706774399999";
        BinaryString str = BinaryString.fromString(s);
        TimestampType timestampType = new TimestampType(true, 3);
        BinaryStringUtils.toTimestamp(str, timestampType.getPrecision());
    }

    @Test
    void toDateValidUnixEpoch() {
        String s = "1596240000000"; // Represents a specific date in Unix Epoch milliseconds
        BinaryString str = BinaryString.fromString(s);
        long timestamp = Long.parseLong(s); // Unix Epoch timestamp in milliseconds
        LocalDate date = Instant.ofEpochMilli(timestamp).atZone(ZoneId.of("UTC")).toLocalDate();
        long daysSinceEpoch = date.toEpochDay();
        assertEquals(daysSinceEpoch, BinaryStringUtils.toDate(str));
    }

    @Test
    void toDateInvalidUnixEpoch() {
        String s = "abc123";
        BinaryString str = BinaryString.fromString(s);
        assertThrows(DateTimeException.class, () -> BinaryStringUtils.toDate(str));
    }

    @Test
    void toDateValidDateString() {
        String s = "2020-08-01";
        BinaryString str = BinaryString.fromString(s);
        LocalDate epoch = LocalDate.of(1970, 1, 1);
        LocalDate date = LocalDate.of(2020, 8, 1);
        long daysSinceEpoch = ChronoUnit.DAYS.between(epoch, date);
        assertEquals(daysSinceEpoch, BinaryStringUtils.toDate(str));
    }

    @Test
    void toDateInvalidDateString() {
        String s = "01-08-2020";
        BinaryString str = BinaryString.fromString(s);
        assertThrows(DateTimeException.class, () -> BinaryStringUtils.toDate(str));
    }

    @Test
    void toTimeValidUnixEpoch() {
        String s = "82800000"; // Specific time in Unix Epoch milliseconds (23:00:00 UTC)
        BinaryString str = BinaryString.fromString(s);
        int expectedTimeValue =
                82800 * 1000; // 23 hours * 3600 seconds/hour * 1000 milliseconds/second
        assertEquals(expectedTimeValue, BinaryStringUtils.toTime(str));
    }

    @Test
    void toTimeInvalidUnixEpoch() {
        String s = "xyz789";
        BinaryString str = BinaryString.fromString(s);
        assertThrows(DateTimeException.class, () -> BinaryStringUtils.toTime(str));
    }

    @Test
    void toTimeValidFormattedTimeString() {
        // Assuming the formatted time string is in "HH:mm:ss" format
        String s = "23:00:00"; // 23 hours, 0 minutes, 0 seconds
        BinaryString str = BinaryString.fromString(s);
        int expectedTimeValue = 82800000; // 23 hours * 3600 seconds/hour * 1000 milliseconds/second
        assertEquals(expectedTimeValue, BinaryStringUtils.toTime(str));
    }

    @Test
    void toTimeInvalidTimeString() {
        String s = "11 PM";
        BinaryString str = BinaryString.fromString(s);
        assertThrows(DateTimeException.class, () -> BinaryStringUtils.toTime(str));
    }
}
