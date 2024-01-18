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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Expression.FromUnixtime}. */
public class FromUnixtimeComputerTest {

    private static Object[][] prepareData() {
        return new Object[][] {
            {"computedColumnField", "1705563152342", "", "2024-01-18 15:32:32.342"},
            {"computedColumnField", "1705563152", "", "2024-01-18 15:32:32"},
            {"computedColumnField", "1705563152342123", "", "2024-01-18 15:32:32.342123"},
            {"computedColumnField", "1705563152342", "UTC", "2024-01-18 07:32:32.342"},
            {"computedColumnField", "1705563152", "UTC", "2024-01-18 07:32:32"},
            {"computedColumnField", "1705563152342123", "UTC", "2024-01-18 07:32:32.342123"},
        };
    }

    @Test
    public void testFromUnixtime() {
        Object[][] testData = prepareData();
        for (Object[] testDatum : testData) {
            String fieldReference = (String) testDatum[0];
            String value = (String) testDatum[1];
            String pattern = testDatum[2].equals("") ? null : (String) testDatum[2];
            String expected = (String) testDatum[3];

            Expression fromUnixtime = Expression.fromUnixtime(fieldReference, pattern);
            assertThat(fromUnixtime.eval(value)).isEqualTo(expected);
        }
    }

    @Test
    public void testTruncateWithException() {
        String fieldReference = "computedColumnField";
        String value = "x123456789";

        Expression fromUnixtime = Expression.fromUnixtime(fieldReference);

        assertThatThrownBy(() -> fromUnixtime.eval(value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid value for from_unixtime function: " + value + ".");
    }
}
