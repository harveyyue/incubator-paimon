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

package org.apache.paimon.flink.action.cdc.http.source.util;

import org.apache.paimon.flink.action.cdc.http.source.reader.HttpRecord;
import org.apache.paimon.flink.action.cdc.http.source.reader.deserializer.PrometheusHttpRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Utils for Http. */
public class HttpUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

    public static final Pattern PROMETHEUS_FORMAT_PATTERN =
            Pattern.compile("(.*)\\{(.*)}\\s+([0-9-.EeNa]+)");
    public static final Pattern PROMETHEUS_COMMENT_PATTERN = Pattern.compile("^#.*");
    public static final Pattern BREAKLINE_PATTERN = Pattern.compile("\\n");
    public static final Pattern COMMA_PATTERN = Pattern.compile(",");
    public static final Pattern EQUAL_PATTERN = Pattern.compile("=");

    public static List<PrometheusHttpRecord> parse(HttpRecord record) {
        return Arrays.stream(BREAKLINE_PATTERN.split(record.getData()))
                .map(
                        line -> {
                            PrometheusHttpRecord result = null;
                            Matcher matcher = PROMETHEUS_FORMAT_PATTERN.matcher(line);
                            if (matcher.matches()) {
                                String metricName = matcher.group(1);
                                String label = matcher.group(2);
                                Map<String, String> labels = new HashMap<>();
                                if (label != null) {
                                    Arrays.stream(COMMA_PATTERN.split(label))
                                            .forEach(
                                                    pair -> {
                                                        String[] pairs = EQUAL_PATTERN.split(pair);
                                                        if (pairs.length == 2) {
                                                            labels.put(
                                                                    pairs[0].trim(),
                                                                    pairs[1].replace("\"", "")
                                                                            .trim());
                                                        }
                                                    });
                                }
                                String value = matcher.group(3);
                                result =
                                        new PrometheusHttpRecord(
                                                record.getHttpUrl(),
                                                record.getTimestamp(),
                                                metricName,
                                                labels,
                                                value);
                            } else {
                                LOG.warn("Skip unparsed line: {}", line);
                            }
                            return result;
                        })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
