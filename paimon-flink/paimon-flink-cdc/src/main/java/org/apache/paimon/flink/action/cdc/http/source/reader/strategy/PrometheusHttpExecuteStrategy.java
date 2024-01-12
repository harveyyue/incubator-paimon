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

package org.apache.paimon.flink.action.cdc.http.source.reader.strategy;

import org.apache.paimon.flink.action.cdc.http.source.HttpSourceConfig;
import org.apache.paimon.flink.action.cdc.http.source.reader.HttpRecord;
import org.apache.paimon.flink.action.cdc.http.source.split.HttpSplit;
import org.apache.paimon.flink.action.cdc.http.source.util.HttpUtils;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Prometheus type implementation for {@link AbstractHttpExecuteStrategy}. */
public class PrometheusHttpExecuteStrategy extends AbstractHttpExecuteStrategy {

    private transient Predicate<String> filterPredicate;

    public PrometheusHttpExecuteStrategy(HttpSourceConfig config) {
        super(config);
    }

    @Override
    protected HttpRecord convert(HttpSplit httpSplit, String data) {
        if (filterPredicate == null) {
            filterPredicate = buildFilterPredicate(config);
        }
        data =
                Arrays.stream(HttpUtils.BREAKLINE_PATTERN.split(data))
                        .filter(filterPredicate)
                        .collect(Collectors.joining("\n"));
        return super.convert(httpSplit, data);
    }

    private Predicate<String> buildFilterPredicate(HttpSourceConfig config) {
        return config.getIncludeMetricsPattern() != null
                ? line -> config.getIncludeMetricsPattern().matcher(line).matches()
                : line -> !HttpUtils.PROMETHEUS_COMMENT_PATTERN.matcher(line).matches();
    }
}
