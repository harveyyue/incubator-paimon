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

package org.apache.paimon.flink.action.cdc.http.source;

import org.apache.paimon.flink.action.cdc.http.source.reader.strategy.DefaultHttpExecuteStrategy;
import org.apache.paimon.flink.action.cdc.http.source.reader.strategy.HttpExecuteStrategy;
import org.apache.paimon.flink.action.cdc.http.source.reader.strategy.PrometheusHttpExecuteStrategy;

import java.io.Serializable;
import java.util.Set;
import java.util.regex.Pattern;

/** A Http Source configuration which is used by {@link HttpSource}. */
public class HttpSourceConfig implements Serializable {
    private final Set<String> httpUrls;
    private final HttpExecuteMode httpExecuteMode;
    private final HttpExecuteStrategy httpExecuteStrategy;
    private final Pattern includeMetricPattern;
    private final long httpExecuteInitialSeconds;
    private final long httpExecuteIntervalSeconds;

    public HttpSourceConfig(
            Set<String> httpUrls,
            HttpExecuteMode httpExecuteMode,
            String includeMetricsRegex,
            long httpExecuteInitialSeconds,
            long httpExecuteIntervalSeconds) {
        this.httpUrls = httpUrls;
        this.httpExecuteMode = httpExecuteMode;
        this.httpExecuteStrategy = buildHttpExecuteStrategy();
        this.includeMetricPattern =
                includeMetricsRegex != null
                        ? Pattern.compile(includeMetricsRegex, Pattern.CASE_INSENSITIVE)
                        : null;
        this.httpExecuteInitialSeconds = httpExecuteInitialSeconds;
        this.httpExecuteIntervalSeconds = httpExecuteIntervalSeconds;
    }

    public Set<String> getHttpUrls() {
        return httpUrls;
    }

    private HttpExecuteStrategy buildHttpExecuteStrategy() {
        if (httpExecuteMode.equals(HttpExecuteMode.PROMETHEUS)) {
            return new PrometheusHttpExecuteStrategy(this);
        }
        return new DefaultHttpExecuteStrategy(this);
    }

    public HttpExecuteStrategy getHttpExecuteStrategy() {
        return httpExecuteStrategy;
    }

    public Pattern getIncludeMetricsPattern() {
        return includeMetricPattern;
    }

    public long getHttpExecuteInitialSeconds() {
        return httpExecuteInitialSeconds;
    }

    public long getHttpExecuteIntervalSeconds() {
        return httpExecuteIntervalSeconds;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** A factory to construct {@link HttpSourceConfig}. */
    public static class Builder {
        private Set<String> httpUrls;
        private HttpExecuteMode httpExecuteMode = HttpExecuteMode.DEFAULT;
        private String includeMetricsRegex;
        private long httpExecuteInitialSeconds = 10L;
        private long httpExecuteIntervalSeconds = 60L;

        public Builder httpUrls(Set<String> httpUrls) {
            this.httpUrls = httpUrls;
            return this;
        }

        public Builder httpExecuteMode(String mode) {
            this.httpExecuteMode = HttpExecuteMode.getHttpExecuteMode(mode);
            return this;
        }

        public Builder includeMetricsRegex(String includeMetricsRegex) {
            this.includeMetricsRegex = includeMetricsRegex;
            return this;
        }

        public Builder httpExecuteInitialSeconds(long httpExecuteInitialSeconds) {
            this.httpExecuteInitialSeconds = httpExecuteInitialSeconds;
            return this;
        }

        public Builder httpExecuteIntervalSeconds(long httpExecuteIntervalSeconds) {
            this.httpExecuteIntervalSeconds = httpExecuteIntervalSeconds;
            return this;
        }

        public HttpSourceConfig build() {
            return new HttpSourceConfig(
                    httpUrls,
                    httpExecuteMode,
                    includeMetricsRegex,
                    httpExecuteInitialSeconds,
                    httpExecuteIntervalSeconds);
        }
    }

    /** An enum for {@link HttpSourceConfig}. */
    public enum HttpExecuteMode {
        DEFAULT,
        PROMETHEUS;

        public static HttpExecuteMode getHttpExecuteMode(String mode) {
            for (HttpExecuteMode value : HttpExecuteMode.values()) {
                if (value.name().equalsIgnoreCase(mode)) {
                    return value;
                }
            }
            throw new IllegalArgumentException("Not support http execute mode: " + mode);
        }
    }
}
