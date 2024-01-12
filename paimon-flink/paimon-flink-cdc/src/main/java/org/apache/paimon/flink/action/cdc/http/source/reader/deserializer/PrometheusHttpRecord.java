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

package org.apache.paimon.flink.action.cdc.http.source.reader.deserializer;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Represent prometheus http response record. */
public class PrometheusHttpRecord implements Serializable {
    private String httpUrl;
    private long timestamp;
    private String metricName;
    private Map<String, String> labels;
    private String value;

    public PrometheusHttpRecord() {}

    public PrometheusHttpRecord(
            String httpUrl,
            long timestamp,
            String metricName,
            Map<String, String> labels,
            String value) {
        this.httpUrl = httpUrl;
        this.timestamp = timestamp;
        this.metricName = metricName;
        this.labels = labels;
        this.value = value;
    }

    public String getHttpUrl() {
        return httpUrl;
    }

    public void setHttpUrl(String httpUrl) {
        this.httpUrl = httpUrl;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<String> labelKeys() {
        return labels.keySet().stream().sorted().collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PrometheusHttpRecord)) {
            return false;
        }
        PrometheusHttpRecord that = (PrometheusHttpRecord) o;
        return Objects.equals(httpUrl, that.httpUrl)
                && timestamp == that.timestamp
                && Objects.equals(metricName, that.metricName)
                && Objects.equals(labels, that.labels)
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(httpUrl, timestamp, metricName, labels, value);
    }

    @Override
    public String toString() {
        return "PrometheusHttpRecord{httpUrl='"
                + httpUrl
                + "', timestamp="
                + timestamp
                + ", metricName='"
                + metricName
                + "', labels="
                + labels
                + ", value='"
                + value
                + "'}";
    }
}
