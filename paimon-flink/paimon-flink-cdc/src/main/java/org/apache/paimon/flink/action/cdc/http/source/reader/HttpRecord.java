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

package org.apache.paimon.flink.action.cdc.http.source.reader;

import java.io.Serializable;
import java.util.Objects;

/** Represent http response record. */
public class HttpRecord implements Serializable {
    private String httpUrl;
    private long timestamp;
    private String data;

    public HttpRecord() {}

    public HttpRecord(String httpUrl, long timestamp, String data) {
        this.httpUrl = httpUrl;
        this.timestamp = timestamp;
        this.data = data;
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

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HttpRecord)) {
            return false;
        }
        HttpRecord that = (HttpRecord) o;
        return Objects.equals(httpUrl, that.httpUrl)
                && timestamp == that.timestamp
                && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(httpUrl, timestamp, data);
    }

    @Override
    public String toString() {
        return "HttpRecord{httpUrl='"
                + httpUrl
                + ", timestamp="
                + timestamp
                + ", data='"
                + data
                + "'}";
    }
}
