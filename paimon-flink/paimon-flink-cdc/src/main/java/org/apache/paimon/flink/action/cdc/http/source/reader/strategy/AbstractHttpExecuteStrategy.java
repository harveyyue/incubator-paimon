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

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** An abstract implementation for {@link HttpExecuteStrategy}. */
public abstract class AbstractHttpExecuteStrategy implements HttpExecuteStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHttpExecuteStrategy.class);

    protected HttpSourceConfig config;
    private transient OkHttpClient okHttpClient;

    public AbstractHttpExecuteStrategy(HttpSourceConfig config) {
        this.config = config;
    }

    @Override
    public void open() {
        if (okHttpClient == null) {
            okHttpClient = defaultOkHttpClient();
        }
    }

    @Override
    public HttpRecord execute(HttpSplit httpSplit) {
        HttpRecord result = null;
        try (Response response =
                okHttpClient
                        .newCall(new Request.Builder().url(httpSplit.splitId()).get().build())
                        .execute()) {
            if (response.isSuccessful() && response.body() != null) {
                result = convert(httpSplit, response.body().string());
            }
        } catch (IOException ex) {
            LOG.error("Unexpect failed:", ex);
        }
        return result;
    }

    protected HttpRecord convert(HttpSplit httpSplit, String data) {
        return new HttpRecord(httpSplit.splitId(), System.currentTimeMillis(), data);
    }

    private OkHttpClient defaultOkHttpClient() {
        return new OkHttpClient.Builder()
                .readTimeout(180, TimeUnit.SECONDS)
                .writeTimeout(180, TimeUnit.SECONDS)
                .build();
    }
}
