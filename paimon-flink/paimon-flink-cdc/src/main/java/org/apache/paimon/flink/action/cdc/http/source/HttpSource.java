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

import org.apache.paimon.flink.action.cdc.http.source.enumerator.HttpSourceEnumState;
import org.apache.paimon.flink.action.cdc.http.source.enumerator.HttpSourceEnumStateSerializer;
import org.apache.paimon.flink.action.cdc.http.source.enumerator.HttpSplitEnumerator;
import org.apache.paimon.flink.action.cdc.http.source.reader.HttpRecord;
import org.apache.paimon.flink.action.cdc.http.source.reader.HttpRecordEmitter;
import org.apache.paimon.flink.action.cdc.http.source.reader.HttpSourceReader;
import org.apache.paimon.flink.action.cdc.http.source.reader.HttpSplitReader;
import org.apache.paimon.flink.action.cdc.http.source.reader.deserializer.HttpRecordDeserializationSchema;
import org.apache.paimon.flink.action.cdc.http.source.split.HttpSplit;
import org.apache.paimon.flink.action.cdc.http.source.split.HttpSplitSerializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

/** The Source implementation of Http Get Request. */
public class HttpSource<OUT>
        implements Source<OUT, HttpSplit, HttpSourceEnumState>, ResultTypeQueryable<OUT> {

    private final HttpSourceConfig httpSourceConfig;
    private final Set<String> httpUrls;
    private final Boundedness boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
    private final HttpRecordDeserializationSchema<OUT> deserializationSchema;

    public HttpSource(
            HttpSourceConfig httpSourceConfig,
            HttpRecordDeserializationSchema<OUT> deserializationSchema) {
        this.httpSourceConfig = httpSourceConfig;
        this.httpUrls = httpSourceConfig.getHttpUrls();
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<OUT, HttpSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<HttpRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        Supplier<HttpSplitReader> splitReaderSupplier = () -> new HttpSplitReader(httpSourceConfig);
        HttpRecordEmitter<OUT> recordEmitter = new HttpRecordEmitter<>(deserializationSchema);
        Configuration config = toConfiguration(new Properties());

        return new HttpSourceReader<>(
                elementsQueue,
                new SingleThreadFetcherManager(elementsQueue, splitReaderSupplier, config),
                recordEmitter,
                config,
                readerContext);
    }

    @Override
    public SplitEnumerator<HttpSplit, HttpSourceEnumState> createEnumerator(
            SplitEnumeratorContext<HttpSplit> enumContext) throws Exception {
        return new HttpSplitEnumerator(httpUrls, enumContext, boundedness);
    }

    @Override
    public SplitEnumerator<HttpSplit, HttpSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<HttpSplit> enumContext, HttpSourceEnumState checkpoint)
            throws Exception {
        return new HttpSplitEnumerator(checkpoint.assignedUrls(), enumContext, boundedness);
    }

    @Override
    public SimpleVersionedSerializer<HttpSplit> getSplitSerializer() {
        return new HttpSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<HttpSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new HttpSourceEnumStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    // ----------- private helper methods ---------------

    private Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }
}
