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

package org.apache.paimon.flink.action.cdc.http.source.enumerator;

import org.apache.paimon.flink.action.cdc.http.source.split.HttpSplit;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.util.FlinkRuntimeException;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** The enumerator class for Http source. */
public class HttpSplitEnumerator implements SplitEnumerator<HttpSplit, HttpSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSplitEnumerator.class);
    private final Set<String> httpUrls;
    private final SplitEnumeratorContext<HttpSplit> context;
    private final Boundedness boundedness;
    private final Set<String> assignedSplits;
    private final Map<Integer, Set<HttpSplit>> pendingSplitAssignment;

    public HttpSplitEnumerator(
            Set<String> httpUrls,
            SplitEnumeratorContext<HttpSplit> context,
            Boundedness boundedness) {
        this.httpUrls = httpUrls;
        this.context = context;
        this.boundedness = boundedness;
        this.assignedSplits = new HashSet<>();
        this.pendingSplitAssignment = new HashMap<>();
    }

    @Override
    public void start() {
        context.callAsync(this::getHttpUrls, this::assignHttpUrls);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void addSplitsBack(List<HttpSplit> splits, int subtaskId) {
        addSplitChangeToPendingAssignments(splits);

        // If the failed subtask has already restarted, we need to assign pending splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            assignPendingSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Adding reader {} to HttpSplitEnumerator.", subtaskId);
        assignPendingSplits(Collections.singleton(subtaskId));
    }

    @Override
    public HttpSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new HttpSourceEnumState(assignedSplits);
    }

    @Override
    public void close() throws IOException {}

    // --------------- private class ---------------
    private Set<String> getHttpUrls() {
        return httpUrls;
    }

    private void assignHttpUrls(Set<String> httpUrls, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list http urls due to ", t);
        }

        List<HttpSplit> splits = httpUrls.stream().map(HttpSplit::new).collect(Collectors.toList());
        addSplitChangeToPendingAssignments(splits);
        assignPendingSplits(context.registeredReaders().keySet());
    }

    // This method should only be invoked in the coordinator executor thread.
    private void addSplitChangeToPendingAssignments(Collection<HttpSplit> newPrometheusHttpSplits) {
        int numReaders = context.currentParallelism();
        for (HttpSplit split : newPrometheusHttpSplits) {
            int ownerReader = getSplitOwner(split.splitId(), numReaders);
            pendingSplitAssignment.computeIfAbsent(ownerReader, r -> new HashSet<>()).add(split);
        }
    }

    // This method should only be invoked in the coordinator executor thread.
    private void assignPendingSplits(Set<Integer> pendingReaders) {
        Map<Integer, List<HttpSplit>> incrementalAssignment = new HashMap<>();

        // Check if there's any pending splits for given readers
        for (int pendingReader : pendingReaders) {
            checkReaderRegistered(pendingReader);

            // Remove pending assignment for the reader
            final Set<HttpSplit> pendingAssignmentForReader =
                    pendingSplitAssignment.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                incrementalAssignment
                        .computeIfAbsent(pendingReader, r -> new ArrayList<>())
                        .addAll(pendingAssignmentForReader);

                pendingAssignmentForReader.forEach(split -> assignedSplits.add(split.splitId()));
            }
        }

        // Assign pending splits to readers
        if (!incrementalAssignment.isEmpty()) {
            LOG.info("Assigning splits to readers {}", incrementalAssignment);
            context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        }
    }

    private int getSplitOwner(String httpUrl, int numReaders) {
        return ((httpUrl.hashCode() * 31) & 0x7FFFFFFF) % numReaders;
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }
}
