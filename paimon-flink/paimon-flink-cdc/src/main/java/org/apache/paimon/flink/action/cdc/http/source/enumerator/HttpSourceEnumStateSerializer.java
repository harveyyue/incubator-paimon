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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** The {@link SimpleVersionedSerializer Serializer} for the enumerator state of Http source. */
public class HttpSourceEnumStateSerializer
        implements SimpleVersionedSerializer<HttpSourceEnumState> {
    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(HttpSourceEnumState enumState) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            out.writeInt(enumState.assignedUrls().size());
            for (String url : enumState.assignedUrls()) {
                out.writeUTF(url);
            }
            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public HttpSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        if (version == CURRENT_VERSION) {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {

                final int size = in.readInt();
                Set<String> urls = new HashSet<>(size);
                for (int i = 0; i < size; i++) {
                    urls.add(in.readUTF());
                }

                return new HttpSourceEnumState(urls);
            }
        }

        throw new IOException(
                String.format(
                        "The bytes are serialized with version %d, "
                                + "while this deserializer only supports version up to %d",
                        version, CURRENT_VERSION));
    }
}
