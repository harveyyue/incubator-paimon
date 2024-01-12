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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Configurations for {@link HttpSource}. * */
public class HttpSourceOptions {

    public static final ConfigOption<String> HTTP_URLS =
            ConfigOptions.key("http-urls")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Http urls, using comma as delimiter if have multiple urls.");

    public static final ConfigOption<String> HTTP_EXECUTE_MODE =
            ConfigOptions.key("http-execute-mode")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("Http execute mode, support mode: default, prometheus.");

    public static final ConfigOption<String> INCLUDE_METRICS_REGEX =
            ConfigOptions.key("include-metrics-regex")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Provide the include metrics regex in prometheus mode if need.");

    public static final ConfigOption<Long> HTTP_EXECUTE_INITIAL_SECONDS =
            ConfigOptions.key("http-execute-initial-seconds")
                    .longType()
                    .defaultValue(10L)
                    .withDescription("The time to delay first execution.");

    public static final ConfigOption<Long> HTTP_EXECUTE_INTERVAL_SECONDS =
            ConfigOptions.key("http-execute-interval-seconds")
                    .longType()
                    .defaultValue(60L)
                    .withDescription(
                            "The delay between the termination of one execution and the commencement of the next.");
}
