/*
 *    Copyright 2024 Mishmash IO UK Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

/**
 * <h2>Stand-alone OpenTelemetry server</h2>
 * <p>
 * This package contains the implementation of a simple stand-alone
 * OpenTelemetry server that writes incoming logs, metrics and traces
 * to Apache Parquet files.
 * </p>
 * <p>
 * Use {@link CollectorsMain#main(String[])} to start it. This will
 * instantiate the embeddable collectors -
 * {@link io.mishmash.opentelemetry.server.collector.LogsCollector},
 * {@link io.mishmash.opentelemetry.server.collector.MetricsCollector} and
 * {@link io.mishmash.opentelemetry.server.collector.TracesCollector},
 * bind them to an Eclipse Vert.x server (for both gRPC and HTTP traffic)
 * and start accepting incoming connections.
 * </p>
 * <p>
 * For each individual type of records - logs, metrics or traces -
 * a subscriber is added to perform the actual writing to output files.
 * These are implemented in {@link FileLogs}, {@link FileMetrics} and
 * {@link FileSpans}.
 * </p>
 * <p>
 * <b>
 * WARNING: This server does not include any kind of authentication
 * or authorization, its server ports are completely open to everyone.
 * </b>
 * Use it for testing locally or as an example of how to embed the
 * collectors as a data source in another system.
 * </p>
 * <p>
 * This package is part of a broader set of OpenTelemetry-related activities
 * at mishmash io. To find out more about this and other packages visit the
 * links below.
 * </p>
 * @see <a href="https://mishmash.io/open_source/opentelemetry">
 * OpenTelemetry at mishmash io</a>
 * @see <a href="https://github.com/mishmash-io/opentelemetry-server-embedded">
 * This package on GitHub</a>
 * @see <a href="https://mishmash.io/">mishmash io</a>
 */
package io.mishmash.opentelemetry.server.parquet;
