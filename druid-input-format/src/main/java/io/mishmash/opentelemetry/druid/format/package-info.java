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
 * <h2>OpenTelemetry input format for Apache Druid</h2>
 * <p>
 * Use this input format to ingest OTLP (OpenTelemetry protocol) data
 * into Apache Druid.
 * </p>
 * <p>
 * Combine with an input source (Kafka, etc).
 * </p>
 * <p>
 * For more information on how to use this input format visit
 * @see <a href="https://github.com/mishmash-io/opentelemetry-server-embedded/druid-input-format">
 * OTLP Input Format for Apache Druid on GitHub.</a>
 * <p>
 * Apache Druid is a high performance, real-time analytics database that
 * delivers sub-second queries on streaming and batch data at scale
 * and under load. These features make it perfect for OpenTelemetry analytics.
 * To find out more about Apache Druid
 * @see <a href="https://druid.apache.org/">visit its website.</a>
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
package io.mishmash.opentelemetry.druid.format;
