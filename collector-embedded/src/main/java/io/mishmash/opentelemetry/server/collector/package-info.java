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
 * <h2>OpenTelemetry collectors</h2>
 * <p>
 * This package contains the main classes used to embed OpenTelemetry
 * collectors. Use one or all of {@link LogsCollector},
 * {@link MetricsCollector} and {@link TracesCollector} to create
 * an OTEL data source for your system.
 * </p>
 * <p>
 * To use them:
 * <ol>
 * <li>
 * Instantiate one of the collectors
 * </li>
 * <li>
 * Implement one or more subscribers - {@link LogsSubscriber},
 * {@link MetricsSubscriber}, {@link SpansSubscriber} to receive
 * incoming OpenTelemetry signals of a given type
 * </li>
 * <li>
 * Subscribe them to the collector -
 * {@link AbstractCollector#subscribe(java.util.concurrent.Flow.Subscriber)}
 * </li>
 * <li>
 * Setup gRPC and HTTP Vert.x servers and bind the collector to them -
 * {@link AbstractCollector#bind(io.vertx.grpc.server.GrpcServer)} and
 * {@link AbstractCollector#bind(io.vertx.ext.web.Router)}.
 * </li>
 * </ol>
 *
 * <p>
 * When OTLP clients connect to the server and submit data - each individual
 * data item in them - a log entry, a metric data point or a span - will be
 * delivered to the subscribers' {@link LogsSubscriber#onNext(Log)},
 * {@link MetricsSubscriber#onNext(MetricDataPoint)} or
 * {@link SpansSubscriber#onNext(Span)} where you can make it available
 * within the system you're embedding into.
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
package io.mishmash.opentelemetry.server.collector;
