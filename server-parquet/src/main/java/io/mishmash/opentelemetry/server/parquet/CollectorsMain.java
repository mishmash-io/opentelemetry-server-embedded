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

package io.mishmash.opentelemetry.server.parquet;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.mishmash.opentelemetry.server.collector.LogsCollector;
import io.mishmash.opentelemetry.server.collector.MetricsCollector;
import io.mishmash.opentelemetry.server.collector.TracesCollector;
import io.mishmash.opentelemetry.server.collector.Instrumentation;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Launcher;
import io.vertx.ext.web.AllowForwardHeaders;
import io.vertx.ext.web.Router;
import io.vertx.grpc.server.GrpcServer;

/**
 * Main class that launches the OpenTelemetry server and
 * embeds all collectors and their parquet subscribers.
 */
public class CollectorsMain extends AbstractVerticle {

    /**
     * The {@link java.util.logging.Logger} instance used.
     */
    private static final Logger LOG =
            Logger.getLogger(CollectorsMain.class.getName());

    /**
     * Default OpenTelemetry gRPC port.
     */
    public static final int DEFAULT_GRPC_PORT = 4317;
    /**
     * Default OpenTelemetry HTTP port.
     */
    public static final int DEFAULT_HTTP_PORT = 4318;

    /**
     * An {@link io.mishmash.opentelemetry.server.collector.Instrumentation}
     * helper to manage our own telemetry.
     */
    private static Instrumentation otel = new Instrumentation();

    /**
     * The OpenTelemetry logs collector.
     */
    private LogsCollector logsCollector =
            new LogsCollector(LogsServiceGrpc.getExportMethod(), otel);
    /**
     * The OpenTelemetry metrics collector.
     */
    private MetricsCollector metricsCollector =
            new MetricsCollector(MetricsServiceGrpc.getExportMethod(), otel);
    /**
     * The OpenTelemetry traces collector.
     */
    private TracesCollector tracesCollector =
            new TracesCollector(TraceServiceGrpc.getExportMethod(), otel);

    /**
     * Stops all OpenTelemetry collectors and closes them.
     * Called when the server is shutting down.
     */
    @Override
    public void stop() throws Exception {
        try {
            logsCollector.close();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Failed to close logs collector", e);
        }

        try {
            metricsCollector.close();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Failed to close metrics collector", e);
        }

        try {
            tracesCollector.close();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Failed to close traces collector", e);
        }
    }

    /**
     * Starts all OpenTelemetry collectors.
     * Called on server launch.
     */
    @Override
    public void start() throws Exception {
        LOG.log(Level.INFO, "Attaching otel collectors");

        String basePath = System.getenv("PARQUET_LOCATION");

        if (basePath == null || basePath.isBlank()) {
            basePath = "";
        } else if (!basePath.endsWith("/")) {
            basePath = basePath + "/";
        }

        try {
            long currentTimestamp = System.currentTimeMillis();

            String logsPrefix = basePath
                    + "logs"
                    + "-" + currentTimestamp;
            String metricsPrefix = basePath
                    + "metrics"
                    + "-" + currentTimestamp;
            String spansPrefix = basePath
                    + "traces"
                    + "-" + currentTimestamp;

            FileLogs persistentLogs =
                    new FileLogs(logsPrefix, otel);
            FileMetrics persistentMetrics =
                    new FileMetrics(metricsPrefix, otel);
            FileSpans persistentSpans =
                    new FileSpans(spansPrefix, otel);

            logsCollector.subscribe(persistentLogs);
            metricsCollector.subscribe(persistentMetrics);
            tracesCollector.subscribe(persistentSpans);

            LOG.log(Level.INFO,
                    "Collecting logs to files: "
                            + logsPrefix
                            + "*.parquet");
            LOG.log(Level.INFO,
                    "Collecting metrics to files: "
                            + metricsPrefix
                            + "*.parquet");
            LOG.log(Level.INFO,
                    "Collecting traces to files: "
                            + spansPrefix
                            + "*.parquet");
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Failed to attach otel collectors", e);

            throw e;
        }

        GrpcServer grpcServer = GrpcServer.server(vertx);

        logsCollector.bind(grpcServer);
        metricsCollector.bind(grpcServer);
        tracesCollector.bind(grpcServer);

        vertx.createHttpServer()
            .requestHandler(grpcServer)
            .listen(DEFAULT_GRPC_PORT)
            .onSuccess(server -> LOG.info(
                    "GRPC server running on port "
                        + DEFAULT_GRPC_PORT)
            )
            .onFailure(t -> LOG.log(
                    Level.SEVERE,
                    String.format(
                        "GRPC server on port %d failed to start",
                        DEFAULT_GRPC_PORT),
                    t)
            );

        Router router = Router.router(vertx)
                .allowForward(AllowForwardHeaders.ALL);

        logsCollector.bind(router);
        metricsCollector.bind(router);
        tracesCollector.bind(router);

        vertx.createHttpServer()
            .requestHandler(router)
            .listen(DEFAULT_HTTP_PORT)
            .onSuccess(server -> LOG.info(
                    "HTTP server running on port "
                        + DEFAULT_HTTP_PORT)
            )
            .onFailure(t -> LOG.log(
                    Level.SEVERE,
                    String.format(
                       "HTTP server on port %d failed to start",
                        DEFAULT_HTTP_PORT),
                    t)
            );
    }

    /**
     * Main method - launches the OpenTelemetry server.
     *
     * @param args the command-line args
     * @throws Exception if an error occurs
     */
    public static void main(final String[] args) throws Exception {
        Launcher.executeCommand("run", CollectorsMain.class.getName());
    }
}
