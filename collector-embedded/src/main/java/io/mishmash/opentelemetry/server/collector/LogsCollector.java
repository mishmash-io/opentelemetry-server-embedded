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

package io.mishmash.opentelemetry.server.collector;

import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.MethodDescriptor;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.vertx.core.Vertx;

/**
 * Processes incoming OpenTelemetry logs packets - extracts individual
 * Logs from a packet and queues them to all {@link LogsSubscriber}s.
 *
 * Also notifies the client on potential errors.
 */
public class LogsCollector
        extends AbstractCollector<
            ExportLogsServiceRequest,
            ExportLogsServiceResponse,
            Log> {

    /**
     * The {@link java.util.logging.Logger} used by this class.
     */
    private static final Logger LOG =
            Logger.getLogger(LogsCollector.class.getName());

    /**
     * Creates a new logs collector to be added to a Vert.x routing
     * context.
     *
     * @param exportMethod the gRPC method to be attached to
     * @param otel a helper for own telemetry needs
     */
    public LogsCollector(
            final MethodDescriptor<
                ExportLogsServiceRequest,
                ExportLogsServiceResponse> exportMethod,
            final Instrumentation otel) {
        super("/v1/logs", exportMethod, otel,
                ExportLogsServiceRequest::parser,
                ExportLogsServiceRequest::newBuilder,
                ExportLogsServiceResponse::parser,
                ExportLogsServiceResponse::newBuilder,
                new ForkJoinPool());
    }

    /**
     * Processes incoming packets (or batches) - extracts and publishes
     * the contained OpenTelemetry LogRecords (one by one) to all
     * {@link LogsSubscriber}s.
     *
     * @param request the client's OTLP request
     * @param transport the OTLP transport - "grpc" or "http"
     * @param encoding the OTLP encoding - "protobuf" or "json"
     * @param otelContext the {@link io.opentelemetry.context.Context}
     * used for own telemetry
     */
    @Override
    public Batch<Log> loadBatch(
            final ExportLogsServiceRequest request,
            final String transport,
            final String encoding,
            final Context otelContext) {
        try (Scope s = otelContext.makeCurrent()) {
            Span.current().setAttribute("otel.collector.name", "logs");

            Batch<Log> batch = new Batch<>(otelContext);

            long timestamp = System.currentTimeMillis();
            String uuid = UUID.randomUUID().toString();

            int requestItems = 0;

            for (ResourceLogs logs : request.getResourceLogsList()) {
                for (ScopeLogs scopeLogs : logs.getScopeLogsList()) {
                    for (LogRecord log : scopeLogs.getLogRecordsList()) {
                        if (batch.isCancelled()) {
                            return batch;
                        }

                        Span recordSpan = getInstrumentation()
                                .startNewSpan("otel.record");

                        Log l = new Log(batch,
                                Context.current(),
                                Vertx.currentContext().get(VCTX_EMITTER));
                        l.setFrom(
                                timestamp,
                                uuid,
                                requestItems++,
                                logs,
                                scopeLogs,
                                log);

                        /*
                         * FIXME: check if is valid and add an error message
                         * (but still allow it to go to subscribers)
                         */

                        l.addAll(getSubscribers());
                        l.setLoaded();
                        batch.add(l);

                        recordSpan.addEvent("Request item loaded");

                        int estimatedLag = offer(
                                l,
                                (subscriber, droppedItem) -> {
                                    /*
                                     * set an error on this in the response,
                                     * FIXME: use another exception class
                                     */
                                    droppedItem.completeExceptionally(
                                            new RuntimeException(
                                                "Logs collector subscriber "
                                                + subscriber
                                                + " dropped a log record"));

                                    // droppedItem.complete(subscriber);
                                    // do not retry
                                    return false;
                                });

                        if (estimatedLag < 0) {
                            // says how many subscribers dropped the message
                            LOG.info(
                                    String.format(
                                            "Logs batch %s has %d drop(s)",
                                            uuid,
                                            (-estimatedLag)));
                            addDroppedRequestItems(
                                    (-estimatedLag),
                                    transport,
                                    encoding);
                        } else if (estimatedLag == 0) {
                            // there were no subscribers, set an error
                            batch.setLoadFailed(
                                    new IllegalStateException("""
                                            Logs collector currently has \
                                            no subscribers"""));
                            LOG.log(Level.SEVERE, """
                                    Logs batch load failed, logs collector \
                                    currently has no subscribers. \
                                    Batch id: """
                                        + uuid);

                            return batch;
                        // } else {
                            /*
                             * positive number is the estimated lag - number
                             * of items submitted but not yet consumed
                             */

                            // LOG.info("Logs estimated lag: " + estimatedLag);
                        }
                    }
                }
            }

            batch.setLoaded();

            addRequestItems(requestItems, transport, encoding);

            return batch;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExportLogsServiceResponse getBatchResponse(
            final ExportLogsServiceRequest request,
            final Batch<Log> completedBatch,
            final String transport,
            final String encoding) {
        ExportLogsServiceResponse.Builder response =
                ExportLogsServiceResponse.newBuilder();
        String errorMessage = null;
        int numInvalid = 0;

        for (Log l : completedBatch.getProcessedElements()) {
            if (!l.isValid()) {
                numInvalid++;
            }

            if (l.getErrorMessage() != null && errorMessage == null) {
                errorMessage = l.getErrorMessage();
            }
        }

        ExportLogsPartialSuccess.Builder partialSuccess =
                ExportLogsPartialSuccess.newBuilder();

        if (numInvalid > 0) {
            partialSuccess.setRejectedLogRecords(numInvalid);
        }

        if (errorMessage != null) {
            partialSuccess.setErrorMessage(errorMessage);
        }

        if (numInvalid > 0 || errorMessage != null) {
            response.setPartialSuccess(partialSuccess);
        }

        if (numInvalid > 0) {
            addPartiallySucceededRequests(1, transport, encoding);
        } else {
            addSucceededRequests(1, transport, encoding);
        }

        return response.build();
    }

    /**
     * Returns "logs" as the signal type for this collector's own telemetry.
     */
    @Override
    protected String telemetrySignalType() {
        return "logs";
    }
}
