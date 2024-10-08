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

import java.util.concurrent.ForkJoinPool;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.MethodDescriptor;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.vertx.core.Vertx;

/**
 * Processes incoming OpenTelemetry traces packets - extracts individual
 * Spans from a packet and queues them to all {@link SpansSubscriber}s.
 *
 * Also notifies the client on potential errors.
 */
public class TracesCollector
        extends AbstractCollector<
            ExportTraceServiceRequest,
            ExportTraceServiceResponse,
            Span> {

    /**
     * The {@link java.util.logging.Logger} used by this class.
     */
    private static final Logger LOG =
            Logger.getLogger(TracesCollector.class.getName());

    /**
     * Creates a new traces collector to be added to a Vert.x routing
     * context.
     *
     * @param exportMethod the gRPC method to be attached to
     * @param otel a helper for own telemetry needs
     */
    public TracesCollector(
            final MethodDescriptor<
                ExportTraceServiceRequest,
                ExportTraceServiceResponse> exportMethod,
            final Instrumentation otel) {
        super("/v1/traces", exportMethod, otel,
                ExportTraceServiceRequest::parser,
                ExportTraceServiceRequest::newBuilder,
                ExportTraceServiceResponse::parser,
                ExportTraceServiceResponse::newBuilder,
                new ForkJoinPool());
    }

    /**
     * Processes incoming packets (or batches) - extracts and publishes
     * the contained OpenTelemetry Spans (one by one) to all
     * {@link SpansSubscriber}s.
     *
     * @param request the client's OTLP request
     * @param transport the OTLP transport - "grpc" or "http"
     * @param encoding the OTLP encoding - "protobuf" or "json"
     * @param otelContext the {@link io.opentelemetry.context.Context}
     * used for own telemetry
     */
    @Override
    public Batch<Span> loadBatch(
            final ExportTraceServiceRequest request,
            final String transport,
            final String encoding,
            final Context otelContext) {
        try (Scope sc = otelContext.makeCurrent()) {
            io.opentelemetry.api.trace.Span.current()
                .setAttribute("otel.collector", "traces");

            Batch<Span> batch = new Batch<>(otelContext);

            int requestItems = 0;

            for (Span s : new TracesFlattener(
                                batch,
                                Context.current(),
                                request,
                                Vertx.currentContext().get(VCTX_EMITTER))) {
                if (batch.isCancelled()) {
                    return batch;
                }

                /*
                 * FIXME: check if is valid and add an error message
                 * (but still allow it to go to subscribers
                 */

                s.addAll(getSubscribers());
                s.setLoaded();
                batch.add(s);

                requestItems++;

                int estimatedLag = offer(
                        s,
                        (subscriber, droppedItem) -> {
                            /*
                             * set an error on this in the response,
                             * FIXME: use another exception class
                             */
                            droppedItem.completeExceptionally(
                                    new RuntimeException(
                                        "Traces collector subscriber "
                                            + subscriber
                                            + " dropped a span record")
                                    );

                            // do not retry
                            return false;
                        });

                if (estimatedLag < 0) {
                    // says how many subscribers dropped the message
                    LOG.info(
                            String.format(
                                    "Traces batch has %d drop(s)",
                                    (-estimatedLag)));

                    addDroppedRequestItems(
                            (-estimatedLag),
                            transport,
                            encoding);
                } else if (estimatedLag == 0) {
                    // there were no subscribers, set an error
                    batch.setLoadFailed(
                            new IllegalStateException("""
                                    Traces collector currently has \
                                    no subscribers"""));

                    LOG.log(Level.SEVERE,
                            """
                                Traces batch load failed, traces \
                                collector currently has no \
                                subscribers. Batch id: """);

                    return batch;
                // } else {
                    /*
                     * positive number is the estimated lag - number
                     * of items submitted but not yet consumed
                     */

                 // LOG.info("Traces estimated lag: " + estimatedLag);
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
    public ExportTraceServiceResponse getBatchResponse(
            final ExportTraceServiceRequest request,
            final Batch<Span> completedBatch,
            final String transport,
            final String encoding) {
        ExportTraceServiceResponse.Builder response =
                ExportTraceServiceResponse.newBuilder();
        String errorMessage = null;
        int numInvalid = 0;

        for (Span s : completedBatch.getProcessedElements()) {
            if (!s.isValid()) {
                numInvalid++;
            }

            if (s.getErrorMessage() != null && errorMessage == null) {
                errorMessage = s.getErrorMessage();
            }
        }

        ExportTracePartialSuccess.Builder partialSuccess =
                ExportTracePartialSuccess.newBuilder();

        if (numInvalid > 0) {
            partialSuccess.setRejectedSpans(numInvalid);
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
     * Returns "traces" as the signal type for this collector's own telemetry.
     */
    @Override
    protected String telemetrySignalType() {
        return "traces";
    }
}
