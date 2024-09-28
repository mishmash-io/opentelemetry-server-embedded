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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Flow.Subscriber;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.MethodDescriptor;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogram;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Summary;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.vertx.core.Vertx;

/**
 * Processes incoming OpenTelemetry metrics packets - extracts all
 * data points of all metrics contained in a packet and queues them
 * to all {@link MetricsSubscriber}s.
 *
 * Also notifies the client on potential errors.
 */
public class MetricsCollector
        extends AbstractCollector<
            ExportMetricsServiceRequest,
            ExportMetricsServiceResponse,
            MetricDataPoint> {

    /**
     * The {@link java.util.logging.Logger} used by this class.
     */
    private static final Logger LOG =
            Logger.getLogger(MetricsCollector.class.getName());

    /**
     * Creates a new metrics collector to be added to a Vert.x routing
     * context.
     *
     * @param exportMethod the gRPC method to be attached to
     * @param otel a helper for own telemetry needs
     */
    public MetricsCollector(
            final MethodDescriptor<
                ExportMetricsServiceRequest,
                ExportMetricsServiceResponse> exportMethod,
            final Instrumentation otel) {
        super("/v1/metrics", exportMethod, otel,
                ExportMetricsServiceRequest::parser,
                ExportMetricsServiceRequest::newBuilder,
                ExportMetricsServiceResponse::parser,
                ExportMetricsServiceResponse::newBuilder,
                new ForkJoinPool());
    }

    /**
     * Processes incoming packets (or batches) - extracts and publishes
     * the contained OpenTelemetry metric data points (one by one) to all
     * {@link MetricsSubscriber}s.
     *
     * @param request the client's OTLP request
     * @param transport the OTLP transport - "grpc" or "http"
     * @param encoding the OTLP encoding - "protobuf" or "json"
     * @param otelContext the {@link io.opentelemetry.context.Context}
     * used for own telemetry
     */
    @Override
    public Batch<MetricDataPoint> loadBatch(
            final ExportMetricsServiceRequest request,
            final String transport,
            final String encoding,
            final Context otelContext) {
        try (Scope s = otelContext.makeCurrent()) {
            Span.current().setAttribute("otel.collector.name", "metrics");

            Batch<MetricDataPoint> batch = new Batch<>(otelContext);

            long timestamp = System.currentTimeMillis();
            String uuid = UUID.randomUUID().toString();

            int seqNo = 0;
            int requestItems = 0;

            for (ResourceMetrics metrics : request.getResourceMetricsList()) {
                for (ScopeMetrics scopeMetrics
                        : metrics.getScopeMetricsList()) {
                    for (io.opentelemetry.proto.metrics.v1.Metric metric
                            : scopeMetrics.getMetricsList()) {
                        int dpSeqNo = 0;

                        Span recordSpan = getInstrumentation()
                                .startNewSpan("otel.record");

                        switch (metric.getDataCase()) {
                        case DATA_NOT_SET:
                            /*
                             * FIXME: what to do when a metric has no
                             * data points?
                             */
                            continue;

                        case EXPONENTIAL_HISTOGRAM:
                            ExponentialHistogram exponentialHistogram =
                                metric.getExponentialHistogram();

                            for (ExponentialHistogramDataPoint dp
                                    : exponentialHistogram
                                        .getDataPointsList()) {
                                if (batch.isCancelled()) {
                                    return batch;
                                }

                                MetricDataPoint m = new MetricDataPoint(
                                        batch,
                                        Context.current(),
                                        Vertx.currentContext()
                                            .get(VCTX_EMITTER));
                                m.setFrom(timestamp,
                                        uuid,
                                        seqNo,
                                        metrics,
                                        scopeMetrics,
                                        metric,
                                        exponentialHistogram,
                                        dpSeqNo++,
                                        dp);

                                if (!offerDataPoint(
                                        batch,
                                        m,
                                        uuid,
                                        transport,
                                        encoding)) {
                                    return batch;
                                }
                            }

                            break;
                        case GAUGE:
                            Gauge gauge = metric.getGauge();

                            for (NumberDataPoint dp
                                    : gauge.getDataPointsList()) {
                                if (batch.isCancelled()) {
                                    return batch;
                                }

                                MetricDataPoint m = new MetricDataPoint(
                                        batch,
                                        Context.current(),
                                        Vertx.currentContext()
                                            .get(VCTX_EMITTER));
                                m.setFrom(timestamp,
                                        uuid,
                                        seqNo,
                                        metrics,
                                        scopeMetrics,
                                        metric,
                                        gauge,
                                        dpSeqNo++,
                                        dp);

                                if (!offerDataPoint(
                                        batch,
                                        m,
                                        uuid,
                                        transport,
                                        encoding)) {
                                    return batch;
                                }
                            }

                            break;
                        case HISTOGRAM:
                            Histogram histogram = metric.getHistogram();

                            for (HistogramDataPoint dp
                                    : histogram.getDataPointsList()) {
                                if (batch.isCancelled()) {
                                    return batch;
                                }

                                MetricDataPoint m = new MetricDataPoint(
                                        batch,
                                        Context.current(),
                                        Vertx.currentContext()
                                            .get(VCTX_EMITTER));
                                m.setFrom(timestamp,
                                        uuid,
                                        seqNo,
                                        metrics,
                                        scopeMetrics,
                                        metric,
                                        histogram,
                                        dpSeqNo++,
                                        dp);

                                if (!offerDataPoint(
                                        batch,
                                        m,
                                        uuid,
                                        transport,
                                        encoding)) {
                                    return batch;
                                }
                            }

                            break;
                        case SUM:
                            Sum sum = metric.getSum();

                            for (NumberDataPoint dp
                                    : sum.getDataPointsList()) {
                                if (batch.isCancelled()) {
                                    return batch;
                                }

                                MetricDataPoint m = new MetricDataPoint(
                                        batch,
                                        Context.current(),
                                        Vertx.currentContext()
                                            .get(VCTX_EMITTER));
                                m.setFrom(timestamp,
                                        uuid,
                                        seqNo,
                                        metrics,
                                        scopeMetrics,
                                        metric,
                                        sum,
                                        dpSeqNo++,
                                        dp);

                                if (!offerDataPoint(batch,
                                        m,
                                        uuid,
                                        transport,
                                        encoding)) {
                                    return batch;
                                }
                            }

                            break;
                        case SUMMARY:
                            Summary summary = metric.getSummary();

                            for (SummaryDataPoint dp
                                    : summary.getDataPointsList()) {
                                if (batch.isCancelled()) {
                                    return batch;
                                }

                                MetricDataPoint m = new MetricDataPoint(
                                        batch,
                                        Context.current(),
                                        Vertx.currentContext()
                                            .get(VCTX_EMITTER));
                                m.setFrom(timestamp,
                                        uuid,
                                        seqNo,
                                        metrics,
                                        scopeMetrics,
                                        metric,
                                        summary,
                                        dpSeqNo++,
                                        dp);

                                if (!offerDataPoint(
                                        batch,
                                        m,
                                        uuid,
                                        transport,
                                        encoding)) {
                                    return batch;
                                }
                            }

                            break;
                        default:
                            // FIXME: unknown type!
                            break;
                        }

                        requestItems += dpSeqNo;
                        seqNo++;

                        recordSpan.addEvent("Request item loaded");
                    }
                }
            }

            batch.setLoaded();

            addRequestItems(requestItems, transport, encoding);

            return batch;
        }
    }

    /**
     * Actually sumbit a data point to all subscribers.
     *
     * @param batch the batch
     * @param m the data point
     * @param uuid id of the batch
     * @param transport OTLP transport used
     * @param encoding OTLP transport encoding used
     * @return true if successful
     */
    protected boolean offerDataPoint(
            final Batch<MetricDataPoint> batch,
            final MetricDataPoint m,
            final String uuid,
            final String transport,
            final String encoding) {
        /*
         *  FIXME: check if it is valid and add an error message, but allow
         *  it to go to subscribers
         */

        List<Subscriber<? super MetricDataPoint>> subscribers =
                getSubscribers();
        m.addAll(subscribers);
        m.setLoaded();
        batch.add(m);

        int estimatedLag = offer(m, (subscriber, droppedItem) -> {
            /*
             * set an error on this in the response,
             * FIXME: use another exception class
             */
            droppedItem.completeExceptionally(
                    new RuntimeException("Metrics collector subscriber "
                            + subscriber
                            + " dropped a metric record"));

            // do not retry
            return false;
        });

        if (estimatedLag < 0) {
            // it tells how many subscribers dropped the message
            LOG.info(
                    String.format(
                            "Metrics batch %s has %d drop(s)",
                            uuid,
                            (-estimatedLag)));
            addDroppedRequestItems((-estimatedLag), transport, encoding);
        } else if (estimatedLag == 0) {
            // there were no subscribers, set an error
            batch.setLoadFailed(
                    new IllegalStateException(
                            "Metrics collector currently has no subscribers"));
            LOG.log(Level.SEVERE, """
                    Metrics batch load failed, metrics collector currently \
                    has no subscribers. Batch id: """
                            + uuid);

            return false;
        // } else {
            /*
             * positive number is the estimated lag - number of items
             * submitted but not yet consumed
             */

            // LOG.info("Metrics estimated lag: " + estimatedLag);
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExportMetricsServiceResponse getBatchResponse(
            final ExportMetricsServiceRequest request,
            final Batch<MetricDataPoint> completedBatch,
            final String transport,
            final String encoding) {
        ExportMetricsServiceResponse.Builder response =
                ExportMetricsServiceResponse.newBuilder();
        String errorMessage = null;
        int numInvalid = 0;

        for (MetricDataPoint m : completedBatch.getProcessedElements()) {
            if (!m.isValid()) {
                numInvalid++;
            }

            if (m.getErrorMessage() != null && errorMessage == null) {
                errorMessage = m.getErrorMessage();
            }
        }

        ExportMetricsPartialSuccess.Builder partialSuccess =
                ExportMetricsPartialSuccess.newBuilder();

        if (numInvalid > 0) {
            partialSuccess.setRejectedDataPoints(numInvalid);
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
     * Returns "metrics" as the signal type for this collector's own telemetry.
     */
    @Override
    protected String telemetrySignalType() {
        return "metrics";
    }
}
