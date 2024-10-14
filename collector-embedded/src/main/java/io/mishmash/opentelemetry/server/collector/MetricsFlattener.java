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

import java.util.Iterator;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.opentelemetry.context.Context;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.vertx.ext.auth.User;

/**
 * Extracts individual metric data points from an OTLP packet.
 *
 * Turns an {@link ExportMetricsServiceRequest} into
 * an {@link Iterable} of {@link MetricDataPoint}s.
 *
 * The original OTLP protocol message format, as specified
 * by {@link ExportMetricsServiceRequest}, contains lists of
 * individual metric data points. These lists are nested
 * into lists of {@link Metric}s, which are in turn
 * nested inside a list of {@link ScopeMetrics}, and scopes
 * are again nested into a list of {@link ResourceMetrics}.
 *
 * To facilitate further processing this class extracts
 * (or 'flattens') the above nested structures into a 'flat'
 * {@link Iterable} of individual {@link MetricDataPoint} instances.
 *
 * To see examples of 'flattened' data visit
 * @see <a href="https://github.com/mishmash-io/opentelemetry-server-embedded/blob/main/examples/notebooks/basics.ipynb">
 * OpenTelemetry Basics Notebook on GitHub.</a>
 */
public class MetricsFlattener implements Iterable<MetricDataPoint> {

    /**
     * The Logger to use.
     */
    private static final Logger LOG = Logger
            .getLogger(MetricsFlattener.class.getName());
    /**
     * The parent {@link Batch}.
     */
    private Batch<MetricDataPoint> batch;
    /**
     * The own telemetry {@link Context}.
     */
    private Context otel;
    /**
     * The OTLP message received from the client.
     */
    private ExportMetricsServiceRequest request;
    /**
     * An optional {@link User} if authentication was enabled.
     */
    private User user;
    /**
     * The timestamp of this batch.
     */
    private long timestamp;
    /**
     * The batch UUID.
     */
    private String uuid;

    /**
     * Create a {@link MetricDataPoint}s flattener.
     *
     * @param parentBatch the parent {@link Batch}
     * @param otelContext the own telemetry {@link Context}
     * @param metricsRequest the OTLP packet
     * @param authUser the {@link User} submitting the request or null
     * if authentication was not enabled
     */
    public MetricsFlattener(
            final Batch<MetricDataPoint> parentBatch,
            final Context otelContext,
            final ExportMetricsServiceRequest metricsRequest,
            final User authUser) {
        this.batch = parentBatch;
        this.otel = otelContext;
        this.request = metricsRequest;
        this.user = authUser;

        timestamp = System.currentTimeMillis();
        uuid = UUID.randomUUID().toString();
    }

    /**
     * Get the configured Metrics {@link Batch}, if any.
     *
     * @return the batch or null if not set
     */
    public Batch<MetricDataPoint> getBatch() {
        return batch;
    }

    /**
     * Get the own telemetry context, if any.
     *
     * @return the {@link Context} or null if not set
     */
    public Context getOtel() {
        return otel;
    }

    /**
     * Get the parsed protobuf Metrics request.
     *
     * @return the {@link ExportMetricsServiceRequest} message
     */
    public ExportMetricsServiceRequest getRequest() {
        return request;
    }

    /**
     * Get the authenticated user who submitted this message.
     *
     * @return the {@link User} or null if authentication was not enabled
     */
    public User getUser() {
        return user;
    }

    /**
     * Get the timestamp used by this flattener.
     *
     * @return the timestamp in milliseconds
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Get the unique UUID used by this flattener.
     *
     * @return the UUID
     */
    public String getUuid() {
        return uuid;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<MetricDataPoint> iterator() {
        return new DataPointsIterator();
    }

    /**
     * The internal {@link Iterator}, as returned to user.
     */
    private final class DataPointsIterator
            implements Iterator<MetricDataPoint> {

        /**
         * Iterator over the OTLP Resource Metrics.
         */
        private Iterator<ResourceMetrics> resourceIt;
        /**
         * Iterator over the OTLP Scope Metrics within an OTLP Resource Metrics.
         */
        private Iterator<ScopeMetrics> scopeIt;
        /**
         * Iterator over the OTLP Metrics within a scope.
         */
        private Iterator<Metric> metricIt;
        /**
         * Iterator over individual OTLP Exponential Histogram data points.
         */
        private Iterator<ExponentialHistogramDataPoint> expHistIt;
        /**
         * Iterator over individual OTLP Gauge or Sum data points.
         */
        private Iterator<NumberDataPoint> numIt;
        /**
         * Iterator over individual OTLP Histogram data points.
         */
        private Iterator<HistogramDataPoint> histIt;
        /**
         * Iterator over individual OTLP Summary data points.
         */
        private Iterator<SummaryDataPoint> summaryIt;
        /**
         * The current OTLP Resource Metrics.
         */
        private ResourceMetrics currentResource;
        /**
         * The current OTLP Scope Metrics.
         */
        private ScopeMetrics currentScope;
        /**
         * The current OTLP Metric.
         */
        private Metric currentMetric;
        /**
         * A counter of processed metrics.
         */
        private int metricsCount;
        /**
         * A counter of returned {@link MetricDataPoint}s.
         */
        private int itemsCount;

        /**
         * Initialize this iterator.
         */
        private DataPointsIterator() {
            metricsCount = 0;
            itemsCount = 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            if (currentMetric == null || !hasNextDataPoint()) {
                nextMetric();

                while (currentMetric != null
                        && !initDataPointIterator()) {
                    nextMetric();
                }
            }

            return currentMetric != null
                    && hasNextDataPoint();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public MetricDataPoint next() {
            MetricDataPoint dataPoint = new MetricDataPoint(batch, otel, user);

            switch (currentMetric.getDataCase()) {
            case EXPONENTIAL_HISTOGRAM:
                dataPoint.setFrom(
                        timestamp,
                        uuid,
                        metricsCount,
                        currentResource,
                        currentScope,
                        currentMetric,
                        currentMetric.getExponentialHistogram(),
                        itemsCount++,
                        expHistIt.next());
                break;
            case GAUGE:
                dataPoint.setFrom(
                        timestamp,
                        uuid,
                        metricsCount,
                        currentResource,
                        currentScope,
                        currentMetric,
                        currentMetric.getGauge(),
                        itemsCount++,
                        numIt.next());
                break;
            case HISTOGRAM:
                dataPoint.setFrom(
                        timestamp,
                        uuid,
                        metricsCount,
                        currentResource,
                        currentScope,
                        currentMetric,
                        currentMetric.getHistogram(),
                        itemsCount++,
                        histIt.next());
                break;
            case SUM:
                dataPoint.setFrom(
                        timestamp,
                        uuid,
                        metricsCount,
                        currentResource,
                        currentScope,
                        currentMetric,
                        currentMetric.getSum(),
                        itemsCount++,
                        numIt.next());
                break;
            case SUMMARY:
                dataPoint.setFrom(
                        timestamp,
                        uuid,
                        metricsCount,
                        currentResource,
                        currentScope,
                        currentMetric,
                        currentMetric.getSummary(),
                        itemsCount++,
                        summaryIt.next());
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unexpected OTLP metric of type: '%s'",
                                currentMetric.getDataCase()));
            }

            return dataPoint;
        }

        /**
         * Check if the current metric has remaining data points.
         *
         * @return true when there are more data points in the
         * current metric.
         */
        private boolean hasNextDataPoint() {
            switch (currentMetric.getDataCase()) {
            case DATA_NOT_SET:
                return false;
            case EXPONENTIAL_HISTOGRAM:
                return expHistIt != null && expHistIt.hasNext();
            case GAUGE:
            case SUM:
                return numIt != null && numIt.hasNext();
            case HISTOGRAM:
                return histIt != null && histIt.hasNext();
            case SUMMARY:
                return summaryIt != null && summaryIt.hasNext();
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown OTLP metric type: '%s'",
                                currentMetric.getDataCase()));
            }
        }

        /**
         * Initialize the data point iterator that corresponds
         * to the current metric type.
         *
         * Call only once after a new metric has been obtained.
         *
         * @return true if the new metric has data points
         */
        private boolean initDataPointIterator() {
            boolean res;

            switch (currentMetric.getDataCase()) {
            case DATA_NOT_SET:
                // the metric type is missing, not expecting data here
                LOG.log(Level.WARNING,
                        "Received an OTLP metric without a type, skipping");
                expHistIt = null;
                numIt = null;
                histIt = null;
                summaryIt = null;

                return false;
            case EXPONENTIAL_HISTOGRAM:
                expHistIt = currentMetric
                                .getExponentialHistogram()
                                .getDataPointsList()
                                .iterator();
                res = expHistIt.hasNext();
                break;
            case GAUGE:
                numIt = currentMetric
                                .getGauge()
                                .getDataPointsList()
                                .iterator();
                res = numIt.hasNext();
                break;
            case HISTOGRAM:
                histIt = currentMetric
                                .getHistogram()
                                .getDataPointsList()
                                .iterator();
                res = histIt.hasNext();
                break;
            case SUM:
                numIt = currentMetric
                                .getSum()
                                .getDataPointsList()
                                .iterator();
                res = numIt.hasNext();
                break;
            case SUMMARY:
                summaryIt = currentMetric
                                .getSummary()
                                .getDataPointsList()
                                .iterator();
                res = summaryIt.hasNext();
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown OTLP metric type: '%s'",
                                currentMetric.getDataCase()));
            }

            if (res) {
                itemsCount = 0;
                metricsCount++;
            }

            return res;
        }

        /**
         * Move the nested iterators to next OTLP Metric.
         */
        private void nextMetric() {
            if (metricIt == null || !metricIt.hasNext()) {
                nextScope();

                while (currentScope != null
                        && !(
                                metricIt = currentScope
                                            .getMetricsList()
                                            .iterator()
                            ).hasNext()) {
                    nextScope();
                }
            }

            currentMetric = metricIt == null || !metricIt.hasNext()
                    ? null
                    : metricIt.next();
        }

        /**
         * Move the nested iterators to next OTLP Scope Metrics.
         */
        private void nextScope() {
            if (scopeIt == null || !scopeIt.hasNext()) {
                nextResource();

                while (currentResource != null
                        && !(
                                scopeIt = currentResource
                                        .getScopeMetricsList()
                                        .iterator()
                            ).hasNext()) {
                    nextResource();
                }
            }

            currentScope = scopeIt == null || !scopeIt.hasNext()
                    ? null
                    : scopeIt.next();
        }

        /**
         * Move the nested iterators to next OTLP Resource Metrics.
         */
        private void nextResource() {
            if (resourceIt == null) {
                resourceIt = request.getResourceMetricsList().iterator();
            }

            currentResource = resourceIt.hasNext()
                    ? resourceIt.next()
                    : null;
        }
    }
}
