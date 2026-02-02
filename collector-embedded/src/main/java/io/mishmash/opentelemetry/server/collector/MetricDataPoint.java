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

import io.opentelemetry.context.Context;
import io.opentelemetry.proto.common.v1.EntityRef;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogram;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric.DataCase;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Summary;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;
import io.vertx.ext.auth.User;

/**
 * Holds the details of an individual OTLP metric data point
 * received from a client as a {@link Batch} of all subscribers
 * that are expected to process it.
 */
public class MetricDataPoint extends SubscribersBatch<MetricDataPoint>
        implements SignalResource {

    /**
     * Timestamp when the client's OTLP packet was received (in ms).
     */
    private long batchTimestamp;
    /**
     * A unique id of the client's OTLP packet.
     */
    private String batchUUID;
    /**
     * The sequence number of this metric within the client's
     * OTLP packet (zero-based).
     */
    private int seqNo;
    /**
     * The OTLP Resource details.
     */
    private Resource resource;
    /**
     * The Schema URL of the OTLP Resource.
     */
    private String resourceSchemaUrl;
    /**
     * The OTLP Scope details.
     */
    private InstrumentationScope scope;
    /**
     * The metric name.
     */
    private String name;
    /**
     * The metric description.
     */
    private String description;
    /**
     * The metric unit of measurement.
     */
    private String unit;
    /**
     * The metric type - GAUGE, HISTOGRAM, etc...
     */
    private DataCase type;

    /**
     * The sequence number of this metric data point within its containing
     * metric (zero-based).
     */
    private int datapointSeqNo;
    /**
     * The details of an exponential histogram data point.
     */
    private ExponentialHistogramDataPoint exponentialHistogram;
    /**
     * The details of a gauge data point.
     */
    private NumberDataPoint gauge;
    /**
     * The details of a histogram data point.
     */
    private HistogramDataPoint histogram;
    /**
     * The details of a number data point.
     */
    private NumberDataPoint sum;
    /**
     * The details of a summary data point.
     */
    private SummaryDataPoint summary;

    /**
     * The Schema URL of the OTLP metric.
     */
    private String metricSchemaUrl;

    /**
     * Aggregation temporality of the metric - delta, cumulative, etc...
     */
    private AggregationTemporality aggregationTemporality;
    /**
     * If the metric is monotonic.
     */
    private boolean isMonotonic;

    /**
     * If this record is valid.
     */
    private boolean isValid;
    /**
     * An error message to be sent back if this packet has an error.
     */
    private String errorMessage;

    /**
     * Metadata of the metric data point.
     */
    private List<KeyValue> metaData;

    /**
     * Create a new empty metric data point.
     *
     * @param parent parent {@link Batch}
     * @param otelContext {@link io.opentelemetry.context.Context} for own
     * telemetry
     * @param authUser the authenticated user or null if auth wasn't enabled
     */
    public MetricDataPoint(
            final Batch<MetricDataPoint> parent,
            final Context otelContext,
            final User authUser) {
        super(parent, otelContext, authUser);
    }

    /**
     * Set the details of an exponential histogram data point.
     * Also verifies the data and potentially sets an error message.
     *
     * @param batchTS the batch timestamp
     * @param batchID the batch id
     * @param sequenceNum the sequence number of this metric
     * @param resourceMetric used to fill-in OpenTelemetry Resource details
     * @param scopeMetric used to fill-in OpenTelemetry Scope details
     * @param metric the metric details
     * @param otelExponentialHistogram the exponential histogram details
     * @param dpSeqNo the data point sequence number
     * @param dp the data point itself
     */
    public void setFrom(
            final long batchTS,
            final String batchID,
            final int sequenceNum,
            final ResourceMetrics resourceMetric,
            final ScopeMetrics scopeMetric,
            final io.opentelemetry.proto.metrics.v1.Metric metric,
            final ExponentialHistogram otelExponentialHistogram,
            final int dpSeqNo,
            final ExponentialHistogramDataPoint dp) {
        this.batchTimestamp = batchTS;
        this.batchUUID = batchID;
        this.seqNo = sequenceNum;
        this.resource = resourceMetric.getResource();
        this.resourceSchemaUrl = resourceMetric.getSchemaUrl();
        this.scope = scopeMetric.getScope();
        this.name = metric.getName();
        this.description = metric.getDescription();
        this.unit = metric.getUnit();
        this.type = metric.getDataCase();
        this.metricSchemaUrl = scopeMetric.getSchemaUrl();
        this.metaData = metric.getMetadataList();

        this.aggregationTemporality =
                otelExponentialHistogram.getAggregationTemporality();
        this.datapointSeqNo = dpSeqNo;
        this.exponentialHistogram = dp;

        // FIXME: add checks for validity and set isValid, errorMessage?
        isValid = true;
    }

    /**
     * Set the details of a gauge data point.
     * Also verifies the data and potentially sets an error message.
     *
     * @param batchTS the batch timestamp
     * @param batchID the batch id
     * @param sequenceNum the sequence number of this metric
     * @param resourceMetric used to fill-in OpenTelemetry Resource details
     * @param scopeMetric used to fill-in OpenTelemetry Scope details
     * @param metric the metric details
     * @param otelGauge the gauge details
     * @param dpSeqNo the data point sequence number
     * @param dp the data point itself
     */
    public void setFrom(
            final long batchTS,
            final String batchID,
            final int sequenceNum,
            final ResourceMetrics resourceMetric,
            final ScopeMetrics scopeMetric,
            final io.opentelemetry.proto.metrics.v1.Metric metric,
            final Gauge otelGauge,
            final int dpSeqNo,
            final NumberDataPoint dp) {
        this.batchTimestamp = batchTS;
        this.batchUUID = batchID;
        this.seqNo = sequenceNum;
        this.resource = resourceMetric.getResource();
        this.resourceSchemaUrl = resourceMetric.getSchemaUrl();
        this.scope = scopeMetric.getScope();
        this.name = metric.getName();
        this.description = metric.getDescription();
        this.unit = metric.getUnit();
        this.type = metric.getDataCase();
        this.metricSchemaUrl = scopeMetric.getSchemaUrl();
        this.metaData = metric.getMetadataList();

        this.datapointSeqNo = dpSeqNo;
        this.gauge = dp;

        // FIXME: add checks for validity and set isValid, errorMessage?
        isValid = true;
    }

    /**
     * Set the details of a histogram data point.
     * Also verifies the data and potentially sets an error message.
     *
     * @param batchTS the batch timestamp
     * @param batchID the batch id
     * @param sequenceNum the sequence number of this metric
     * @param resourceMetric used to fill-in OpenTelemetry Resource details
     * @param scopeMetric used to fill-in OpenTelemetry Scope details
     * @param metric the metric details
     * @param otelHistogram the histogram details
     * @param dpSeqNo the data point sequence number
     * @param dp the data point itself
     */
    public void setFrom(
            final long batchTS,
            final String batchID,
            final int sequenceNum,
            final ResourceMetrics resourceMetric,
            final ScopeMetrics scopeMetric,
            final io.opentelemetry.proto.metrics.v1.Metric metric,
            final Histogram otelHistogram,
            final int dpSeqNo,
            final HistogramDataPoint dp) {
        this.batchTimestamp = batchTS;
        this.batchUUID = batchID;
        this.seqNo = sequenceNum;
        this.resource = resourceMetric.getResource();
        this.resourceSchemaUrl = resourceMetric.getSchemaUrl();
        this.scope = scopeMetric.getScope();
        this.name = metric.getName();
        this.description = metric.getDescription();
        this.unit = metric.getUnit();
        this.type = metric.getDataCase();
        this.metricSchemaUrl = scopeMetric.getSchemaUrl();
        this.metaData = metric.getMetadataList();

        this.aggregationTemporality =
                otelHistogram.getAggregationTemporality();
        this.datapointSeqNo = dpSeqNo;
        this.histogram = dp;

        // FIXME: add checks for validity and set isValid, errorMessage?
        isValid = true;
    }

    /**
     * Set the details of a number data point.
     * Also verifies the data and potentially sets an error message.
     *
     * @param batchTS the batch timestamp
     * @param batchID the batch id
     * @param sequenceNum the sequence number of this metric
     * @param resourceMetric used to fill-in OpenTelemetry Resource details
     * @param scopeMetric used to fill-in OpenTelemetry Scope details
     * @param metric the metric details
     * @param otelSum the sum details
     * @param dpSeqNo the data point sequence number
     * @param dp the data point itself
     */
    public void setFrom(
            final long batchTS,
            final String batchID,
            final int sequenceNum,
            final ResourceMetrics resourceMetric,
            final ScopeMetrics scopeMetric,
            final io.opentelemetry.proto.metrics.v1.Metric metric,
            final Sum otelSum,
            final int dpSeqNo,
            final NumberDataPoint dp) {
        this.batchTimestamp = batchTS;
        this.batchUUID = batchID;
        this.seqNo = sequenceNum;
        this.resource = resourceMetric.getResource();
        this.resourceSchemaUrl = resourceMetric.getSchemaUrl();
        this.scope = scopeMetric.getScope();
        this.name = metric.getName();
        this.description = metric.getDescription();
        this.unit = metric.getUnit();
        this.type = metric.getDataCase();
        this.metricSchemaUrl = scopeMetric.getSchemaUrl();
        this.metaData = metric.getMetadataList();

        this.isMonotonic = otelSum.getIsMonotonic();
        this.aggregationTemporality = otelSum.getAggregationTemporality();
        this.datapointSeqNo = dpSeqNo;
        this.sum = dp;

        // FIXME: add checks for validity and set isValid, errorMessage?
        isValid = true;
    }

    /**
     * Set the details of a summary data point.
     * Also verifies the data and potentially sets an error message.
     *
     * @param batchTS the batch timestamp
     * @param batchID the batch id
     * @param sequenceNum the sequence number of this metric
     * @param resourceMetric used to fill-in OpenTelemetry Resource details
     * @param scopeMetric used to fill-in OpenTelemetry Scope details
     * @param metric the metric details
     * @param otelSummary the summary details
     * @param dpSeqNo the data point sequence number
     * @param dp the data point itself
     */
    public void setFrom(
            final long batchTS,
            final String batchID,
            final int sequenceNum,
            final ResourceMetrics resourceMetric,
            final ScopeMetrics scopeMetric,
            final io.opentelemetry.proto.metrics.v1.Metric metric,
            final Summary otelSummary,
            final int dpSeqNo,
            final SummaryDataPoint dp) {
        this.batchTimestamp = batchTS;
        this.batchUUID = batchID;
        this.seqNo = sequenceNum;
        this.resource = resourceMetric.getResource();
        this.resourceSchemaUrl = resourceMetric.getSchemaUrl();
        this.scope = scopeMetric.getScope();
        this.name = metric.getName();
        this.description = metric.getDescription();
        this.unit = metric.getUnit();
        this.type = metric.getDataCase();
        this.metricSchemaUrl = scopeMetric.getSchemaUrl();
        this.metaData = metric.getMetadataList();

        this.datapointSeqNo = seqNo;
        this.summary = dp;

        // FIXME: add checks for validity and set isValid, errorMessage?
        isValid = true;
    }

    /**
     * Get the timestamp when the containing OTLP packet was received.
     *
     * @return the timestamp, in ms
     */
    public long getBatchTimestamp() {
        return batchTimestamp;
    }

    /**
     * Get the unique id of the containing OTLP packet.
     *
     * @return the UUID
     */
    public String getBatchUUID() {
        return batchUUID;
    }

    /**
     * Get the sequence number of this metric within the containing
     * OTLP packet.
     *
     * @return the sequence number, zero-based
     */
    public int getSeqNo() {
        return seqNo;
    }

    /**
     * Get the OpenTelemetry Resource attributes of this metric.
     *
     * If configured, additional attributes may be added. See the
     * configuration options.
     *
     * @return the resource attributes
     */
    @Override
    public Iterable<KeyValue> getResourceAttributes() {
        return computeResourceAttributes(resource.getAttributesList());
    }

    /**
     * Get the count of dropped OpenTelemetry Resource attributes.
     *
     * @return the number of attributes dropped
     */
    @Override
    public int getResourceDroppedAttributesCount() {
        return resource.getDroppedAttributesCount();
    }

    /**
     * Get the OpenTelemetry Resource Entities of this metric.
     *
     * @return the resource entities.
     */
    @Override
    public Iterable<EntityRef> getResourceEntityRefs() {
        return resource.getEntityRefsList();
    }

    /**
     * Get the schema URL of the OpenTelemetry Resouce of this metric.
     *
     * @return the schema URL
     */
    @Override
    public String getResourceSchemaUrl() {
        return resourceSchemaUrl;
    }

    /**
     * Get the OpenTelemetry Scope of this metric.
     *
     * @return the scope details
     */
    public InstrumentationScope getScope() {
        return scope;
    }

    /**
     * Get the metric name.
     *
     * @return the name of this metric
     */
    public String getName() {
        return name;
    }

    /**
     * Get the metric description.
     *
     * @return the description of this metric
     */
    public String getDescription() {
        return description;
    }

    /**
     * Get the metric unit of measure.
     *
     * @return the UOM
     */
    public String getUnit() {
        return unit;
    }

    /**
     * Get the metric type - GAUGE, HISTOGRAM, etc...
     *
     * @return the type
     */
    public DataCase getType() {
        return type;
    }

    /**
     * Get the sequence number of this datapoint within the containing metric.
     *
     * @return the sequence number, zero-based
     */
    public int getDatapointSeqNo() {
        return datapointSeqNo;
    }

    /**
     * Get the exponential histogram details.
     *
     * @return the details or null if metric is of different type
     */
    public ExponentialHistogramDataPoint getExponentialHistogram() {
        return exponentialHistogram;
    }

    /**
     * Get the gauge details.
     *
     * @return the details or null if metric is of different type
     */
    public NumberDataPoint getGauge() {
        return gauge;
    }

    /**
     * Get the histogram details.
     *
     * @return the details or null if metric is of different type
     */
    public HistogramDataPoint getHistogram() {
        return histogram;
    }

    /**
     * Get the number details.
     *
     * @return the details or null if metric is of different type
     */
    public NumberDataPoint getSum() {
        return sum;
    }

    /**
     * Get the summary details.
     *
     * @return the details or null if metric is of different type
     */
    public SummaryDataPoint getSummary() {
        return summary;
    }

    /**
     * Get the schema URL of the OpenTelemetry metric.
     *
     * @return the schema URL
     */
    public String getMetricSchemaUrl() {
        return metricSchemaUrl;
    }

    /**
     * Get the aggregation temporality of this metric -
     * DELTA, CUMULATIVE, etc...
     *
     * @return the aggregation temporality
     */
    public AggregationTemporality getAggregationTemporality() {
        return aggregationTemporality;
    }

    /**
     * Check if the metric is monotonic.
     *
     * @return true if monotonic
     */
    public boolean isMonotonic() {
        return isMonotonic;
    }

    /**
     * Check if this record is valid.
     *
     * @return true if data is valid
     */
    public boolean isValid() {
        return isValid;
    }

    /**
     * Get the message associated with an error in the data.
     *
     * @return the error message or null if data is valid
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Get the metric meta data.
     *
     * @return metric meta data as a list of {@link KeyValue}s.
     */
    public List<KeyValue> getMetaData() {
        return metaData;
    }
}
