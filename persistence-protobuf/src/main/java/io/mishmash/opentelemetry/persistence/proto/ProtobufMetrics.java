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

package io.mishmash.opentelemetry.persistence.proto;

import io.mishmash.opentelemetry.persistence.proto.v1.MetricsPersistenceProto.PersistedMetric;
import io.mishmash.opentelemetry.server.collector.MetricDataPoint;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;

/**
 * Utility class to help with protobuf serialization of
 * {@link MetricDataPoint} instances.
 */
public final class ProtobufMetrics {

    private ProtobufMetrics() {
        // constructor is hidden
    }

    /**
     * Get a protobuf representation of a {@link MetricDataPoint}.
     *
     * @param metric the metric data point
     * @return a populated {@link PersistedMetric.Builder}
     */
    public static PersistedMetric.Builder buildMetricDataPoint(
            final MetricDataPoint metric) {
        PersistedMetric.Builder builder = PersistedMetric.newBuilder()
                .setBatchTimestamp(metric.getBatchTimestamp())
                .setBatchUUID(metric.getBatchUUID())
                .setSeqNo(metric.getSeqNo())
                .setIsValid(metric.isValid())
                .setDatapointSeqNo(metric.getDatapointSeqNo());

        if (metric.getErrorMessage() != null) {
            builder = builder
                    .setErrorMessage(metric.getErrorMessage());
        }

        if (metric.getResource() != null) {
            builder = builder
                    .addAllResourceAttributes(
                            metric.getResource().getAttributesList())
                    .setResourceDroppedAttributesCount(
                            metric.getResource()
                                .getDroppedAttributesCount());
        }

        if (metric.getResourceSchemaUrl() != null) {
            builder = builder
                    .setResourceSchemaUrl(
                            metric.getResourceSchemaUrl());
        }

        if (metric.getScope() != null) {
            builder = builder
                    .setScopeName(metric.getScope().getName())
                    .setScopeVersion(metric.getScope().getVersion())
                    .addAllScopeAttributes(
                            metric.getScope().getAttributesList())
                    .setScopeDroppedAttributesCount(
                            metric.getScope()
                                .getDroppedAttributesCount());
        }

        if (metric.getName() != null) {
            builder = builder.setName(metric.getName());
        }

        if (metric.getDescription() != null) {
            builder = builder.setDescription(metric.getDescription());
        }

        if (metric.getUnit() != null) {
            builder = builder.setUnit(metric.getUnit());
        }

        if (metric.getType() != null) {
            builder = builder.setType(metric.getType().name());
        }

        if (metric.getMetaData() != null) {
            builder = builder
                    .addAllMetricMetadata(metric.getMetaData());
        }

        switch (metric.getType()) {
        case EXPONENTIAL_HISTOGRAM:
            ExponentialHistogramDataPoint ehdp =
                metric.getExponentialHistogram();

            builder = builder
                .addAllAttributes(ehdp.getAttributesList())
                .setStartTimeUnixNano(ehdp.getStartTimeUnixNano())
                .setTimeUnixNano(ehdp.getTimeUnixNano())
                .addAllExemplars(ehdp.getExemplarsList())
                .setFlags(ehdp.getFlags())
                .setExponentialHistogramCount(ehdp.getCount())
                .setExponentialHistogramSum(ehdp.getSum())
                .setExponentialHistogramScale(ehdp.getScale())
                .setExponentialHistogramZeroCount(ehdp.getZeroCount())
                .setExponentialHistogramPositive(ehdp.getPositive())
                .setExponentialHistogramNegative(ehdp.getNegative())
                .setExponentialHistogramMin(ehdp.getMin())
                .setExponentialHistogramMax(ehdp.getMax())
                .setExponentialHistogramZeroThreshold(
                        ehdp.getZeroThreshold())
                .setAggregationTemporality(
                        metric.getAggregationTemporality());
            break;
        case GAUGE:
            NumberDataPoint gdp = metric.getGauge();

            builder = builder
                .addAllAttributes(gdp.getAttributesList())
                .setStartTimeUnixNano(
                        gdp.getStartTimeUnixNano())
                .setTimeUnixNano(gdp.getTimeUnixNano())
                .addAllExemplars(gdp.getExemplarsList())
                .setFlags(gdp.getFlags())
                .setGaugeType(gdp.getValueCase().name())
                .setGaugeDouble(gdp.getAsDouble())
                .setGaugeInt(gdp.getAsInt());
            break;
        case HISTOGRAM:
            HistogramDataPoint hdp = metric.getHistogram();

            builder = builder
                .addAllAttributes(hdp.getAttributesList())
                .setStartTimeUnixNano(
                        hdp.getStartTimeUnixNano())
                .setTimeUnixNano(hdp.getTimeUnixNano())
                .addAllExemplars(hdp.getExemplarsList())
                .setFlags(hdp.getFlags())
                .setHistogramCount(hdp.getCount())
                .setHistogramSum(hdp.getSum())
                .addAllHistogramBucketCounts(
                        hdp.getBucketCountsList())
                .addAllHistogramExplicitBounds(
                        hdp.getExplicitBoundsList())
                .setHistogramMin(hdp.getMin())
                .setHistogramMax(hdp.getMax())
                .setAggregationTemporality(
                        metric.getAggregationTemporality());
            break;
        case SUM:
            NumberDataPoint ndp = metric.getSum();

            builder = builder
                .addAllAttributes(ndp.getAttributesList())
                .setStartTimeUnixNano(
                        ndp.getStartTimeUnixNano())
                .setTimeUnixNano(ndp.getTimeUnixNano())
                .addAllExemplars(ndp.getExemplarsList())
                .setFlags(ndp.getFlags())
                .setSumType(ndp.getValueCase().name())
                .setSumDouble(ndp.getAsDouble())
                .setSumInt(ndp.getAsInt())
                .setAggregationTemporality(
                        metric.getAggregationTemporality())
                .setIsMonotonic(metric.isMonotonic());
            break;
        case SUMMARY:
            SummaryDataPoint sdp = metric.getSummary();

            builder = builder
                .addAllAttributes(sdp.getAttributesList())
                .setStartTimeUnixNano(
                        sdp.getStartTimeUnixNano())
                .setTimeUnixNano(sdp.getTimeUnixNano())
                .setFlags(sdp.getFlags())
                .setSummaryCount(sdp.getCount())
                .setSummarySum(sdp.getSum())
                .addAllSummaryQuantileValues(
                        sdp.getQuantileValuesList());
            break;
        case DATA_NOT_SET:
            // FIXME: skip for now
            break;
        default:
            // FIXME: unknown type...
            break;
        }

        if (metric.getMetricSchemaUrl() != null) {
            builder = builder
                    .setMetricSchemaUrl(metric.getMetricSchemaUrl());
        }

        return builder;
    }
}
