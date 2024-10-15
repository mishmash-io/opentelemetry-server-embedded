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

package io.mishmash.opentelemetry.druid.format;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.parsers.CloseableIteratorWithMetadata;
import org.apache.druid.java.util.common.parsers.ParseException;

import com.google.protobuf.Descriptors.FieldDescriptor;

import io.mishmash.opentelemetry.persistence.proto.v1.MetricsPersistenceProto.PersistedMetric;
import io.mishmash.opentelemetry.persistence.protobuf.ProtobufMetrics;
import io.mishmash.opentelemetry.server.collector.MetricDataPoint;
import io.mishmash.opentelemetry.server.collector.MetricsFlattener;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint.Buckets;

/**
 * An {@link IntermediateRowParsingReader} for OpenTelemetry metrics signals.
 *
 * Takes an {@link InputEntity} containing a 'raw' protobuf OTLP
 * {@link ExportMetricsServiceRequest} and 'flattens' it using
 * {@link MetricsFlattener}, or takes a single (already flattened)
 * {@link PersistedMetric} protobuf message.
 */
public class MetricsReader
        extends IntermediateRowParsingReader<PersistedMetric> {

    /**
     * The name of the column for the 'extracted' histogram values.
     */
    private static final String COL_EXTRACTED_HISTOGRAM = "histogram";
    /**
     * The name of the column for the 'extracted' exponential histogram values.
     */
    private static final String COL_EXTRACTED_EXPONENTIAL_HISTOGRAM =
            "exponential_histogram";
    /**
     * The list of default candidates for metrics columns.
     */
    private static final String[] DEFAULT_METRIC_NAMES = new String[] {
            "gauge_double",
            "gauge_int",
            "sum_double",
            "sum_int",
            "histogram_count",
            "histogram_sum",
            "histogram_min",
            "histogram_max",
            "exponential_histogram_count",
            "exponential_histogram_sum",
            "exponential_histogram_scale",
            "exponential_histogram_zero_count",
            "exponential_histogram_min",
            "exponential_histogram_max",
            "exponential_histogram_zero_threshold",
            "summary_count",
            "summary_sum"
    };

    /**
     * The source to read data from.
     */
    private InputEntity source;
    /**
     * True if the 'raw' format was configured.
     */
    private boolean isRaw = false;
    /**
     * The ingestion schema config.
     */
    private InputRowSchema schema;
    /**
     * Keeps a reference to all possible dimension column names.
     */
    private Set<String> metricsDimensions;
    /**
     * Keeps a reference to all possible metric column names.
     */
    private Set<String> metricsMetrics;

    /**
     * Create an OTLP metrics reader.
     *
     * @param rowSchema the schema as set in ingestion config
     * @param input the {@link InputEntity} containing protobuf-encoded bytes
     * @param isRawFormat true if input contains a 'raw'
     * {@link ExportMetricsServiceRequest}
     */
    public MetricsReader(
            final InputRowSchema rowSchema,
            final InputEntity input,
            final boolean isRawFormat) {
        this.schema = rowSchema;
        this.source = input;
        this.isRaw = isRawFormat;

        metricsMetrics = new HashSet<>();

        /*
         * Before we configure the defaults for metrics -
         * make sure a column is not already considered a
         * dimension instead.
         */
        Set<String> configuredDimensions = new HashSet<>();
        configuredDimensions.addAll(rowSchema
                                        .getDimensionsSpec()
                                        .getDimensionNames());
        configuredDimensions.addAll(rowSchema
                                        .getDimensionsSpec()
                                        .getDimensionExclusions());

        for (int i = 0; i < DEFAULT_METRIC_NAMES.length; i++) {
            String metricName = DEFAULT_METRIC_NAMES[i];

            if (!configuredDimensions.contains(metricName)) {
                metricsMetrics.add(metricName);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<InputRow> parseInputRows(
            final PersistedMetric intermediateRow)
                    throws IOException, ParseException {
        Map<String, Object> rowMap = ProtobufMetrics
                .toJsonMap(intermediateRow, false);

        rowMap.putAll(extractBuckets(intermediateRow, false));

        return Collections.singletonList(
                MapInputRowParser.parse(
                        schema.getTimestampSpec(),
                        MapInputRowParser.findDimensions(
                                schema.getTimestampSpec(),
                                schema.getDimensionsSpec(),
                                allDimensions()),
                        rowMap));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Map<String, Object>> toMap(
            final PersistedMetric intermediateRow) throws IOException {
        /*
         * This is called when Druid is sampling data, so, provide defaults
         * for missing fields.
         */
        Map<String, Object> res = ProtobufMetrics
                .toJsonMap(intermediateRow, true);

        res.putAll(extractBuckets(intermediateRow, true));

        return Collections.singletonList(res);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CloseableIteratorWithMetadata<PersistedMetric>
            intermediateRowIteratorWithMetadata() throws IOException {
        try (InputStream is = source().open()) {
            if (isRaw) {
                ExportMetricsServiceRequest req =
                        ExportMetricsServiceRequest.parseFrom(is);

                return new RawIterator<>(
                        new MetricsFlattener(null, null, req, null)) {
                    @Override
                    public PersistedMetric convert(
                            final MetricDataPoint element) {
                        return ProtobufMetrics
                                .buildMetricDataPoint(element).build();
                    }

                    @Override
                    public void initMetaFor(
                            final PersistedMetric element) {
                        initIteratorMeta(this, element);
                    }
                };
            } else {
                PersistedMetric m = PersistedMetric.parseFrom(is);

                return new FlatIterator<>(m) {
                    @Override
                    public void initMetaFor(
                            final PersistedMetric element) {
                        initIteratorMeta(this, element);
                    }
                };
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String intermediateRowAsString(final PersistedMetric row) {
        return row.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected InputEntity source() {
        return source;
    }

    /**
     * Initialize the metadata associated with the given metric data point.
     *
     * A {@link CloseableIteratorWithMetadata} has to provide metadata
     * for each element it returns through its next() method. This
     * metadata is computed here, before next() returns.
     *
     * @param it - the iterator requesting metadata
     * @param metric - the new element to be returned by next()
     */
    protected void initIteratorMeta(
            final BaseIterator<PersistedMetric> it,
            final PersistedMetric metric) {
        it.clearMeta();

        it.setMeta("batchUUID", metric.getBatchUUID());
        it.setMeta("batchTimestamp", metric.getBatchTimestamp());
        it.setMeta("metricSeqNo", metric.getSeqNo());
        it.setMeta("dataPointSeqNo", metric.getDatapointSeqNo());
    }

    /**
     * Return all possible dimensions names.
     *
     * This method is used to compute a full list of dimension names
     * (including optional values that might be missing) when supplying rows.
     *
     * @return a {@link Set} of all possible dimension names
     */
    protected Set<String> allDimensions() {
        if (metricsDimensions == null) {
            metricsDimensions = new HashSet<>();

            for (FieldDescriptor field : PersistedMetric
                                            .getDescriptor()
                                            .getFields()) {
                String fieldName = field.getName();
                if (!metricsMetrics.contains(fieldName)
                        && !schema.getMetricNames().contains(fieldName)) {
                    metricsDimensions.add(fieldName);
                }
            }

            if (!metricsMetrics.contains(COL_EXTRACTED_HISTOGRAM)
                    && !schema
                            .getMetricNames()
                            .contains(COL_EXTRACTED_HISTOGRAM)) {
                metricsDimensions.add(COL_EXTRACTED_HISTOGRAM);
            }

            if (!metricsMetrics.contains(COL_EXTRACTED_EXPONENTIAL_HISTOGRAM)
                    && !schema
                            .getMetricNames()
                            .contains(COL_EXTRACTED_EXPONENTIAL_HISTOGRAM)) {
                metricsDimensions.add(COL_EXTRACTED_EXPONENTIAL_HISTOGRAM);
            }
        }

        return metricsDimensions;
    }

    /**
     * Extract some metrics types into more useful columns.
     *
     * For example - this method extracts the bucket counts of Histogram
     * metrics into a single 'array' column, where elements contain the
     * bucket bounds and the count.
     *
     * @param metric the metric being processed
     * @param withDefaults if defaults for missing values should be added
     * @return a {@link Map} with the new, extracted columns (or defaults)
     */
    protected Map<String, Object> extractBuckets(
            final PersistedMetric metric,
            final boolean withDefaults) {
        Map<String, Object> res = new LinkedHashMap<>();

        if (withDefaults) {
            res.put(COL_EXTRACTED_HISTOGRAM,
                    Collections.emptyList());
            res.put(COL_EXTRACTED_EXPONENTIAL_HISTOGRAM,
                    Collections.emptyList());
        }

        switch (metric.getType()) {
        case "HISTOGRAM":
            if (metric.getHistogramBucketCountsCount()
                    != metric.getHistogramExplicitBoundsCount() + 1) {
                throw new IllegalArgumentException(
                    "Illegal metric bucket array sizes");
            }

            Double prevBound = null;
            List<Map<String, Object>> histogram =
                    new ArrayList<>(metric.getHistogramBucketCountsCount());
            Map<String, Object> bucket;

            for (int i = 0;
                    i < metric.getHistogramExplicitBoundsCount();
                    i++) {
                bucket = new LinkedHashMap<>();

                bucket.put("lower_bound", prevBound);
                bucket.put("upper_bound",
                        prevBound = metric.getHistogramExplicitBounds(i));
                bucket.put("count", metric.getHistogramBucketCounts(i));

                histogram.add(bucket);
            }

            bucket = new LinkedHashMap<>();

            bucket.put("lower_bound", prevBound);
            bucket.put("upper_bound", null);
            bucket.put("count",
                    metric.getHistogramBucketCounts(
                            metric.getHistogramBucketCountsCount() - 1));

            histogram.add(bucket);

            res.put(COL_EXTRACTED_HISTOGRAM, histogram);

            break;

        case "EXPONENTIAL_HISTOGRAM":
            double base = Math.pow(
                    2,
                    Math.pow(2,
                            -metric.getExponentialHistogramScale()));

            Buckets negative = metric.getExponentialHistogramNegative();
            Buckets positive = metric.getExponentialHistogramPositive();
            int negativeOffset = negative.getOffset();
            int positiveOffset = positive.getOffset();
            List<Map<String, Object>> exp = new ArrayList<>(
                    negative.getBucketCountsCount()
                    + positive.getBucketCountsCount());

            for (int i = 0; i < negative.getBucketCountsCount(); i++) {
                Map<String, Object> eb = new LinkedHashMap<>();

                eb.put("lower_bound", -Math.pow(base, negativeOffset + i));
                eb.put("upper_bound", -Math.pow(base, negativeOffset + i + 1));
                eb.put("count", negative.getBucketCounts(i));

                exp.add(eb);
            }

            for (int i = 0; i < positive.getBucketCountsCount(); i++) {
                Map<String, Object> eb = new LinkedHashMap<>();

                eb.put("lower_bound", Math.pow(base, positiveOffset + i));
                eb.put("upper_bound", Math.pow(base, positiveOffset + i + 1));
                eb.put("count", positive.getBucketCounts(i));

                exp.add(eb);
            }

            res.put(COL_EXTRACTED_EXPONENTIAL_HISTOGRAM, exp);

            break;

        default:
        }

        return res;
    }
}
