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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.parsers.CloseableIteratorWithMetadata;
import org.apache.druid.java.util.common.parsers.ParseException;

import io.mishmash.opentelemetry.persistence.proto.v1.MetricsPersistenceProto.PersistedMetric;
import io.mishmash.opentelemetry.persistence.protobuf.ProtobufMetrics;
import io.mishmash.opentelemetry.server.collector.MetricDataPoint;
import io.mishmash.opentelemetry.server.collector.MetricsFlattener;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;

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
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<InputRow> parseInputRows(
            final PersistedMetric intermediateRow)
                    throws IOException, ParseException {
        return Collections.singletonList(
                MapInputRowParser.parse(
                        schema,
                        ProtobufMetrics.toJsonMap(intermediateRow, false)));
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
        return Collections.singletonList(
                ProtobufMetrics.toJsonMap(intermediateRow, true));
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
}
