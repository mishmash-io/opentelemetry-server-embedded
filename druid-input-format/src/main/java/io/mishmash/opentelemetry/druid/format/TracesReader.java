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
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.java.util.common.parsers.CloseableIteratorWithMetadata;
import org.apache.druid.java.util.common.parsers.ParseException;

import io.mishmash.opentelemetry.persistence.proto.ProtobufSpans;
import io.mishmash.opentelemetry.persistence.proto.v1.TracesPersistenceProto.PersistedSpan;
import io.mishmash.opentelemetry.server.collector.Span;
import io.mishmash.opentelemetry.server.collector.TracesFlattener;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;

/**
 * An {@link IntermediateRowParsingReader} for OpenTelemetry traces signals.
 *
 * Takes an {@link InputEntity} containing a 'raw' protobuf OTLP
 * {@link ExportTraceServiceRequest} and 'flattens' it using
 * {@link TracesFlattener}, or takes a single (already flattened)
 * {@link PersistedSpan} protobuf message.
 */
public class TracesReader extends IntermediateRowParsingReader<PersistedSpan> {

    /**
     * The source to read data from.
     */
    private InputEntity source;
    /**
     * True if the 'raw' format was configured.
     */
    private boolean isRaw = false;

    /**
     * Create an OTLP logs reader.
     *
     * @param input the {@link InputEntity} containing protobuf-encoded bytes
     * @param isRawFormat true if input contains a 'raw'
     * {@link ExportTraceServiceRequest}
     */
    public TracesReader(
            final InputEntity input,
            final boolean isRawFormat) {
        this.source = input;
        this.isRaw = isRawFormat;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<InputRow> parseInputRows(
            final PersistedSpan intermediateRow)
                    throws IOException, ParseException {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Map<String, Object>> toMap(
            final PersistedSpan intermediateRow) throws IOException {
        return Collections.singletonList(
                ProtobufSpans.toJsonMap(intermediateRow));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CloseableIteratorWithMetadata<PersistedSpan>
            intermediateRowIteratorWithMetadata() throws IOException {
        try (InputStream is = source().open()) {
            if (isRaw) {
                ExportTraceServiceRequest req =
                        ExportTraceServiceRequest.parseFrom(is);

                return new RawIterator<>(
                        new TracesFlattener(null, null, req, null)) {
                    @Override
                    public PersistedSpan convert(final Span element) {
                        return ProtobufSpans.buildSpan(element).build();
                    }

                    @Override
                    public void initMetaFor(
                            final PersistedSpan element) {
                        initIteratorMeta(this, element);
                    }
                };
            } else {
                PersistedSpan span = PersistedSpan.parseFrom(is);

                return new FlatIterator<>(span) {
                    @Override
                    public void initMetaFor(
                            final PersistedSpan element) {
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
    protected String intermediateRowAsString(final PersistedSpan row) {
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
     * Initialize the metadata associated with the given trace span.
     *
     * A {@link CloseableIteratorWithMetadata} has to provide metadata
     * for each element it returns through its next() method. This
     * metadata is computed here, before next() returns.
     *
     * @param it - the iterator requesting metadata
     * @param span - the new element to be returned by next()
     */
    protected void initIteratorMeta(
            final BaseIterator<PersistedSpan> it,
            final PersistedSpan span) {
        it.clearMeta();

        it.setMeta("batchUUID", span.getBatchUUID());
        it.setMeta("batchTimestamp", span.getBatchTimestamp());
        it.setMeta("seqNo", span.getSeqNo());
    }
}
