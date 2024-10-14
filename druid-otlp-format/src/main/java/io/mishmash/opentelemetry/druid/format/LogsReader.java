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

import io.mishmash.opentelemetry.persistence.proto.v1.LogsPersistenceProto.PersistedLog;
import io.mishmash.opentelemetry.persistence.protobuf.ProtobufLogs;
import io.mishmash.opentelemetry.server.collector.Log;
import io.mishmash.opentelemetry.server.collector.LogsFlattener;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;

/**
 * An {@link IntermediateRowParsingReader} for OpenTelemetry logs signals.
 *
 * Takes an {@link InputEntity} containing a 'raw' protobuf OTLP
 * {@link ExportLogsServiceRequest} and 'flattens' it using
 * {@link LogsFlattener}, or takes a single (already flattened)
 * {@link PersistedLog} protobuf message.
 */
public class LogsReader extends IntermediateRowParsingReader<PersistedLog> {

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
     * Create an OTLP logs reader.
     *
     * @param rowSchema the schema as set in ingestion config
     * @param input the {@link InputEntity} containing protobuf-encoded bytes
     * @param isRawFormat true if input contains a 'raw'
     * {@link ExportLogsServiceRequest}
     */
    public LogsReader(
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
            final PersistedLog intermediateRow)
                    throws IOException, ParseException {
        return Collections.singletonList(
                MapInputRowParser.parse(
                        schema,
                        ProtobufLogs.toJsonMap(intermediateRow, false)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Map<String, Object>> toMap(
            final PersistedLog intermediateRow) throws IOException {
        /*
         * This is called when Druid is sampling data, so, provide defaults
         * for missing fields.
         */
        return Collections.singletonList(
                ProtobufLogs.toJsonMap(intermediateRow, true));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CloseableIteratorWithMetadata<PersistedLog>
            intermediateRowIteratorWithMetadata() throws IOException {
        try (InputStream is = source().open()) {
            if (isRaw) {
                ExportLogsServiceRequest req =
                        ExportLogsServiceRequest.parseFrom(is);

                return new RawIterator<>(
                        new LogsFlattener(null, null, req, null)) {
                    @Override
                    public PersistedLog convert(final Log element) {
                        return ProtobufLogs.buildLog(element).build();
                    }

                    @Override
                    public void initMetaFor(
                            final PersistedLog element) {
                        initIteratorMeta(this, element);
                    }
                };
            } else {
                PersistedLog log = PersistedLog.parseFrom(is);

                return new FlatIterator<>(log) {
                    @Override
                    public void initMetaFor(
                            final PersistedLog element) {
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
    protected String intermediateRowAsString(final PersistedLog row) {
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
     * Initialize the metadata associated with the given log.
     *
     * A {@link CloseableIteratorWithMetadata} has to provide metadata
     * for each element it returns through its next() method. This
     * metadata is computed here, before next() returns.
     *
     * @param it - the iterator requesting metadata
     * @param log - the new element to be returned by next()
     */
    protected void initIteratorMeta(
            final BaseIterator<PersistedLog> it,
            final PersistedLog log) {
        it.clearMeta();

        it.setMeta("batchUUID", log.getBatchUUID());
        it.setMeta("batchTimestamp", log.getBatchTimestamp());
        it.setMeta("seqNo", log.getSeqNo());
    }
}
