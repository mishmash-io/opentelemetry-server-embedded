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

package io.mishmash.opentelemetry.server.parquet;

import java.io.IOException;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.mishmash.opentelemetry.server.collector.Instrumentation;
import io.mishmash.opentelemetry.server.collector.Span;
import io.mishmash.opentelemetry.server.collector.SpansSubscriber;
import io.mishmash.opentelemetry.persistence.proto.ProtobufSpans;
import io.mishmash.opentelemetry.persistence.proto.v1.TracesPersistenceProto.PersistedSpan;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.context.Scope;

/**
 * Subscribes to incoming OpenTelemetry traces and writes them to parquet
 * files using {@link ParquetPersistence}.
 */
public class FileSpans implements SpansSubscriber {

    /**
     * The {@link java.util.logging.Logger} instance used.
     */
    private static final Logger LOG =
            Logger.getLogger(FileSpans.class.getName());

    /**
     * The {@link java.util.concurrent.Flow.Subscription} that
     * supplies incoming records.
     */
    private Subscription subscription;
    /**
     * The {@link ParquetPersistence} instance that writes
     * incoming records to files.
     */
    private ParquetPersistence<PersistedSpan> parquet;
    /**
     * An {@link io.mishmash.opentelemetry.server.collector.Instrumentation}
     * helper to manage our own telemetry.
     */
    private Instrumentation otel;
    /**
     * A telemetry metric of the number of records written.
     */
    private LongCounter numWritten;
    /**
     * A telemetry metric of the number of records that failed.
     */
    private LongCounter numFailed;
    /**
     * A telemetry metric of the number of files written so far.
     */
    private ObservableLongGauge numCompletedFiles;
    /**
     * A telemetry metric of the number of records written in the
     * current output file.
     */
    private ObservableLongGauge numRecordsInCurrentFile;
    /**
     * A telemetry metric of the current output file size.
     */
    private ObservableLongGauge currentFileSize;

    /**
     * Creates a new OpenTelemetry traces subscriber to
     * save spans into parquet files.
     *
     * @param baseFileName the file name prefix (including any path)
     * @param instrumentation helper instance for own telemetry
     * @throws IOException if files cannot be opened for writing
     */
    public FileSpans(
                final String baseFileName,
                final Instrumentation instrumentation)
            throws IOException {
        parquet = new ParquetPersistence<>(baseFileName, PersistedSpan.class);

        this.otel = instrumentation;

        numWritten = otel.newLongCounter(
            "parquet_traces_written",
            "1",
            "Number of trace entries successfully written to output file");

        numFailed = otel.newLongCounter(
            "parquet_traces_failed",
            "1",
            """
                Number of trace entries that could not be written \
                due to an error""");

        numCompletedFiles = otel.newLongGauge(
            "parquet_traces_completed_files",
            "1",
            "Number of closed and completed trace files",
            g -> g.record(
                    parquet == null
                        ? 0
                        : parquet.getNumCompletedFiles()));

        numRecordsInCurrentFile = otel.newLongGauge(
            "parquet_traces_current_file_written",
            "1",
            "Number of trace records written in current file",
            g -> g.record(
                    parquet == null
                        ? 0
                        : parquet.getCurrentNumCompletedRecords()));

        currentFileSize = otel.newLongGauge(
            "parquet_traces_current_file_size",
            "By",
            "Size (in bytes) of the current traces file",
            g -> g.record(
                    parquet == null
                        ? 0
                        : parquet.getCurrentDataSize()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onComplete() {
        // triggered by a call to the publisher's close method
        try {
            if (parquet != null) {
                parquet.close();
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING,
                    "Failed to close traces parquet file writer",
                    e);
        } finally {
            parquet = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onError(final Throwable t) {
        /*
         * triggered when the publisher is closed exceptionally
         * (or other errors)
         */
        LOG.log(Level.WARNING,
            "Closing traces parquet file writer because of a publisher error",
            t);

        try {
            if (parquet != null) {
                parquet.close();
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING,
                    "Failed to close traces parquet file writer",
                    e);
        } finally {
            parquet = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onNext(final Span span) {
        try (Scope s = span.getOtelContext().makeCurrent()) {
            io.opentelemetry.api.trace.Span otelSpan
                = otel.startNewSpan("otel.file.spans");

            try {
                otelSpan.addEvent("Build persisted span");
                otelSpan.addEvent("Write persisted span to file");
                parquet.write(ProtobufSpans
                        .buildSpan(span)
                        .build());

                numWritten.add(1);

                span.complete(this);
            } catch (Exception e) {
                numFailed.add(1);

                span.completeExceptionally(e);
                otel.addErrorEvent(otelSpan, e);
            }

            otelSpan.end();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onSubscribe(final Subscription newSubscription) {
        this.subscription = newSubscription;

        subscription.request(Long.MAX_VALUE);
    }
}
