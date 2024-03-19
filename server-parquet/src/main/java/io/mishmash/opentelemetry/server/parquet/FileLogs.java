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
import io.mishmash.opentelemetry.server.collector.Log;
import io.mishmash.opentelemetry.server.collector.LogsSubscriber;
import io.mishmash.opentelemetry.server.parquet.persistence.proto.v1.LogsPersistenceProto.PersistedLog;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import io.opentelemetry.proto.common.v1.AnyValue;

/**
 * Subscribes to incoming OpenTelemetry logs and writes them to parquet
 * files using {@link ParquetPersistence}.
 */
public class FileLogs implements LogsSubscriber {

    /**
     * The {@link java.util.logging.Logger} instance used.
     */
    private static final Logger LOG =
            Logger.getLogger(FileLogs.class.getName());

    /**
     * The {@link java.util.concurrent.Flow.Subscription} that
     * supplies incoming records.
     */
    private Subscription subscription;
    /**
     * The {@link ParquetPersistence} instance that writes
     * incoming records to files.
     */
    private ParquetPersistence<PersistedLog> parquet;
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
     * Creates a new OpenTelemetry logs subscriber to
     * save logs into parquet files.
     *
     * @param baseFileName the file name prefix (including any path)
     * @param instrumentation helper instance for own telemetry
     * @throws IOException if files cannot be opened for writing
     */
    public FileLogs(
                final String baseFileName,
                final Instrumentation instrumentation)
            throws IOException {
        parquet = new ParquetPersistence<>(baseFileName, PersistedLog.class);

        this.otel = instrumentation;

        numWritten = otel.newLongCounter(
            "parquet_logs_written",
            "1",
            "Number of log entries successfully written to output file");

        numFailed = otel.newLongCounter(
            "parquet_logs_failed",
            "1",
            "Number of log entries that could not be written due to an error");

        numCompletedFiles = otel.newLongGauge(
            "parquet_logs_completed_files",
            "1",
            "Number of closed and completed log files",
            g -> g.record(
                    parquet == null
                        ? 0
                        : parquet.getNumCompletedFiles()));

        numRecordsInCurrentFile = otel.newLongGauge(
            "parquet_logs_current_file_written",
            "1",
            "Number of log records written in current file",
            g -> g.record(
                    parquet == null
                        ? 0
                        : parquet.getCurrentNumCompletedRecords()));

        currentFileSize = otel.newLongGauge(
            "parquet_logs_current_file_size",
            "By",
            "Size (in bytes) of the current logs file",
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
                    "Failed to close logs parquet file writer",
                    e);
        } finally {
            parquet = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onError(final Throwable error) {
        /*
         * triggered when the publisher is closed exceptionally
         * (or other errors)
         */
        LOG.log(Level.WARNING,
            "Closing logs parquet file writer because of a publisher error",
            error);

        try {
            if (parquet != null) {
                parquet.close();
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING,
                    "Failed to close logs parquet file writer",
                    e);
        } finally {
            parquet = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onNext(final Log log) {

        try (Scope s = log.getOtelContext().makeCurrent()) {
            Span span = otel.startNewSpan("otel.file.logs");

            try {
                span.addEvent("Build persisted log");
                PersistedLog.Builder builder = PersistedLog.newBuilder()
                        .setBatchTimestamp(log.getBatchTimestamp())
                        .setBatchUUID(log.getBatchUUID())
                        .setSeqNo(log.getSeqNo())
                        .setIsValid(log.isValid());

                if (log.getErrorMessage() != null) {
                    builder = builder.setErrorMessage(log.getErrorMessage());
                }

                if (log.getResource() != null) {
                    builder = builder
                        .addAllResourceAttributes(
                                log.getResource().getAttributesList())
                        .setResourceDroppedAttributesCount(
                                log.getResource().getDroppedAttributesCount());
                }

                if (log.getResourceSchemaUrl() != null) {
                    builder = builder
                        .setResourceSchemaUrl(log.getResourceSchemaUrl());
                }

                if (log.getScope() != null) {
                    builder = builder
                        .setScopeName(log.getScope().getName())
                        .setScopeVersion(log.getScope().getVersion())
                        .addAllScopeAttributes(
                                log.getScope().getAttributesList())
                        .setScopeDroppedAttributesCount(
                                log.getScope().getDroppedAttributesCount());
                }

                if (log.getLog() != null) {
                    builder = builder
                        .setTimeUnixNano(log.getLog().getTimeUnixNano())
                        .setObservedTimeUnixNano(
                                log.getLog().getObservedTimeUnixNano())
                        .setSeverityNumber(log.getLog().getSeverityNumber())
                        .setSeverityText(log.getLog().getSeverityText())
                        .addAllAttributes(log.getLog().getAttributesList())
                        .setDroppedAttributesCount(
                                log.getLog().getDroppedAttributesCount())
                        .setFlags(log.getLog().getFlags())
                        .setTraceId(log.getLog().getTraceId())
                        .setSpanId(log.getLog().getSpanId());

                    AnyValue body = log.getLog().getBody();

                    builder = builder.setBodyType(body.getValueCase().name());

                    switch (body.getValueCase()) {
                    case ARRAY_VALUE:
                        builder = builder.setBodyArray(body.getArrayValue());
                        break;
                    case BOOL_VALUE:
                        builder = builder.setBodyBool(body.getBoolValue());
                        break;
                    case BYTES_VALUE:
                        builder = builder.setBodyBytes(body.getBytesValue());
                        break;
                    case DOUBLE_VALUE:
                        builder = builder.setBodyDouble(body.getDoubleValue());
                        break;
                    case INT_VALUE:
                        builder = builder.setBodyInt(body.getIntValue());
                        break;
                    case KVLIST_VALUE:
                        builder = builder.setBodyKvlist(body.getKvlistValue());
                        break;
                    case STRING_VALUE:
                        builder = builder.setBodyString(body.getStringValue());
                        break;
                    case VALUE_NOT_SET:
                        // FIXME: what to do when not set?
                        break;
                    default:
                        // FIXME: should not ignore
                        break;
                    }
                }

                if (log.getLogSchemaUrl() != null) {
                    builder = builder.setLogSchemaUrl(log.getLogSchemaUrl());
                }

                span.addEvent("Write persisted log to file");
                parquet.write(builder.build());

                numWritten.add(1);

                log.complete(this);
            } catch (Exception e) {
                numFailed.add(1);

                log.completeExceptionally(e);
                otel.addErrorEvent(span, e);
            }

            span.end();
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
