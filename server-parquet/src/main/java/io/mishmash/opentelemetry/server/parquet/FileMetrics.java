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
import io.mishmash.opentelemetry.server.collector.MetricDataPoint;
import io.mishmash.opentelemetry.server.collector.MetricsSubscriber;
import io.mishmash.opentelemetry.persistence.proto.v1.MetricsPersistenceProto.PersistedMetric;
import io.mishmash.opentelemetry.persistence.protobuf.ProtobufMetrics;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

/**
 * Subscribes to incoming OpenTelemetry metrics and writes them to parquet
 * files using {@link ParquetPersistence}.
 */
public class FileMetrics implements MetricsSubscriber {

    /**
     * The {@link java.util.logging.Logger} instance used.
     */
    private static final Logger LOG =
            Logger.getLogger(FileMetrics.class.getName());

    /**
     * The {@link java.util.concurrent.Flow.Subscription} that
     * supplies incoming records.
     */
    private Subscription subscription;
    /**
     * The {@link ParquetPersistence} instance that writes
     * incoming records to files.
     */
    private ParquetPersistence<PersistedMetric> parquet;
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
     * Creates a new OpenTelemetry metrics subscriber to
     * save metrics into parquet files.
     *
     * @param baseFileName the file name prefix (including any path)
     * @param instrumentation helper instance for own telemetry
     * @throws IOException if files cannot be opened for writing
     */
    public FileMetrics(
                final String baseFileName,
                final Instrumentation instrumentation)
            throws IOException {
        parquet = new ParquetPersistence<>(
                baseFileName,
                PersistedMetric.class);

        this.otel = instrumentation;

        numWritten = otel.newLongCounter(
            "parquet_metrics_written",
            "1",
            """
                Number of metric data point entries successfully written \
                to output file""");

        numFailed = otel.newLongCounter(
            "parquet_metrics_failed",
            "1",
            """
                Number of metric data point entries that could not be written \
                due to an error""");

        numCompletedFiles = otel.newLongGauge(
            "parquet_metrics_completed_files",
            "1",
            "Number of closed and completed metric data point files",
            g -> g.record(
                    parquet == null
                        ? 0
                        : parquet.getNumCompletedFiles()));

        numRecordsInCurrentFile = otel.newLongGauge(
            "parquet_metrics_current_file_written",
            "1",
            "Number of metric data point records written in current file",
            g -> g.record(
                    parquet == null
                        ? 0
                        : parquet.getCurrentNumCompletedRecords()));

        currentFileSize = otel.newLongGauge(
            "parquet_metrics_current_file_size",
            "By",
            "Size (in bytes) of the current metrics file",
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
                    "Failed to close metrics parquet file writer",
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
            "Closing metrics parquet file writer because of a publisher error",
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
    public void onNext(final MetricDataPoint metric) {

        try (Scope s = metric.getOtelContext().makeCurrent()) {
            Span span = otel.startNewSpan("otel.file.metrics");

            try {
                span.addEvent("Build persisted metric");
                span.addEvent("Write persisted metric to file");
                parquet.write(ProtobufMetrics
                        .buildMetricDataPoint(metric)
                        .build());

                numWritten.add(1);

                metric.complete(this);
            } catch (Exception e) {
                numFailed.add(1);

                metric.completeExceptionally(e);
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
