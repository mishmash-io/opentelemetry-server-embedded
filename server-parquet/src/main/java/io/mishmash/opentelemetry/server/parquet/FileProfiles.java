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
import io.mishmash.opentelemetry.server.collector.ProfileSampleValue;
import io.mishmash.opentelemetry.server.collector.ProfilesSubscriber;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.PersistedProfile;
import io.mishmash.opentelemetry.persistence.protobuf.ProtobufProfiles;

/**
 * Subscribes to incoming OpenTelemetry profiles and writes them to parquet
 * files using {@link ParquetPersistence}.
 */
public class FileProfiles implements ProfilesSubscriber {
    /**
     * The {@link java.util.logging.Logger} instance used.
     */
    private static final Logger LOG =
            Logger.getLogger(FileProfiles.class.getName());

    /**
     * The {@link java.util.concurrent.Flow.Subscription} that
     * supplies incoming records.
     */
    private Subscription subscription;
    /**
     * The {@link ParquetPersistence} instance that writes
     * incoming records to files.
     */
    private ParquetPersistence<PersistedProfile> parquet;
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
     * Creates a new OpenTelemetry profiles subscriber to
     * save profiles into parquet files.
     *
     * @param baseFileName the file name prefix (including any path)
     * @param instrumentation helper instance for own telemetry
     * @throws IOException if files cannot be opened for writing
     */
    public FileProfiles(
                final String baseFileName,
                final Instrumentation instrumentation)
            throws IOException {
        parquet = new ParquetPersistence<>(
                baseFileName,
                PersistedProfile.class);

        this.otel = instrumentation;

        numWritten = otel.newLongCounter(
            "parquet_profiles_written",
            "1",
            """
            Number of profile entries successfully written \
            to output file""");

        numFailed = otel.newLongCounter(
            "parquet_profiles_failed",
            "1",
            """
            Number of profile entries that could not be \
            written due to an error""");

        numCompletedFiles = otel.newLongGauge(
            "parquet_profiles_completed_files",
            "1",
            "Number of closed and completed profile files",
            g -> g.record(
                    parquet == null
                        ? 0
                        : parquet.getNumCompletedFiles()));

        numRecordsInCurrentFile = otel.newLongGauge(
            "parquet_profiles_current_file_written",
            "1",
            "Number of profile records written in current file",
            g -> g.record(
                    parquet == null
                        ? 0
                        : parquet.getCurrentNumCompletedRecords()));

        currentFileSize = otel.newLongGauge(
            "parquet_profiles_current_file_size",
            "By",
            "Size (in bytes) of the current profiles file",
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
                    "Failed to close profiles parquet file writer",
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
            """
            Closing profiles parquet file writer because \
            of a publisher error""",
            error);

        try {
            if (parquet != null) {
                parquet.close();
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING,
                    "Failed to close profiles parquet file writer",
                    e);
        } finally {
            parquet = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onNext(final ProfileSampleValue profile) {
        try (Scope scope = profile.getOtelContext().makeCurrent()) {
            Span span = otel.startNewSpan("otel.file.profiles");

            try {
                span.addEvent("Build persisted profile");

                span.addEvent("Write persisted profile to file");
                parquet.write(ProtobufProfiles
                        .buildProfileSampleValue(profile)
                        .build());

                numWritten.add(1);

                profile.complete(this);
            } catch (Exception e) {
                numFailed.add(1);

                profile.completeExceptionally(e);
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
