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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.profiles.v1experimental.AttributeUnit;
import io.opentelemetry.proto.profiles.v1experimental.Function;
import io.opentelemetry.proto.profiles.v1experimental.Label;
import io.opentelemetry.proto.profiles.v1experimental.Line;
import io.opentelemetry.proto.profiles.v1experimental.Link;
import io.opentelemetry.proto.profiles.v1experimental.Location;
import io.opentelemetry.proto.profiles.v1experimental.Mapping;
import io.opentelemetry.proto.profiles.v1experimental.Profile;
import io.opentelemetry.proto.profiles.v1experimental.ProfileContainer;
import io.opentelemetry.proto.profiles.v1experimental.Sample;
import io.opentelemetry.proto.profiles.v1experimental.ValueType;
import io.mishmash.opentelemetry.server.parquet.persistence.proto.v1.ProfilesPersistenceProto.KeyValueUnit;
import io.mishmash.opentelemetry.server.parquet.persistence.proto.v1.ProfilesPersistenceProto.PersistedProfile;
import io.mishmash.opentelemetry.server.parquet.persistence.proto.v1.ProfilesPersistenceProto.StrFunction;
import io.mishmash.opentelemetry.server.parquet.persistence.proto.v1.ProfilesPersistenceProto.StrLabel;
import io.mishmash.opentelemetry.server.parquet.persistence.proto.v1.ProfilesPersistenceProto.StrLine;
import io.mishmash.opentelemetry.server.parquet.persistence.proto.v1.ProfilesPersistenceProto.StrLocation;
import io.mishmash.opentelemetry.server.parquet.persistence.proto.v1.ProfilesPersistenceProto.StrMapping;
import io.mishmash.opentelemetry.server.parquet.persistence.proto.v1.ProfilesPersistenceProto.StrValueType;

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

                PersistedProfile.Builder builder =
                        PersistedProfile.newBuilder()
                            .setBatchTimestamp(profile.getBatchTimestamp())
                            .setBatchUUID(profile.getBatchUUID())
                            .setResourceSeqNo(
                                    profile.getResourceProfileSeqNo())
                            .setScopeSeqNo(profile.getScopeProfileSeqNo())
                            .setProfileSeqNo(profile.getProfileSeqNo())
                            .setSampleSeqNo(profile.getSampleSeqNo())
                            .setValueSeqNo(profile.getValueSeqNo())
                            .setIsValid(profile.isValid());

                if (profile.getErrorMessage() != null) {
                    builder = builder
                            .setErrorMessage(profile.getErrorMessage());
                }

                if (profile.getResource() != null) {
                    builder = builder
                            .addAllResourceAttributes(
                                    profile.getResource().getAttributesList())
                            .setResourceDroppedAttributesCount(
                                    profile.getResource()
                                        .getDroppedAttributesCount());
                }

                if (profile.getResourceSchemaUrl() != null) {
                    builder = builder
                            .setResourceSchemaUrl(
                                    profile.getResourceSchemaUrl());
                }

                if (profile.getScope() != null) {
                    builder = builder
                            .setScopeName(profile.getScope().getName())
                            .setScopeVersion(profile.getScope().getVersion())
                            .addAllScopeAttributes(
                                    profile.getScope().getAttributesList())
                            .setScopeDroppedAttributesCount(
                                    profile.getScope()
                                        .getDroppedAttributesCount());
                }

                if (profile.getProfileSchemaUrl() != null) {
                    builder = builder
                            .setProfileSchemaUrl(profile.getProfileSchemaUrl());
                }

                if (profile.getProfileContainer() != null) {
                    ProfileContainer container = profile.getProfileContainer();

                    builder = builder
                            .setProfileId(container.getProfileId())
                            .setStartTimeUnixNano(
                                    container.getStartTimeUnixNano())
                            .setEndTimeUnixNano(container.getEndTimeUnixNano())
                            .addAllProfileAttributes(
                                    container.getAttributesList())
                            .setProfileDroppedAttributesCount(
                                    container.getDroppedAttributesCount())
                            .setOriginalPayloadFormat(
                                    container.getOriginalPayloadFormat())
                            .setOriginalPayload(
                                    container.getOriginalPayload());
                }

                Profile p = profile.getProfile();
                if (profile.getProfile() != null) {
                    builder = builder
                            .setDropFrames(getStrAt(p, p.getDropFrames()))
                            .setKeepFrames(getStrAt(p, p.getKeepFrames()))
                            .setTimeNanos(p.getTimeNanos())
                            .setDurationNanos(p.getDurationNanos())
                            .setPeriodType(toStr(p, p.getPeriodType()))
                            .setPeriod(p.getPeriod())
                            .addAllComment(toStr(p, p.getCommentList()))
                            .setDefaultSampleType(
                                    getStrAt(p, p.getDefaultSampleType()));
                }

                if (profile.getSample() != null) {
                    Sample s = profile.getSample();

                    builder = builder
                            .setStacktraceId(
                                    getStrAt(p, s.getStacktraceIdIndex()))
                            .addAllLocations(resolveLocations(p, s))
                            .addAllLabels(resolveLabels(p, s.getLabelList()))
                            .addAllAttributes(
                                    resolveAttributes(
                                            p,
                                            s.getAttributesList()))
                            .addAllTimestampsUnixNano(
                                    s.getTimestampsUnixNanoList())
                            .setType(
                                    toStr(
                                        p,
                                        p.getSampleType(
                                            profile.getValueSeqNo())));

                    Link l = p.getLinkTableCount() == 0
                            ? null
                            : p.getLinkTable((int) s.getLink());
                    if (l != null) {
                        builder = builder
                                .setTraceId(l.getTraceId())
                                .setSpanId(l.getSpanId());
                    }
                }

                builder = builder.setValue(profile.getValue());

                span.addEvent("Write persisted profile to file");
                parquet.write(builder.build());

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

    /**
     * Return the string contained in this profile at a given index.
     *
     * @param profile the OTLP profile
     * @param index the index
     * @return the string at that index
     */
    private String getStrAt(final Profile profile, final long index) {
        return profile.getStringTable((int) index);
    }

    /**
     * Convert a ValueType to StrValueType, resolving all strings.
     *
     * @param p an OTLP Profile
     * @param vt an OTLP ValueType
     * @return a converted value type
     */
    private StrValueType.Builder toStr(final Profile p, final ValueType vt) {
        return StrValueType.newBuilder()
                .setType(getStrAt(p, vt.getType()))
                .setUnit(getStrAt(p, vt.getUnit()))
                .setAggregationTemporality(vt.getAggregationTemporality());
    }

    /**
     * Resolve an OTLP Mapping.
     *
     * @param p the OTLP Profile, used for resolution
     * @param mapping the OTLP Mapping
     * @return a resolved mapping
     */
    private StrMapping.Builder resolve(
            final Profile p,
            final Mapping mapping) {
        return StrMapping.newBuilder()
                .setMemoryStart(mapping.getMemoryStart())
                .setMemoryLimit(mapping.getMemoryLimit())
                .setFileOffset(mapping.getFileOffset())
                .setFilename(getStrAt(p, mapping.getFilename()))
                .setBuildId(getStrAt(p, mapping.getBuildId()))
                .setBuildIdKind(mapping.getBuildIdKind())
                .addAllAttributes(
                        resolveAttributes(p, mapping.getAttributesList()))
                .setHasFunctions(mapping.getHasFunctions())
                .setHasFilenames(mapping.getHasFilenames())
                .setHasLineNumbers(mapping.getHasLineNumbers())
                .setHasInlineFrames(mapping.getHasInlineFrames());
    }

    /**
     * Resolve an OTLP Function.
     *
     * @param p the OTLP Profile, used for resolution
     * @param f the OTLP Function
     * @return a function with resolved strings and values
     */
    private StrFunction.Builder resolve(
            final Profile p,
            final Function f) {
        return StrFunction.newBuilder()
                .setName(getStrAt(p, f.getName()))
                .setSystemName(getStrAt(p, f.getSystemName()))
                .setFilename(getStrAt(p, f.getFilename()))
                .setStartLine(f.getStartLine());
    }

    /**
     * Resolve an OTLP Line.
     *
     * @param p the OTLP Profile, used for resolution
     * @param l the OTLP Line
     * @return a line with resolved strings and values
     */
    private StrLine.Builder resolve(
            final Profile p,
            final Line l) {
        return StrLine.newBuilder()
                .setFunction(
                        resolve(
                            p,
                            p.getFunction((int) l.getFunctionIndex())))
                .setLine(l.getLine())
                .setColumn(l.getColumn());
    }

    /**
     * Convert an OTLP Location by resolving all indexes.
     *
     * @param p the OTLP Profile, used for resolution
     * @param location the OTLP Location
     * @return a resolved location
     */
    private StrLocation.Builder resolve(
            final Profile p,
            final Location location) {
        return StrLocation.newBuilder()
                .setMapping(
                        resolve(
                            p,
                            p.getMapping((int) location.getMappingIndex())))
                .setAddress(location.getAddress())
                .addAllLines(resolveLines(p, location.getLineList()))
                .setIsFolded(location.getIsFolded())
                .setType(getStrAt(p, location.getTypeIndex()))
                .addAllAttributes(
                        resolveAttributes(p, location.getAttributesList()));
    }

    /**
     * Resolve an OTLP Label.
     *
     * @param p the OTLP Profile used for resolution
     * @param label the OTLP Label
     * @return a resolved label
     */
    private StrLabel.Builder resolve(
            final Profile p,
            final Label label) {
        StrLabel.Builder builder = StrLabel.newBuilder()
                .setKey(getStrAt(p, label.getKey()));

        if (label.getNum() > 0) {
            builder = builder
                    .setNum(label.getNum())
                    .setNumUnit(getStrAt(p, label.getNumUnit()));
        } else {
            builder = builder
                    .setStr(getStrAt(p, label.getStr()));
        }

        return builder;
    }

    /**
     * Resolve an OTLP Sample attribute, adding an attribute unit if found.
     *
     * @param p the OTLP Profile used for lookup
     * @param attr the Sample attribute
     * @return an attribute with a unit (if present)
     */
    private KeyValueUnit.Builder resolve(
            final Profile p,
            final KeyValue attr) {
        String key = attr.getKey();
        KeyValueUnit.Builder builder = KeyValueUnit.newBuilder()
                .setKey(key)
                .setValue(attr.getValue());

        for (AttributeUnit u : p.getAttributeUnitsList()) {
            String unit = getStrAt(p, u.getAttributeKey());

            if (key.equals(unit)) {
                builder = builder
                        .setUnit(unit);

                break;
            }
        }

        return builder;
    }

    /**
     * Get all locations contained in an OTLP Sample and convert them.
     *
     * @param p the OTLP Profile, used for resolution
     * @param s the OTLP Sample
     * @return a list of all contained locations, resolved
     */
    private List<StrLocation> resolveLocations(
            final Profile p,
            final Sample s) {
        if (s.getLocationIndexCount() > 0) {
            return s.getLocationIndexList().stream()
                    .map(l -> p.getLocation(l.intValue()))
                    .map(l -> resolve(p, l))
                    .map(l -> l.build())
                    .toList();
        } else if (s.getLocationsLength() > 0) {
            ArrayList<StrLocation> res =
                    new ArrayList<>((int) s.getLocationsLength());

            int startIdx = (int) s.getLocationsStartIndex();
            for (int idx = startIdx;
                    idx < startIdx + (int) s.getLocationsLength();
                    idx++) {
                res.add(
                        resolve(p, p.getLocation(idx))
                            .build());
            }

            return res;
        } else {
            // couldn't extract locations?
            return Collections.emptyList();
        }
    }

    /**
     * Resolve a list of OTLP Labels.
     *
     * @param p the OTLP Profile to use for resolution
     * @param labels the list of OTLP Labels
     * @return a list of resolved labels
     */
    private List<StrLabel> resolveLabels(
            final Profile p,
            final List<Label> labels) {
        return labels.stream()
                .map(l -> resolve(p, l))
                .map(l -> l.build())
                .toList();
    }

    /**
     * Resolve a list of OTLP Sample attributes.
     *
     * @param p the OTLP Profile used for resolution
     * @param attributes the list of attribute indexes
     * @return a list of resolved attributes
     */
    private List<KeyValueUnit> resolveAttributes(
            final Profile p,
            final List<Long> attributes) {
        return attributes.stream()
                .map(i -> p.getAttributeTable(i.intValue()))
                .map(a -> resolve(p, a))
                .map(a -> a.build())
                .toList();
    }

    /**
     * Resolve a list of Sample Lines.
     *
     * @param p the OTLP Profile used for resolution
     * @param lines the OTLP Lines
     * @return a list of resolved lines
     */
    private List<StrLine> resolveLines(
            final Profile p,
            final List<Line> lines) {
        return lines.stream()
                .map(l -> resolve(p, l))
                .map(l -> l.build())
                .toList();
    }

    /**
     * Resolve a list of string indexes to the original strings.
     *
     * @param p an OTLP Profile
     * @param indexes the list of indexes
     * @return a list of resolved strings
     */
    private List<String> toStr(final Profile p, final List<Long> indexes) {
        return indexes.stream()
                .map(l -> getStrAt(p, l))
                .toList();
    }
}
