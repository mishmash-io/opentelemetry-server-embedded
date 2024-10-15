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

import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.PersistedProfile;
import io.mishmash.opentelemetry.persistence.protobuf.ProtobufProfiles;
import io.mishmash.opentelemetry.server.collector.ProfileSampleValue;
import io.mishmash.opentelemetry.server.collector.ProfilesFlattener;
import io.opentelemetry.proto.collector.profiles.v1experimental.ExportProfilesServiceRequest;

/**
 * An {@link IntermediateRowParsingReader} for OpenTelemetry profiles signals.
 *
 * Takes an {@link InputEntity} containing a 'raw' protobuf OTLP
 * {@link ExportProfilesServiceRequest} and 'flattens' it using
 * {@link ProfilesFlattener}, or takes a single (already flattened)
 * {@link PersistedProfile} protobuf message.
 */
public class ProfilesReader
        extends IntermediateRowParsingReader<PersistedProfile> {

    /**
     * The list of default candidates for metrics columns.
     */
    private static final String[] DEFAULT_METRIC_NAMES = new String[] {
            "duration_nanos",
            "period",
            "value"
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
     * Keeps a reference to all possible dimensions.
     */
    private Set<String> profilesDimensions;
    /**
     * Keeps a reference to all possible metrics.
     */
    private Set<String> profilesMetrics;

    /**
     * Create an OTLP profiles reader.
     *
     * @param rowSchema the schema as set in ingestion config
     * @param input the {@link InputEntity} containing protobuf-encoded bytes
     * @param isRawFormat true if input contains a 'raw'
     * {@link ExportProfilesServiceRequest}
     */
    public ProfilesReader(
            final InputRowSchema rowSchema,
            final InputEntity input,
            final boolean isRawFormat) {
        this.schema = rowSchema;
        this.source = input;
        this.isRaw = isRawFormat;

        profilesMetrics = new HashSet<>();

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
                profilesMetrics.add(metricName);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<InputRow> parseInputRows(
            final PersistedProfile intermediateRow)
                    throws IOException, ParseException {
        return Collections.singletonList(
                MapInputRowParser.parse(
                        schema.getTimestampSpec(),
                        MapInputRowParser.findDimensions(
                                schema.getTimestampSpec(),
                                schema.getDimensionsSpec(),
                                allDimensions()),
                        ProtobufProfiles.toJsonMap(intermediateRow, false)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Map<String, Object>> toMap(
            final PersistedProfile intermediateRow) throws IOException {
        /*
         * This is called when Druid is sampling data, so, provide defaults
         * for missing fields.
         */
        return Collections.singletonList(
                ProtobufProfiles.toJsonMap(intermediateRow, true));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CloseableIteratorWithMetadata<PersistedProfile>
            intermediateRowIteratorWithMetadata() throws IOException {
        try (InputStream is = source().open()) {
            if (isRaw) {
                ExportProfilesServiceRequest req =
                        ExportProfilesServiceRequest.parseFrom(is);

                return new RawIterator<>(
                        new ProfilesFlattener(null, null, req, null)) {
                    @Override
                    public PersistedProfile convert(
                            final ProfileSampleValue element) {
                        return ProtobufProfiles
                                .buildProfileSampleValue(element).build();
                    }

                    @Override
                    public void initMetaFor(
                            final PersistedProfile element) {
                        initIteratorMeta(this, element);
                    }
                };
            } else {
                PersistedProfile p = PersistedProfile.parseFrom(is);

                return new FlatIterator<>(p) {
                    @Override
                    public void initMetaFor(
                            final PersistedProfile element) {
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
    protected String intermediateRowAsString(final PersistedProfile row) {
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
     * Initialize the metadata associated with the given profile.
     *
     * A {@link CloseableIteratorWithMetadata} has to provide metadata
     * for each element it returns through its next() method. This
     * metadata is computed here, before next() returns.
     *
     * @param it - the iterator requesting metadata
     * @param profile - the new element to be returned by next()
     */
    protected void initIteratorMeta(
            final BaseIterator<PersistedProfile> it,
            final PersistedProfile profile) {
        it.clearMeta();

        it.setMeta("batchUUID", profile.getBatchUUID());
        it.setMeta("batchTimestamp", profile.getBatchTimestamp());
        it.setMeta("resourceSeqNo", profile.getResourceSeqNo());
        it.setMeta("scopeSeqNo", profile.getScopeSeqNo());
        it.setMeta("profileSeqNo", profile.getProfileSeqNo());
        it.setMeta("sampleSeqNo", profile.getSampleSeqNo());
        it.setMeta("valueSeqNo", profile.getValueSeqNo());
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
        if (profilesDimensions == null) {
            profilesDimensions = new HashSet<>();

            for (FieldDescriptor field : PersistedProfile
                                            .getDescriptor()
                                            .getFields()) {
                if (!profilesMetrics.contains(field.getName())) {
                    profilesDimensions.add(field.getName());
                }
            }
        }

        return profilesDimensions;
    }
}
