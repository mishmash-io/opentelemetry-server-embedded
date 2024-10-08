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

import io.mishmash.opentelemetry.persistence.proto.ProtobufProfiles;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.PersistedProfile;
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
     * The source to read data from.
     */
    private InputEntity source;
    /**
     * True if the 'raw' format was configured.
     */
    private boolean isRaw = false;

    /**
     * Create an OTLP profiles reader.
     *
     * @param input the {@link InputEntity} containing protobuf-encoded bytes
     * @param isRawFormat true if input contains a 'raw'
     * {@link ExportProfilesServiceRequest}
     */
    public ProfilesReader(
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
            final PersistedProfile intermediateRow)
                    throws IOException, ParseException {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Map<String, Object>> toMap(
            final PersistedProfile intermediateRow) throws IOException {
        return Collections.singletonList(
                ProtobufProfiles.toJsonMap(intermediateRow));
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
}
