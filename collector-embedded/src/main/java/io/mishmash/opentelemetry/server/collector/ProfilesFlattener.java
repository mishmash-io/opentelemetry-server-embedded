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

package io.mishmash.opentelemetry.server.collector;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import io.opentelemetry.context.Context;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.profiles.v1development.Profile;
import io.opentelemetry.proto.profiles.v1development.ResourceProfiles;
import io.opentelemetry.proto.profiles.v1development.Sample;
import io.opentelemetry.proto.profiles.v1development.ScopeProfiles;
import io.vertx.ext.auth.User;

/**
 * Extracts individual profile sample value messages from an OTLP packet.
 *
 * Turns an {@link ExportProfilesServiceRequest} into
 * an {@link Iterable} of {@link ProfileSampleValue}s.
 *
 * The original OTLP protocol message format, as specified
 * by {@link ExportProfilesServiceRequest}, contains lists of
 * individual values (of type long). These lists are nested
 * into lists of {@link Sample}s, which are in turn
 * nested inside a list of {@link Profile}s, which are
 * again nested into a list of {@link ScopeProfiles}, further
 * nested into a list of {@link ResourceProfiles}.
 *
 * In addition, repetitive fields within the same message are
 * encoded with the help of lookup tables (or dictionaries) to reduce the
 * message size.
 *
 * To facilitate further processing this class extracts
 * (or 'flattens') the above nested structures into a 'flat'
 * {@link Iterable} of individual {@link ProfileSampleValue} instances.
 * It also performs the necessary lookup to extract the actual values
 * of dictionary-encoded fields.
 *
 * To see examples of 'flattened' data visit
 * @see <a href="https://github.com/mishmash-io/opentelemetry-server-embedded/blob/main/examples/notebooks/basics.ipynb">
 * OpenTelemetry Basics Notebook on GitHub.</a>
 */
public class ProfilesFlattener implements Iterable<ProfileSampleValue> {

    /**
     * The parent {@link Batch}.
     */
    private Batch<ProfileSampleValue> batch;
    /**
     * The own telemetry {@link Context}.
     */
    private Context otel;
    /**
     * The OTLP message received from the client.
     */
    private ExportProfilesServiceRequest request;
    /**
     * An optional {@link User} if authentication was enabled.
     */
    private User user;
    /**
     * The timestamp of this batch.
     */
    private long timestamp;
    /**
     * The batch UUID.
     */
    private String uuid;

    /**
     * Create a {@link ProfileSampleValue}s flattener.
     *
     * @param parentBatch the parent {@link Batch}
     * @param otelContext the own telemetry {@link Context}
     * @param profilesRequest the OTLP packet
     * @param authUser the {@link User} submitting the request or null
     * if authentication was not enabled
     */
    public ProfilesFlattener(
            final Batch<ProfileSampleValue> parentBatch,
            final Context otelContext,
            final ExportProfilesServiceRequest profilesRequest,
            final User authUser) {
        this.batch = parentBatch;
        this.otel = otelContext;
        this.request = profilesRequest;
        this.user = authUser;

        timestamp = System.currentTimeMillis();
        uuid = UUID.randomUUID().toString();
    }

    /**
     * Get the configured Profiles {@link Batch}, if any.
     *
     * @return the batch or null if not set
     */
    public Batch<ProfileSampleValue> getBatch() {
        return batch;
    }

    /**
     * Get the own telemetry context, if any.
     *
     * @return the {@link Context} or null if not set
     */
    public Context getOtel() {
        return otel;
    }

    /**
     * Get the parsed protobuf Profiles request.
     *
     * @return the {@link ExportProfilesServiceRequest} message
     */
    public ExportProfilesServiceRequest getRequest() {
        return request;
    }

    /**
     * Get the authenticated user who submitted this message.
     *
     * @return the {@link User} or null if authentication was not enabled
     */
    public User getUser() {
        return user;
    }

    /**
     * Get the timestamp used by this flattener.
     *
     * @return the timestamp in milliseconds
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Get the unique UUID used by this flattener.
     *
     * @return the UUID
     */
    public String getUuid() {
        return uuid;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<ProfileSampleValue> iterator() {
        return new ProfilesIterator();
    }

    /**
     * The internal {@link Iterator}, as returned to user.
     */
    private final class ProfilesIterator
            implements Iterator<ProfileSampleValue> {

        /**
         * Iterator over the OTLP Resource Profiles.
         */
        private Iterator<ResourceProfiles> resourceIt;
        /**
         * Iterator over the OTLP Scope Profiles within an OTLP
         * Resource Profile.
         */
        private Iterator<ScopeProfiles> scopeIt;
        /**
         * Iterator over the OTLP Profiles within an OTLP
         * Scope Profile.
         */
        private Iterator<Profile> profileIt;
        /**
         * Iterator over the OTLP Sample within an OTLP Profile.
         */
        private Iterator<Sample> sampleIt;
        /**
         * Iterator over the values of an OTLP Sample.
         */
        private Iterator<SampleValue> valueIt;
        /**
         * The current OTLP Resource Profile.
         */
        private ResourceProfiles currentResource;
        /**
         * The current OTLP Scope Profiles.
         */
        private ScopeProfiles currentScope;
        /**
         * The current OTLP Profile Container.
         */
        private Profile currentProfile;
        /**
         * The current OTLP Sample.
         */
        private Sample currentSample;
        /**
         * A counter of Resource Profiles.
         */
        private int resourcesCount;
        /**
         * A counter of Scope Profiles.
         */
        private int scopesCount;
        /**
         * A counter of Profiles.
         */
        private int profilesCount;
        /**
         * A counter of Samples.
         */
        private int samplesCount;
        /**
         * A counter of values.
         */
        private int valuesCount;
        /**
         * A counter of returned {@link ProfileSampleValue}s.
         */
        private int itemsCount;

        /**
         * Initialize this iterator.
         */
        private ProfilesIterator() {
            resourcesCount = -1;
            scopesCount = -1;
            profilesCount = -1;
            samplesCount = -1;
            valuesCount = -1;
            itemsCount = 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            if (valueIt == null || !valueIt.hasNext()) {
                nextSample();

                while (currentSample != null
                        && !(
                                valueIt = new SamplesIterator(currentSample)
                            ).hasNext()) {
                    nextSample();
                }
            }

            return valueIt != null && valueIt.hasNext();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ProfileSampleValue next() {
            SampleValue rec = valueIt.next();
            ProfileSampleValue psv =
                    new ProfileSampleValue(batch, otel, user);

            psv.setFrom(
                    timestamp,
                    uuid,
                    resourcesCount,
                    currentResource,
                    scopesCount,
                    currentScope,
                    profilesCount,
                    currentProfile,
                    samplesCount,
                    ++valuesCount,
                    request.getDictionary());

            itemsCount++;

            return psv;
        }

        /**
         * Move the nested iterators to next OTLP Sample.
         */
        private void nextSample() {
            if (sampleIt == null || !sampleIt.hasNext()) {
                nextProfile();

                while (currentProfile != null
                        && !(
                                sampleIt = currentProfile
                                        .getSamplesList()
                                        .iterator()
                            ).hasNext()) {
                    nextProfile();
                }
            }

            if (sampleIt == null || !sampleIt.hasNext()) {
                currentSample = null;
            } else {
                currentSample = sampleIt.next();
                samplesCount++;
                valuesCount = -1;
            }
        }

        /**
         * Move the nested iterators to next OTLP Profile Container.
         */
        private void nextProfile() {
            if (profileIt == null || !profileIt.hasNext()) {
                nextScope();

                while (currentScope != null
                        && !(
                                profileIt = currentScope
                                        .getProfilesList()
                                        .iterator()
                            ).hasNext()) {
                    nextScope();
                }
            }

            if (profileIt == null || !profileIt.hasNext()) {
                currentProfile = null;
            } else {
                currentProfile = profileIt.next();
                profilesCount++;
                samplesCount = -1;
                valuesCount = -1;
            }
        }

        /**
         * Move the nested iterators to next OTLP Scope Profiles.
         */
        private void nextScope() {
            if (scopeIt == null || !scopeIt.hasNext()) {
                nextResource();

                while (currentResource != null
                        && !(
                                scopeIt = currentResource
                                        .getScopeProfilesList()
                                        .iterator()
                            ).hasNext()) {
                    nextResource();
                }
            }

            if (scopeIt == null || !scopeIt.hasNext()) {
                currentScope = null;
            } else {
                currentScope = scopeIt.next();
                scopesCount++;
                profilesCount = -1;
                samplesCount = -1;
                valuesCount = -1;
            }
        }

        /**
         * Move the nested iterators to next OTLP Resource Profiles.
         */
        private void nextResource() {
            if (resourceIt == null) {
                resourceIt = request.getResourceProfilesList().iterator();
            }

            if (resourceIt.hasNext()) {
                currentResource = resourceIt.next();
                resourcesCount++;
                scopesCount = -1;
                profilesCount = -1;
                samplesCount = -1;
                valuesCount = -1;
            } else {
                currentResource = null;
            }
        }
    }

    /**
     * A value/timestamp pair (either is optional and can be null).
     *
     * @param timestamp An optional timestamp when the observation was made
     * @param value An optional value associated with this profiling sample
     */
    private record SampleValue(Long timestamp, Long value) { }

    /**
     * Helper iterator to simplify iterating over optional values or
     * timestamps.
     *
     * According to the OTLP protocol if both lists are preset they should
     * be of equal size.
     */
    private final class SamplesIterator implements Iterator<SampleValue> {

        /**
         * The list of values to iterate over (or empty).
         */
        private List<Long> values;
        /**
         * The list of timestamps to iterate over (or empty).
         */
        private List<Long> timestamps;
        /**
         * Current index in the lists.
         */
        private int currentIdx;

        private SamplesIterator(final Sample profileSample) {
            this.values = profileSample.getValuesList();
            this.timestamps = profileSample.getTimestampsUnixNanoList();
            this.currentIdx = 0;

            if (!values.isEmpty() && !timestamps.isEmpty()) {
                if (values.size() != timestamps.size()) {
                    throw new IndexOutOfBoundsException(
                            """
                            OTLP Profiles Samples must contain an equal \
                            number of values and timestamps""");
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return currentIdx < values.size()
                    || currentIdx < timestamps.size();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SampleValue next() {
            SampleValue r = new SampleValue(
                    timestamps.isEmpty() ? null : timestamps.get(currentIdx),
                    values.isEmpty() ? null : values.get(currentIdx));

            currentIdx++;

            return r;
        }
    }
}
