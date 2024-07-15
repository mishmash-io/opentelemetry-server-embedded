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

import io.opentelemetry.context.Context;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.profiles.v1experimental.Profile;
import io.opentelemetry.proto.profiles.v1experimental.ProfileContainer;
import io.opentelemetry.proto.profiles.v1experimental.ResourceProfiles;
import io.opentelemetry.proto.profiles.v1experimental.Sample;
import io.opentelemetry.proto.profiles.v1experimental.ScopeProfiles;
import io.opentelemetry.proto.resource.v1.Resource;

/**
 * Holds an individual OTLP Profile Sample value as a {@link Batch}
 * of all subscribers that are expected to process it.
 */
public class ProfileSampleValue
        extends SubscribersBatch<ProfileSampleValue> {

    /**
     * Timestamp when the client's OTLP packet was received (in ms).
     */
    private long batchTimestamp;
    /**
     * A unique id of the client's OTLP packet.
     */
    private String batchUUID;
    /**
     * The sequence number of this ResourceProfile within the client's
     * OTLP packet (zero-based).
     */
    private int resourceProfileSeqNo;
    /**
     * The OTLP Resource details.
     */
    private Resource resource;
    /**
     * The Schema URL of the OTLP Resource.
     */
    private String resourceSchemaUrl;
    /**
     * The sequence number of this ScopeProfile within the current
     * Resource Profile (zero-based).
     */
    private int scopeProfileSeqNo;
    /**
     * The OTLP Scope details.
     */
    private InstrumentationScope scope;
    /**
     * The Schema URL of the OTLP Profile.
     */
    private String profileSchemaUrl;
    /**
     * The sequence number of this Profile within the current
     * Scope Profile (zero-based).
     */
    private int profileSeqNo;
    /**
     * The OTLP ProfileContainer.
     */
    private ProfileContainer profileContainer;
    /**
     * The OTLP Profile.
     */
    private Profile profile;

    /**
     * The sequence number of this Sample within the current
     * Profile (zero-based).
     */
    private int sampleSeqNo;
    /**
     * The OTLP Sample.
     */
    private Sample sample;
    /**
     * The sequence number of this value within the current
     * Sample Location (zero-based).
     */
    private int valueSeqNo;
    /**
     * The value for the given sample type and location.
     */
    private long value;

    /**
     * If this record is valid.
     */
    private boolean isValid;
    /**
     * An error message to be sent back if this packet has an error.
     */
    private String errorMessage;

    /**
     * Creates a new empty profile record.
     *
     * @param parent the parent {@link Batch}
     * @param otelContext {@link io.opentelemetry.context.Context} for own
     * telemetry
     */
    public ProfileSampleValue(
            final Batch<ProfileSampleValue> parent,
            final Context otelContext) {
        super(parent, otelContext);
    }

    /**
     * Sets the details of this profiling record.
     *
     * Also sets the isValid flag and potentially an error message.
     *
     * @param batchTS the batch timestamp
     * @param batchID the batch id
     * @param resourceProfileSequenceNum the sequence number of the resource
     * profile in the received OTLP packet
     * @param resourceProfile the resource profile
     * @param scopeProfileSequenceNum the sequence number of the scope profile
     * in the received OTLP packet
     * @param scopeProfile the scope profile
     * @param profileSequenceNum the sequence number of the profile as
     * received from the client
     * @param container an OTLP profile container
     * @param otelProfile the OTLP profile
     * @param sampleSequenceNum the sequence number of a sample within
     * the profile
     * @param valueSequenceNum the sequence number of an individual value
     */
    public void setFrom(
            final long batchTS,
            final String batchID,
            final int resourceProfileSequenceNum,
            final ResourceProfiles resourceProfile,
            final int scopeProfileSequenceNum,
            final ScopeProfiles scopeProfile,
            final int profileSequenceNum,
            final ProfileContainer container,
            final Profile otelProfile,
            final int sampleSequenceNum,
            final int valueSequenceNum
            ) {
        this.batchTimestamp = batchTS;
        this.batchUUID = batchID;
        this.resourceProfileSeqNo = resourceProfileSequenceNum;
        this.resource = resourceProfile.getResource();
        this.resourceSchemaUrl = resourceProfile.getSchemaUrl();
        this.scopeProfileSeqNo = scopeProfileSequenceNum;
        this.scope = scopeProfile.getScope();
        this.profileSchemaUrl = scopeProfile.getSchemaUrl();
        this.profileSeqNo = profileSequenceNum;
        this.profileContainer = container;
        this.profile = otelProfile;
        this.sampleSeqNo = sampleSequenceNum;
        this.sample = profile.getSample(sampleSequenceNum);
        this.valueSeqNo = valueSequenceNum;
        this.value = this.sample.getValue(valueSequenceNum);

        // FIXME: add checks for validity and set isValid, errorMessage?
        isValid = true;
    }

    /**
     * Get the timestamp when the containing OTLP packet was received.
     *
     * @return the timestamp, in ms
     */
    public long getBatchTimestamp() {
        return batchTimestamp;
    }

    /**
     * Get the unique id of the containing OTLP packet.
     *
     * @return the UUID
     */
    public String getBatchUUID() {
        return batchUUID;
    }

    /**
     * Get the sequence number of this resource within the containing
     * OTLP packet.
     *
     * @return the sequence number, zero-based
     */
    public int getResourceProfileSeqNo() {
        return resourceProfileSeqNo;
    }

    /**
     * Get the sequence number of this scope within the containing
     * resource.
     *
     * @return the sequence number, zero-based
     */
    public int getScopeProfileSeqNo() {
        return scopeProfileSeqNo;
    }

    /**
     * Get the sequence number of this profile within the containing
     * scope.
     *
     * @return the sequence number, zero-based
     */
    public int getProfileSeqNo() {
        return profileSeqNo;
    }

    /**
     * Get the sequence number of this sample within the containing
     * profile.
     *
     * @return the sequence number, zero-based
     */
    public int getSampleSeqNo() {
        return sampleSeqNo;
    }

    /**
     * Get the sequence number of this value within the containing
     * sample.
     *
     * @return the sequence number, zero-based
     */
    public int getValueSeqNo() {
        return valueSeqNo;
    }

    /**
     * Get the OpenTelemetry Resource of this profile.
     *
     * @return the resource details
     */
    public Resource getResource() {
        return resource;
    }

    /**
     * Get the schema URL of the OpenTelemetry Resouce of this profile.
     *
     * @return the schema URL
     */
    public String getResourceSchemaUrl() {
        return resourceSchemaUrl;
    }

    /**
     * Get the OpenTelemetry Scope of this profile.
     *
     * @return the scope details
     */
    public InstrumentationScope getScope() {
        return scope;
    }

    /**
     * Get the profile schema url.
     *
     * @return the profile schema url
     */
    public String getProfileSchemaUrl() {
        return profileSchemaUrl;
    }

    /**
     * Get the OTLP Profile Container.
     *
     * @return the profile container
     */
    public ProfileContainer getProfileContainer() {
        return profileContainer;
    }

    /**
     * Get the OTLP Profile.
     *
     * @return the profile
     */
    public Profile getProfile() {
        return profile;
    }

    /**
     * Get the OTLP Sample.
     *
     * @return the sample
     */
    public Sample getSample() {
        return sample;
    }

    /**
     * Get the value of this record.
     *
     * @return the value
     */
    public long getValue() {
        return value;
    }

    /**
     * Check if this record is valid.
     *
     * @return true if data is valid
     */
    public boolean isValid() {
        return isValid;
    }

    /**
     * Get the message associated with an error in the data.
     *
     * @return the error message or null if data is valid
     */
    public String getErrorMessage() {
        return errorMessage;
    }
}
