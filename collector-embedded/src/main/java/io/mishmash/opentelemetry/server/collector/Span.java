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
import io.opentelemetry.proto.common.v1.EntityRef;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.vertx.ext.auth.User;

/**
 * Holds the details of an individual OTLP span record
 * received from a client as a {@link Batch} of all subscribers
 * that are expected to process it.
 */
public class Span extends SubscribersBatch<Span> implements SignalResource {

    /**
     * Timestamp when the client's OTLP packet was received (in ms).
     */
    private long batchTimestamp;
    /**
     * A unique id of the client's OTLP packet.
     */
    private String batchUUID;
    /**
     * The sequence number of this Span within the client's
     * OTLP packet (zero-based).
     */
    private int seqNo;
    /**
     * The OTLP Resource details.
     */
    private Resource resource;
    /**
     * The Schema URL of the OTLP Resource.
     */
    private String resourceSchemaUrl;
    /**
     * The OTLP Scope details.
     */
    private InstrumentationScope scope;
    /**
     * The OTLP Span.
     */
    private io.opentelemetry.proto.trace.v1.Span span;
    /**
     * The Schema URL of the OTLP Span.
     */
    private String spanSchemaUrl;

    /**
     * If this record is valid.
     */
    private boolean isValid;
    /**
     * An error message to be sent back if this packet has an error.
     */
    private String errorMessage;

    /**
     * Create a new empty span.
     *
     * @param parent parent {@link Batch}
     * @param otelContext {@link io.opentelemetry.context.Context} for own
     * telemetry
     * @param authUser the authenticated user or null if auth wasn't enabled
     */
    public Span(
            final Batch<Span> parent,
            final Context otelContext,
            final User authUser) {
        super(parent, otelContext, authUser);
    }

    /**
     * Set the details of this span. Also sets the validity of this record
     * and potentially an error message.
     *
     * @param batchTS the batch timestamp
     * @param batchID the batch id
     * @param sequenceNum the sequence number of this span
     * @param resourceSpan used to fill-in OpenTelemetry Resource details
     * @param scopeSpan used to fill-in OpenTelemetry Scope details
     * @param otelSpan the span details
     */
    public void setFrom(
            final long batchTS,
            final String batchID,
            final int sequenceNum,
            final ResourceSpans resourceSpan,
            final ScopeSpans scopeSpan,
            final io.opentelemetry.proto.trace.v1.Span otelSpan) {
        this.batchTimestamp = batchTS;
        this.batchUUID = batchID;
        this.seqNo = sequenceNum;
        this.resource = resourceSpan.getResource();
        this.resourceSchemaUrl = resourceSpan.getSchemaUrl();
        this.scope = scopeSpan.getScope();
        this.span = otelSpan;
        this.spanSchemaUrl = scopeSpan.getSchemaUrl();

        // FIXME: add checks for validity and set isValid, errorMessage
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
     * Get the sequence number of this span within the containing
     * OTLP packet.
     *
     * @return the sequence number, zero-based
     */
    public int getSeqNo() {
        return seqNo;
    }

    /**
     * Get the OpenTelemetry Resource attributes of this span.
     *
     * If configured, additional attributes may be added. See the
     * configuration options.
     *
     * @return the resource attributes
     */
    @Override
    public Iterable<KeyValue> getResourceAttributes() {
        return computeResourceAttributes(resource.getAttributesList());
    }

    /**
     * Get the count of dropped OpenTelemetry Resource attributes.
     *
     * @return the number of attributes dropped
     */
    @Override
    public int getResourceDroppedAttributesCount() {
        return resource.getDroppedAttributesCount();
    }

    /**
     * Get the OpenTelemetry Resource Entities of this span.
     *
     * @return the resource entities.
     */
    @Override
    public Iterable<EntityRef> getResourceEntityRefs() {
        return resource.getEntityRefsList();
    }

    /**
     * Get the schema URL of the OpenTelemetry Resouce of this span.
     *
     * @return the schema URL
     */
    @Override
    public String getResourceSchemaUrl() {
        return resourceSchemaUrl;
    }

    /**
     * Get the OpenTelemetry Scope of this span.
     *
     * @return the scope details
     */
    public InstrumentationScope getScope() {
        return scope;
    }

    /**
     * Get the OpenTelemetry Span.
     *
     * @return the span details
     */
    public io.opentelemetry.proto.trace.v1.Span getSpan() {
        return span;
    }

    /**
     * Get the schema URL of the OpenTelemetry Span.
     *
     * @return the schema URL
     */
    public String getSpanSchemaUrl() {
        return spanSchemaUrl;
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
