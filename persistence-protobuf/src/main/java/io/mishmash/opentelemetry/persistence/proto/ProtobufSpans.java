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

package io.mishmash.opentelemetry.persistence.proto;

import io.mishmash.opentelemetry.persistence.proto.v1.TracesPersistenceProto.PersistedSpan;
import io.mishmash.opentelemetry.server.collector.Span;

/**
 * Utility class to help with protobuf serialization of {@link Span}
 * instances.
 */
public final class ProtobufSpans {

    private ProtobufSpans() {
        // constructor is hidden
    }

    /**
     * Get a protobuf representation of a {@link Span}.
     *
     * @param span the span signal
     * @return a populated {@link PersistedSpan.Builder}
     */
    public static PersistedSpan.Builder buildSpan(final Span span) {
        PersistedSpan.Builder builder = PersistedSpan.newBuilder()
                .setBatchTimestamp(span.getBatchTimestamp())
                .setBatchUUID(span.getBatchUUID())
                .setSeqNo(span.getSeqNo())
                .setIsValid(span.isValid());

        if (span.getErrorMessage() != null) {
            builder = builder.setErrorMessage(span.getErrorMessage());
        }

        if (span.getResource() != null) {
            builder = builder
                    .addAllResourceAttributes(
                            span.getResource().getAttributesList())
                    .setResourceDroppedAttributesCount(
                            span.getResource()
                                .getDroppedAttributesCount());
        }

        if (span.getResourceSchemaUrl() != null) {
            builder = builder
                    .setResourceSchemaUrl(span.getResourceSchemaUrl());
        }

        if (span.getScope() != null) {
            builder = builder
                    .setScopeName(span.getScope().getName())
                    .setScopeVersion(span.getScope().getVersion())
                    .addAllScopeAttributes(
                            span.getScope().getAttributesList())
                    .setScopeDroppedAttributesCount(
                            span.getScope()
                                .getDroppedAttributesCount());
        }

        if (span.getSpan() != null) {
            builder = builder
                    .setTraceId(span.getSpan().getTraceId())
                    .setSpanId(span.getSpan().getSpanId())
                    .setTraceState(span.getSpan().getTraceState())
                    .setParentSpanId(span.getSpan().getParentSpanId())
                    .setFlags(span.getSpan().getFlags())
                    .setName(span.getSpan().getName())
                    .setKind(span.getSpan().getKind())
                    .setStartTimeUnixNano(
                            span.getSpan().getStartTimeUnixNano())
                    .setEndTimeUnixNano(
                            span.getSpan().getEndTimeUnixNano())
                    .addAllAttributes(
                            span.getSpan().getAttributesList())
                    .setDroppedAttributesCount(
                            span.getSpan().getDroppedAttributesCount())
                    .addAllEvents(span.getSpan().getEventsList())
                    .setDroppedEventsCount(
                            span.getSpan().getDroppedEventsCount())
                    .addAllLinks(span.getSpan().getLinksList())
                    .setDroppedLinksCount(
                            span.getSpan().getDroppedLinksCount())
                    .setStatus(span.getSpan().getStatus());
        }

        if (span.getSpanSchemaUrl() != null) {
            builder = builder
                    .setSpanSchemaUrl(span.getSpanSchemaUrl());
        }

        return builder;
    }
}
