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

package io.mishmash.opentelemetry.persistence.protobuf;

import java.util.Map;

import io.mishmash.opentelemetry.persistence.proto.v1.LogsPersistenceProto.PersistedLog;
import io.mishmash.opentelemetry.server.collector.Log;
import io.opentelemetry.proto.common.v1.AnyValue;

/**
 * Utility class to help with protobuf serialization of {@link Log} instances.
 */
public final class ProtobufLogs {

    private ProtobufLogs() {
        // constructor is hidden
    }

    /**
     * Get a protobuf representation of a {@link Log}.
     *
     * @param log the log signal
     * @return a populated {@link PersistedLog.Builder}
     */
    public static PersistedLog.Builder buildLog(final Log log) {
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

        return builder;
    }

    /**
     * Convert a {@link PersistedLog} to a {@link Map} suitable for JSON
     * encoding.
     *
     * @param log the persisted log protobuf message
     * @return the {@link Map}
     */
    public static Map<String, Object> toJsonMap(final PersistedLog log) {
        return ProtobufUtils.toJsonMap(log.getAllFields());
    }
}
