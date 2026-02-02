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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import io.mishmash.opentelemetry.persistence.proto.v1.LogsPersistenceProto.PersistedLog;
import io.mishmash.opentelemetry.server.collector.Log;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.logs.v1.SeverityNumber;

public class ProtobufLogsTests extends Base {

    private static final String LOG_SCHEMA_URL = "http://log.schema.test";
    private static final int LOG_SEQ_NO = 2;
    private static final long LOG_TIMESTAMP = 5;
    private static final long LOG_OBSERVED_TIMESTAMP = 6;
    private static final int LOG_SEVERITY_NUM = 7;
    private static final String LOG_SEVERITY = "SEVERITY_DEBUG3";
    private static final String LOG_BODY = "test log message";
    private static final String LOG_ATTR_KEY = "log-attr";
    private static final Boolean LOG_ATTR_VAL = Boolean.FALSE;
    private static final Integer LOG_DROPPED_ATTRS_COUNT = 0;
    private static final int LOG_FLAGS = 0;
    private static final byte[] LOG_TRACE_ID = new byte[] {0x11};
    private static final byte[] LOG_SPAN_ID = new byte[] {0x12};

    @Test
    void buildPersistedLog() {
        Log l = defaultLog();
        PersistedLog pl = ProtobufLogs.buildLog(l).build();

        testBatchTimestamp(pl.getBatchTimestamp());
        testBatchUUID(pl.getBatchUUID());
        assertEquals(LOG_SEQ_NO, pl.getSeqNo());
        testResourceSchemaUrl(l.getResourceSchemaUrl());
        testResourceAttributes(l.getResourceAttributes());
        // TODO: test resource entities
        assertEquals(LOG_SCHEMA_URL, l.getLogSchemaUrl());
        testScope(l.getScope());
        testLog(l.getLog());
        assertEquals(l.isValid(), pl.getIsValid());
        assertEquals(l.getErrorMessage() != null, pl.hasErrorMessage());
        assertEquals(
                l.getErrorMessage() == null
                    ? ""
                    : l.getErrorMessage(),
                pl.getErrorMessage());
    }

    @Test
    void toJsonMap() {
        Log l = defaultLog();
        PersistedLog pl = ProtobufLogs.buildLog(l).build();

        Map<String, Object> res = ProtobufLogs.toJsonMap(pl, false);

        testBatchTimestamp((Long) res.get("batch_timestamp"));
        testBatchUUID((String) res.get("batch_UUID"));
        assertEquals(LOG_SEQ_NO, res.get("seq_no"));
        testResourceSchemaUrl((String) res.get("resource_schema_url"));
        @SuppressWarnings("unchecked")
        Map<String, Object> resAttrs =
                (Map<String, Object>) res.get("resource_attributes");
        testResource(
                resource(
                        (int) res.get("resource_dropped_attributes_count"),
                        resAttrs == null
                            ? null
                            : attributes(resAttrs.entrySet())));
        assertEquals(LOG_SCHEMA_URL, (String) res.get("log_schema_url"));
        @SuppressWarnings("unchecked")
        Map<String, Object> scopeAttrs =
                (Map<String, Object>) res.get("scope_attributes");
        testScope(
                scope(
                        (String) res.get("scope_name"),
                        (String) res.get("scope_version"),
                        (Integer) res.get("scope_dropped_attributes_count"),
                        scopeAttrs == null
                            ? null
                            : attributes(scopeAttrs.entrySet())));
        @SuppressWarnings("unchecked")
        Map<String, Object> logAttrs =
                (Map<String, Object>) res.get("attributes");
        testLog(
                getLog(
                        (Long) res.get("time_unix_nano"),
                        (Long) res.get("observed_time_unix_nano"),
                        (Integer) res.get("severity_number"),
                        (String) res.get("severity_text"),
                        anyValue(
                                (String) res.get("body_type"),
                                (String) res.get("body_string")),
                        logAttrs == null
                            ? null
                            : attributes(logAttrs.entrySet()),
                        (Integer) res.get("dropped_attributes_count"),
                        (Integer) res.get("flags"),
                        (byte[]) res.get("trace_id"),
                        (byte[]) res.get("span_id")));
        assertEquals(
                l.isValid(),
                (Boolean) res.get("is_valid"));
        assertEquals(
                l.getErrorMessage(),
                (String) res.get("error_message"));
    }

    private Log defaultLog() {
        Log l = new Log(null, null, null);

        l.setFrom(batchTimestamp(),
                batchUUID(),
                LOG_SEQ_NO,
                resourceLogs(),
                scopeLogs(),
                getLog());

        return l;
    }

    private ResourceLogs resourceLogs() {
        ResourceLogs.Builder builder = ResourceLogs
                .newBuilder()
                .setSchemaUrl(resourceSchemaUrl())
                .setResource(resource());

        return builder.build();
    }

    private ScopeLogs scopeLogs() {
        ScopeLogs.Builder builder = ScopeLogs.newBuilder()
                .setSchemaUrl(LOG_SCHEMA_URL);

        builder = builder.setScope(scope());

        return builder.build();
    }

    private LogRecord getLog(
            final Long ts,
            final Long observedTs,
            final Integer severityNum,
            final String severityText,
            final AnyValue body,
            final Iterable<KeyValue> attributes,
            final Integer droppedAttrsCount,
            final Integer flags,
            final byte[] traceId,
            final byte[] spanId) {
        LogRecord.Builder builder = LogRecord.newBuilder();

        if (ts != null) {
            builder = builder.setTimeUnixNano(ts);
        }

        if (observedTs != null) {
            builder = builder.setObservedTimeUnixNano(observedTs);
        }

        if (severityNum != null) {
            builder = builder.setSeverityNumberValue(severityNum);
        }

        if (severityText != null) {
            builder = builder.setSeverityText(severityText);
        }

        if (body != null) {
            builder = builder.setBody(body);
        }

        if (attributes != null) {
            builder = builder.addAllAttributes(attributes);
        }

        if (droppedAttrsCount != null) {
            builder = builder.setDroppedAttributesCount(droppedAttrsCount);
        }

        if (flags != null) {
            builder = builder.setFlags(flags);
        }

        if (traceId != null) {
            builder = builder.setTraceId(ByteString.copyFrom(traceId));
        }

        if (spanId != null) {
            builder = builder.setSpanId(ByteString.copyFrom(spanId));
        }

        return builder.build();
    }

    private LogRecord getLog() {
        return getLog(
                LOG_TIMESTAMP,
                LOG_OBSERVED_TIMESTAMP,
                LOG_SEVERITY_NUM,
                LOG_SEVERITY,
                anyValue(LOG_BODY),
                logAttributes(),
                LOG_DROPPED_ATTRS_COUNT,
                LOG_FLAGS,
                LOG_TRACE_ID,
                LOG_SPAN_ID);
    }

    private void testLog(LogRecord actual) {
        assertEquals(LOG_TIMESTAMP, actual.getTimeUnixNano());
        assertEquals(LOG_OBSERVED_TIMESTAMP, actual.getObservedTimeUnixNano());
        assertEquals(
                SeverityNumber.forNumber(LOG_SEVERITY_NUM),
                actual.getSeverityNumber());
        assertEquals(LOG_SEVERITY, actual.getSeverityText());
        testAnyValue(anyValue(LOG_BODY), actual.getBody());
        testAttributes(logAttributes(), actual.getAttributesList());
        assertEquals(
                LOG_DROPPED_ATTRS_COUNT,
                actual.getDroppedAttributesCount());
        assertEquals(LOG_FLAGS, actual.getFlags());
        assertArrayEquals(
                LOG_TRACE_ID,
                actual.getTraceId().toByteArray());
        assertArrayEquals(
                LOG_SPAN_ID,
                actual.getSpanId().toByteArray());
    }

    private Iterable<KeyValue> logAttributes() {
        return attributes(
                Collections.singleton(
                        Map.entry(LOG_ATTR_KEY, LOG_ATTR_VAL)));
    }
}
