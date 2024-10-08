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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.jupiter.api.Test;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;

public class LogsFlattenerTests {

    @Test
    void emptyLogs() {
        // test with completely empty request
        LogsFlattener flattener = new LogsFlattener(
                null,
                null,
                emptyRequest().build(),
                null);

        assertFalse(flattener.iterator().hasNext());

        // test with a request with an empty ResourceLogs
        flattener = new LogsFlattener(
                null,
                null,
                emptyResourceLogs()
                    .build(),
                null);

        assertFalse(flattener.iterator().hasNext());

        // test with a request with an empty ScopeLogs
        flattener = new LogsFlattener(
                null,
                null,
                emptyScopeLogs()
                    .build(),
                null);

        assertFalse(flattener.iterator().hasNext());
    }

    @Test
    void singleLog() {
        // test with a single log record
        LogsFlattener flattener = new LogsFlattener(
                null,
                null,
                createLogsRequest(
                        null,
                        null,
                        LogRecord.newBuilder())
                    .build(),
                null);
        Iterator<Log> it = flattener.iterator();

        assertTrue(it.hasNext());
        assertNotNull(it.next());
        assertFalse(it.hasNext());

        // test with a single record and then an empty resource
        flattener = new LogsFlattener(
                null,
                null,
                addEmptyResourceLogs(
                    createLogsRequest(
                            null,
                            null,
                            LogRecord.newBuilder())
                ).build(),
                null);
        it = flattener.iterator();

        assertTrue(it.hasNext());
        assertNotNull(it.next());
        assertFalse(it.hasNext());

        // test with a single record and then an empty scope
        flattener = new LogsFlattener(
                null,
                null,
                addEmptyScopeLogs(
                    createLogsRequest(
                            null,
                            null,
                            LogRecord.newBuilder())
                ).build(),
                null);
        it = flattener.iterator();

        assertTrue(it.hasNext());
        assertNotNull(it.next());
        assertFalse(it.hasNext());

        // test with a single record and then an empty resource and then an empty scope
        flattener = new LogsFlattener(
                null,
                null,
                addEmptyScopeLogs(
                    addEmptyResourceLogs(
                        createLogsRequest(
                                null,
                                null,
                                LogRecord.newBuilder()))
                ).build(),
                null);
        it = flattener.iterator();

        assertTrue(it.hasNext());
        assertNotNull(it.next());
        assertFalse(it.hasNext());

    }

    private ExportLogsServiceRequest.Builder emptyRequest() {
        return createLogsRequest();
    }

    private ExportLogsServiceRequest.Builder emptyResourceLogs() {
        return addEmptyResourceLogs(createLogsRequest());
    }

    private ExportLogsServiceRequest.Builder addEmptyResourceLogs(
            ExportLogsServiceRequest.Builder builder) {
        return builder.addResourceLogs(createResourceLogs(null, null));
    }

    private ExportLogsServiceRequest.Builder emptyScopeLogs() {
        return addEmptyScopeLogs(createLogsRequest());
    }

    private ExportLogsServiceRequest.Builder addEmptyScopeLogs(
            ExportLogsServiceRequest.Builder builder) {
        return builder.addResourceLogs(
                addEmptyScopeLogs(createResourceLogs(null, null)));
    }

    private ResourceLogs.Builder addEmptyScopeLogs(ResourceLogs.Builder builder) {
        return builder.addScopeLogs(ScopeLogs.newBuilder());
    }

    private ResourceLogs.Builder createResourceLogs(
            String schemaUrl,
            Integer droppedAttributesCnt,
            KeyValue.Builder...attributes) {
        ResourceLogs.Builder res = ResourceLogs.newBuilder();

        if (schemaUrl != null) {
            res = res.setSchemaUrl(schemaUrl);
        }

        if (attributes == null && droppedAttributesCnt == null) {
            return res;
        }

        Resource.Builder rb = Resource.newBuilder();

        if (droppedAttributesCnt != null) {
            rb = rb.setDroppedAttributesCount(droppedAttributesCnt);
        }

        if (attributes != null) {
            rb = rb.addAllAttributes(
                    Arrays.stream(attributes)
                        .map(a -> a.build())
                        .toList());
        }

        return res.setResource(rb);
    }

    private ExportLogsServiceRequest.Builder createLogsRequest() {
        return ExportLogsServiceRequest.newBuilder();
    }

    private ExportLogsServiceRequest.Builder createLogsRequest(
            ResourceLogs.Builder resource,
            ScopeLogs.Builder scope,
            LogRecord.Builder...logs) {
        if (resource == null) {
            resource = createResourceLogs(null, null);
        }

        return createLogsRequest()
                .addResourceLogs(resource
                        .addScopeLogs(
                                addLogs(scope, logs
                        )));
    }

    private ScopeLogs.Builder addLogs(
            ScopeLogs.Builder scope,
            LogRecord.Builder...logs) {
        if (scope == null) {
            scope = ScopeLogs.newBuilder();
        }

        return scope.addAllLogRecords(Arrays.stream(logs)
            .map(l -> l.build())
            .toList());
    }
}
