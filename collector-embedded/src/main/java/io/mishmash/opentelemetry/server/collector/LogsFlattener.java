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
import java.util.UUID;

import io.opentelemetry.context.Context;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.vertx.ext.auth.User;

/**
 * Extracts individual log messages from an OTLP packet.
 *
 * Turns an {@link ExportLogsServiceRequest} into
 * an {@link Iterable} of {@link Log}s.
 *
 * The original OTLP protocol message format, as specified
 * by {@link ExportLogsServiceRequest}, contains lists of
 * individual {@link LogRecord}s. These lists are nested
 * into lists of {@link ScopeLogs}, which are in turn
 * nested inside a list of {@link ResourceLogs}.
 *
 * To facilitate further processing this class extracts
 * (or 'flattens') the above nested structures into a 'flat'
 * {@link Iterable} of individual {@link Log} instances.
 *
 * To see examples of 'flattened' data visit
 * @see <a href="https://github.com/mishmash-io/opentelemetry-server-embedded/blob/main/examples/notebooks/basics.ipynb">
 * OpenTelemetry Basics Notebook on GitHub.</a>
 */
public class LogsFlattener implements Iterable<Log> {

    /**
     * The parent {@link Batch}.
     */
    private Batch<Log> batch;
    /**
     * The own telemetry {@link Context}.
     */
    private Context otel;
    /**
     * The OTLP message received from the client.
     */
    private ExportLogsServiceRequest request;
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
     * Create a {@link Log}s flattener.
     *
     * @param parentBatch the parent {@link Batch}
     * @param otelContext the own telemetry {@link Context}
     * @param logsRequest the OTLP packet
     * @param authUser the {@link User} submitting the request or null
     * if authentication was not enabled
     */
    public LogsFlattener(
            final Batch<Log> parentBatch,
            final Context otelContext,
            final ExportLogsServiceRequest logsRequest,
            final User authUser) {
        this.batch = parentBatch;
        this.otel = otelContext;
        this.request = logsRequest;
        this.user = authUser;

        timestamp = System.currentTimeMillis();
        uuid = UUID.randomUUID().toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Log> iterator() {
        return new LogsIterator();
    }

    /**
     * The internal {@link Iterator}, as returned to user.
     */
    private final class LogsIterator implements Iterator<Log> {

        /**
         * Iterator over the OTLP Resource Logs.
         */
        private Iterator<ResourceLogs> resourceIt;
        /**
         * Iterator over the OTLP Scope Logs within an OTLP Resource Log.
         */
        private Iterator<ScopeLogs> scopeIt;
        /**
         * Iterator over individual OTLP Log Records within a scope.
         */
        private Iterator<LogRecord> logIt;
        /**
         * The current OTLP Resource Logs.
         */
        private ResourceLogs currentResource;
        /**
         * The current OTLP Scope Logs.
         */
        private ScopeLogs currentScope;
        /**
         * A counter of returned {@link Log}s.
         */
        private int itemsCount;

        /**
         * Initialize this iterator.
         */
        private LogsIterator() {
            itemsCount = 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            if (logIt == null || !logIt.hasNext()) {
                nextScope();

                while (currentScope != null
                        && !(
                                logIt = currentScope
                                            .getLogRecordsList()
                                            .iterator()
                            ).hasNext()) {
                    nextScope();
                }
            }

            return logIt != null && logIt.hasNext();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Log next() {
            LogRecord rec = logIt.next();
            Log log = new Log(batch, otel, user);

            log.setFrom(
                    timestamp,
                    uuid,
                    itemsCount++,
                    currentResource,
                    currentScope,
                    rec);

            return log;
        }

        /**
         * Move the nested iterators to next OTLP Scope Logs.
         */
        private void nextScope() {
            if (scopeIt == null || !scopeIt.hasNext()) {
                nextResource();

                while (currentResource != null
                        && !(
                                scopeIt = currentResource
                                        .getScopeLogsList()
                                        .iterator()
                            ).hasNext()) {
                    nextResource();
                }
            }

            currentScope = scopeIt == null || !scopeIt.hasNext()
                    ? null
                    : scopeIt.next();
        }

        /**
         * Move the nested iterators to next OTLP Resource Logs.
         */
        private void nextResource() {
            if (resourceIt == null) {
                resourceIt = request.getResourceLogsList().iterator();
            }

            currentResource = resourceIt.hasNext()
                    ? resourceIt.next()
                    : null;
        }
    }
}
