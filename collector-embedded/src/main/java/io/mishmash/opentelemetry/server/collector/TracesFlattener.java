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
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.vertx.ext.auth.User;

/**
 * Extracts individual spans from an OTLP packet.
 *
 * Turns an {@link ExportTraceServiceRequest} into
 * an {@link Iterable} of {@link Span}s.
 *
 * The original OTLP protocol message format, as specified
 * by {@link ExportTraceServiceRequest}, contains lists of
 * individual {@link io.opentelemetry.proto.trace.v1.Span}s.
 * These lists are nested into lists of {@link ScopeSpans},
 * which are in turn nested inside a list of {@link ResourceSpans}.
 *
 * To facilitate further processing this class extracts
 * (or 'flattens') the above nested structures into a 'flat'
 * {@link Iterable} of individual {@link Span} instances.
 *
 * To see examples of 'flattened' data visit
 * @see <a href="https://github.com/mishmash-io/opentelemetry-server-embedded/blob/main/examples/notebooks/basics.ipynb">
 * OpenTelemetry Basics Notebook on GitHub.</a>
 */
public class TracesFlattener implements Iterable<Span> {

    /**
     * The parent {@link Batch}.
     */
    private Batch<Span> batch;
    /**
     * The own telemetry {@link Context}.
     */
    private Context otel;
    /**
     * The OTLP message received from the client.
     */
    private ExportTraceServiceRequest request;
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
     * Create a {@link Span}s flattener.
     *
     * @param parentBatch the parent {@link Batch}
     * @param otelContext the own telemetry {@link Context}
     * @param tracesRequest the OTLP packet
     * @param authUser the {@link User} submitting the request or null
     * if authentication was not enabled
     */
    public TracesFlattener(
            final Batch<Span> parentBatch,
            final Context otelContext,
            final ExportTraceServiceRequest tracesRequest,
            final User authUser) {
        this.batch = parentBatch;
        this.otel = otelContext;
        this.request = tracesRequest;
        this.user = authUser;

        timestamp = System.currentTimeMillis();
        uuid = UUID.randomUUID().toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Span> iterator() {
        return new SpansIterator();
    }

    /**
     * The internal {@link Iterator}, as returned to user.
     */
    private final class SpansIterator implements Iterator<Span> {

        /**
         * Iterator over the OTLP Resource Spans.
         */
        private Iterator<ResourceSpans> resourceIt;
        /**
         * Iterator over the OTLP Scope Spans within OTLP Resource Spans.
         */
        private Iterator<ScopeSpans> scopeIt;
        /**
         * Iterator over individual OTLP Spans within a scope.
         */
        private Iterator<io.opentelemetry.proto.trace.v1.Span> spanIt;
        /**
         * The current OTLP Resource Spans.
         */
        private ResourceSpans currentResource;
        /**
         * The current OTLP Scope Spans.
         */
        private ScopeSpans currentScope;
        /**
         * A counter of returned {@link Span}s.
         */
        private int itemsCount;

        /**
         * Initialize this iterator.
         */
        private SpansIterator() {
            itemsCount = 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            if (spanIt == null || !spanIt.hasNext()) {
                nextScope();

                while (currentScope != null
                        && !(
                                spanIt = currentScope
                                            .getSpansList()
                                            .iterator()
                            ).hasNext()) {
                    nextScope();
                }
            }

            return spanIt != null && spanIt.hasNext();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Span next() {
            io.opentelemetry.proto.trace.v1.Span rec = spanIt.next();
            Span span = new Span(batch, otel, user);

            span.setFrom(
                    timestamp,
                    uuid,
                    itemsCount++,
                    currentResource,
                    currentScope,
                    rec);

            return span;
        }

        /**
         * Move the nested iterators to next OTLP Scope Spans.
         */
        private void nextScope() {
            if (scopeIt == null || !scopeIt.hasNext()) {
                nextResource();

                while (currentResource != null
                        && !(
                                scopeIt = currentResource
                                        .getScopeSpansList()
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
         * Move the nested iterators to next OTLP Resource Spans.
         */
        private void nextResource() {
            if (resourceIt == null) {
                resourceIt = request.getResourceSpansList().iterator();
            }

            currentResource = resourceIt.hasNext()
                    ? resourceIt.next()
                    : null;
        }
    }
}
