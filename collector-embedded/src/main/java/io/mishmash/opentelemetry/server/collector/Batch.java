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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

/**
 * Contains a number of elements that should be processed together,
 * and in parallel, but actual processing might happen at different paces.
 *
 * Essentially it's used as a way of flow control - an OTLP client submits
 * a 'batch' of logs, metrics or traces and each individual item might be
 * processed by more than one {@link LogsSubscriber}, {@link MetricsSubscriber},
 * {@link SpansSubscriber} or {@link ProfilesSubscriber}.
 *
 * A response (success or failure) cannot be returned to the client before
 * the entire OTLP packet is processed by all subscribers, but each subscriber
 * might work on its own pace.
 *
 * So, a {@link LogsCollector}, a {@link MetricsCollector}, a
 * {@link TracesCollector} or a {@link ProfilesCollector} will create a batch
 * of elements and subscribers, load it with data from an OTLP packet and
 * 'delay' the response to the client until all the processing - of all
 * elements by all subscribers - is done.
 *
 * @param <T> the type of elements (or work items) of this batch
 */
public class Batch<T> {

    /**
     * All the elements loaded into this batch.
     */
    private Set<T> elements = ConcurrentHashMap.newKeySet();
    /**
     * The elements that were processed so far.
     */
    private Set<T> processedElements = ConcurrentHashMap.newKeySet();
    /**
     * A future that completes when the batch is loaded.
     */
    private CompletableFuture<Void> loadedFuture =
            new CompletableFuture<>();
    /**
     * A future that completes when the batch is processed.
     */
    private CompletableFuture<Void> processedFuture =
            new CompletableFuture<>();
    /**
     * A future that completes when the batch completes.
     */
    private CompletableFuture<Void> finalFuture;
    /**
     * The associated {@link io.opentelemetry.context.Context} to use
     * when processing elements of this batch.
     */
    private Context otelContext;

    /**
     * Create a new batch that will process elements within the given
     * {@link io.opentelemetry.context.Context}.
     *
     * @param ctx the context
     */
    public Batch(final Context ctx) {
        this.otelContext = ctx;

        this.finalFuture = CompletableFuture
                .allOf(loadedFuture, processedFuture)
                .whenComplete((v, t) -> otelComplete(t));
    }

    /**
     * Add a single element to this batch.
     *
     * @param element the element to add
     */
    public void add(final T element) {
        elements.add(element);
    }

    /**
     * Add a collection of elements to this batch.
     *
     * @param batchElements the elements to add
     */
    public void addAll(final Collection<T> batchElements) {
        elements.addAll(batchElements);
    }

    /**
     * Complete a single element of this batch. The batch will
     * successfully complete only when all of its elements are completed
     * successfully.
     *
     * @param element the completed element
     */
    public void complete(final T element) {
        try (Scope s = currentOtelScope()) {
            if (elements.remove(element)) {
                Span.current().addEvent("batch element processed");

                processedElements.add(element);
            }

            if (elements.isEmpty() && isLoaded()) {
                Span.current().addEvent("batch processed");

                processedFuture.complete(null);
            }
        }
    }

    /**
     * Complete this batch with an error.
     *
     * @param t the error encountered
     */
    public void completeExceptionally(final Throwable t) {
        try (Scope s = currentOtelScope()) {
            Span.current().addEvent("batch processing failed");

            processedFuture.completeExceptionally(t);
        }
    }

    /**
     * Mark this batch as successfully loaded - all elements
     * have been added successfully.
     */
    public void setLoaded() {
        try (Scope s = currentOtelScope()) {
            Span.current().addEvent("batch loaded");

            loadedFuture.complete(null);

            if (elements.isEmpty()) {
                Span.current().addEvent("empty batch processed");

                processedFuture.complete(null);
            }
        }
    }

    /**
     * Mark this batch as not fully loaded because of an error.
     *
     * @param t the error encountered during loading
     */
    public void setLoadFailed(final Throwable t) {
        try (Scope s = currentOtelScope()) {
            Span.current().addEvent("batch loading failed");

            loadedFuture.completeExceptionally(t);
        }
    }

    /**
     * Check if all necessary elements of this batch have been added to it.
     *
     * @return true if batch was loaded successfully
     */
    public boolean isLoaded() {
        return loadedFuture.isDone()
                && !loadedFuture.isCompletedExceptionally()
                && !loadedFuture.isCancelled();
    }

    /**
     * Returns a future that will complete when this batch completes.
     *
     * @return the future
     */
    public CompletableFuture<Void> future() {
        return finalFuture;
    }

    /**
     * Get the elements of this batch that were already processed.
     *
     * @return a set of processed items
     */
    public Set<T> getProcessedElements() {
        return processedElements;
    }

    /**
     * Cancel this batch.
     */
    public void cancel() {
        loadedFuture.cancel(true);
        processedFuture.cancel(true);
    }

    /**
     * Check if this batch was cancelled.
     *
     * @return true if cancelled
     */
    public boolean isCancelled() {
        return loadedFuture.isCancelled() || processedFuture.isCancelled();
    }

    /**
     * Make the {@link io.opentelemetry.context.Context} the current
     * context of the current thread.
     *
     * @return the OpenTelemetry Scope
     */
    protected Scope currentOtelScope() {
        return otelContext.makeCurrent();
    }

    /**
     * Get the {@link io.opentelemetry.context.Context} to use when
     * processing data in this batch.
     *
     * @return the context of this batch
     */
    public Context getOtelContext() {
        return otelContext;
    }

    /**
     * Called when the batch is completed.
     *
     * @param t the cause of an error or null if successful
     */
    protected void otelComplete(final Throwable t) {
        try (Scope s = currentOtelScope()) {
            if (t != null) {
                Span.current().recordException(t);
            }

            Span.current().end();
        }
    }
}
