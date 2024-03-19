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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

/**
 * A simple class with helper methods for OpenTelemetry embedded collectors
 * own telemetry.
 */
public class Instrumentation {

    /**
     * The instrumentation scope used.
     */
    private static final String OTLP_INSTRUMENTATION_SCOPE =
            "OpenTelemetry Server";
    /**
     * The instrumentation version used.
     */
    private static final String OTLP_INSTRUMENTATION_VERSION = "1";

    /**
     * An internal {@link io.opentelemetry.api.trace.Tracer}.
     */
    private Tracer tracer = GlobalOpenTelemetry
            .getTracer(
                    OTLP_INSTRUMENTATION_SCOPE,
                    OTLP_INSTRUMENTATION_VERSION);
    /**
     * An internal {@link io.opentelemetry.api.metric.Meter}.
     */
    private Meter meter = GlobalOpenTelemetry
            .meterBuilder(OTLP_INSTRUMENTATION_SCOPE)
            .setInstrumentationVersion(OTLP_INSTRUMENTATION_VERSION)
            .build();

    /**
     * Start a new span.
     *
     * @param spanName the name of the new span
     * @return the created span
     */
    public Span startNewSpan(final String spanName) {
        return tracer.spanBuilder(spanName).startSpan();
    }

    /**
     * Start a new child span.
     *
     * @param parent the parent span
     * @param spanName the new span name
     * @return the created span
     */
    public Span startNewSpan(final Span parent, final String spanName) {
        return tracer.spanBuilder(spanName)
                .setParent(Context.current().with(parent))
                .startSpan();
    }

    /**
     * Set the given span as current.
     *
     * @param span the span
     * @return the scope to be used
     */
    public Scope withSpan(final Span span) {
        return span.makeCurrent();
    }

    /**
     * Wraps a {@link java.util.concurrent.CompletableFuture} so
     * that when it completes a span will be ended accordingly.
     *
     * @param <T> the return type of the future
     * @param future the future
     * @param newSpanSupplier supplier of the span to wrap into
     * @return a new future to be used instead
     */
    public <T> CompletableFuture<T> wrapInSpan(
            final CompletableFuture<T> future,
            final Supplier<Span> newSpanSupplier) {
        Span span = newSpanSupplier.get();

        return future.whenComplete((v, t) -> {
            if (t != null) {
                addErrorEvent(span, t);
            }

            span.end();
        });
    }

    /**
     * Add an error to a span.
     *
     * @param span the span
     * @param t the exception that caused the error
     */
    public void addErrorEvent(final Span span, final Throwable t) {
        span.setStatus(StatusCode.ERROR, t.getMessage());
        span.recordException(t);
    }

    /**
     * Add an error to a span.
     *
     * @param span the span
     * @param errorMsg a text error message
     */
    public void addErrorEvent(final Span span, final String errorMsg) {
        span.setStatus(StatusCode.ERROR, errorMsg);
    }

    /**
     * Add an event to the current span in the given context.
     *
     * @param ctx the context
     * @param message the event message
     */
    public void addEvent(final Context ctx, final String message) {
        if (ctx != null && message != null && !message.isBlank()) {

            try (Scope s = ctx.makeCurrent()) {
                Span.current().addEvent(message);
            }
        }
    }

    /**
     * Add an error to the current span in the given context.
     *
     * @param ctx the context
     * @param t the exception that caused the error
     */
    public void addErrorEvent(final Context ctx, final Throwable t) {
        if (ctx != null) {

            try (Scope s = ctx.makeCurrent()) {
                Span currentSpan = Span.current();
                if (t != null) {
                    addErrorEvent(currentSpan, t);
                }
            }
        }
    }

    /**
     * End the current span in a given context with an error.
     *
     * @param ctx the context
     * @param t the exception that caused the error
     */
    public void endCurrentSpan(final Context ctx, final Throwable t) {
        if (ctx != null) {

            try (Scope s = ctx.makeCurrent()) {
                Span currentSpan = Span.current();
                if (t != null) {
                    addErrorEvent(currentSpan, t);
                }
                currentSpan.end();
            }
        }
    }

    /**
     * Create a new long counter metric.
     *
     * @param name the metric name
     * @param unit the unit of measure
     * @param description the metric description
     * @return a new long counter
     */
    public LongCounter newLongCounter(
            final String name,
            final String unit,
            final String description) {
        return meter.counterBuilder(name)
                .setUnit(unit)
                .setDescription(description)
                .build();
    }

    /**
     * Create a new long UpDown counter metric.
     *
     * @param name the metric name
     * @param unit the unit of measure
     * @param description the metric description
     * @return a new UpDown counter metric
     */
    public LongUpDownCounter newLongUpDownCounter(
            final String name,
            final String unit,
            final String description) {
        return meter.upDownCounterBuilder(name)
                .setUnit(unit)
                .setDescription(description)
                .build();
    }

    /**
     * Create a new long gauge metric with a callback.
     *
     * @param name the metric name
     * @param unit the unit of measure
     * @param description the metric description
     * @param measurementCallback the callback to get values from
     * @return the new long gauge
     */
    public ObservableLongGauge newLongGauge(
            final String name,
            final String unit,
            final String description,
            final Consumer<ObservableLongMeasurement> measurementCallback) {
        return meter.gaugeBuilder(name)
                .setUnit(unit)
                .setDescription(description)
                .ofLongs()
                .buildWithCallback(measurementCallback);
    }

    /**
     * Create a new long histogram metric.
     *
     * @param name the metric name
     * @param unit the unit of measure
     * @param description the metric description
     * @param bucketBoundaries the desired histogram buckets
     * @return the new long histogram metric
     */
    public LongHistogram newLongHistogram(
            final String name,
            final String unit,
            final String description,
            final List<Long> bucketBoundaries) {
        return meter.histogramBuilder(name)
                .setDescription(description)
                .setUnit(unit)
                .ofLongs()
                .setExplicitBucketBoundariesAdvice(bucketBoundaries)
                .build();
    }
}
