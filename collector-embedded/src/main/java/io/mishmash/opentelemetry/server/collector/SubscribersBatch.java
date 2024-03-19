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

import java.util.concurrent.Flow.Subscriber;

import io.opentelemetry.context.Context;

/**
 * A 'batch' of all {@link LogsSubscriber}s, {@link MetricsSubscriber}s
 * or @{link SpansSubscribers} that were given the task to process
 * an OpenTelemetry log record, metric data point or trace span.
 *
 * @param <T> the subscriber record type
 */
public class SubscribersBatch<T> extends Batch<Subscriber<? super T>> {

    /**
     * Create a new 'batch' of subscribers.
     *
     * @param parent the batch of OpenTelemetry data
     * @param otelContext {@link io.opentelemetry.context.Context} for
     * own telemetry
     */
    public SubscribersBatch(
            final Batch<T> parent,
            final Context otelContext) {
        super(otelContext);

        @SuppressWarnings("unchecked")
        T self = (T) this;

        future().whenComplete((v, t) -> {
            if (t == null) {
                parent.complete(self);
            } else {
                parent.completeExceptionally(t);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void otelComplete(final Throwable t) {
        /*
         * do nothing, as we don't want to end the current Otel
         * span in a subscriber
         */
    }
}
