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

import java.util.concurrent.ForkJoinPool;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesPartialSuccess;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse;
import io.vertx.core.Vertx;
import io.vertx.grpc.common.ServiceMethod;

/**
 * Processes incoming OpenTelemetry profiles packets - extracts individual
 * Profiles from a packet and queues them to all {@link ProfilesSubscriber}s.
 *
 * Also notifies the client on potential errors.
 */
public class ProfilesCollector
        extends AbstractCollector<
            ExportProfilesServiceRequest,
            ExportProfilesServiceResponse,
            ProfileSampleValue> {

    /**
     * The {@link java.util.logging.Logger} used by this class.
     */
    private static final Logger LOG =
            Logger.getLogger(ProfilesCollector.class.getName());

    /**
     * Creates a new profiles collector to be added to a Vert.x routing
     * context.
     *
     * @param exportMethod the gRPC method to be attached to
     * @param otel a helper for own telemetry needs
     */
    public ProfilesCollector(
            final ServiceMethod<
                ExportProfilesServiceRequest,
                ExportProfilesServiceResponse> exportMethod,
            final Instrumentation otel) {
        super("/v1development/profiles", exportMethod, otel,
                ExportProfilesServiceRequest::parser,
                ExportProfilesServiceRequest::newBuilder,
                ExportProfilesServiceResponse::parser,
                ExportProfilesServiceResponse::newBuilder,
                new ForkJoinPool());
    }

    /**
     * Processes incoming packets (or batches) - extracts
     * every OpenTelemetry value (contained in each Location of a
     * Sample of a Profile) and publishes it to all
     * {@link LogsSubscriber}s.
     *
     * @param request the client's OTLP request
     * @param transport the OTLP transport - "grpc" or "http"
     * @param encoding the OTLP encoding - "protobuf" or "json"
     * @param otelContext the {@link io.opentelemetry.context.Context}
     * used for own telemetry
     */
    @Override
    protected Batch<ProfileSampleValue> loadBatch(
            final ExportProfilesServiceRequest request,
            final String transport,
            final String encoding,
            final Context otelContext) {
        try (Scope s = otelContext.makeCurrent()) {
            Span.current().setAttribute("otel.collector.name", "profiles");

            Batch<ProfileSampleValue> batch = new Batch<>(otelContext);

            int requestItems = 0;

            for (ProfileSampleValue lv : new ProfilesFlattener(
                                            batch,
                                            Context.current(),
                                            request,
                                            Vertx.currentContext()
                                                .get(VCTX_EMITTER))) {
                if (batch.isCancelled()) {
                    return batch;
                }

                requestItems++;

                /*
                 * FIXME: check if is valid and add an
                 * error message (but still allow it to
                 * go to subscribers)
                 */

                lv.addAll(getSubscribers());
                lv.setLoaded();
                batch.add(lv);

                int estimatedLag = offer(
                        lv,
                        (subscriber, droppedItem) -> {
                            /*
                             * set an error on this in the
                             * response,
                             * FIXME: use another
                             * exception class
                             */
                            droppedItem
                                .completeExceptionally(
                                    new RuntimeException(
                                        """
                                        Profiles \
                                        collector \
                                        subscriber """
                                        + subscriber
                                        + """
                                        dropped a log \
                                        record"""));

                            // droppedItem
                            //      .complete(subscriber);
                            // do not retry
                            return false;
                        });

                if (estimatedLag < 0) {
                    /*
                     * says how many subscribers dropped
                     * the message
                     */
                    LOG.info(
                            String.format("""
                                    Profiles batch has %d drop(s)""",
                                    (-estimatedLag)));
                    addDroppedRequestItems(
                            (-estimatedLag),
                            transport,
                            encoding);
                } else if (estimatedLag == 0) {
                    /*
                     * there were no subscribers,
                     * set an error
                     */
                    batch.setLoadFailed(
                            new IllegalStateException("""
                                    Profiles collector \
                                    currently has \
                                    no subscribers"""));
                    LOG.log(Level.SEVERE, """
                            Profiles batch load failed, \
                            profiles collector \
                            currently has no subscribers. \
                            Batch id: """);

                    return batch;
                // } else {
                    /*
                     * positive number is the estimated
                     * lag - number of items submitted
                     * but not yet consumed
                     */

                    // LOG.info("Logs estimated lag: "
                    // + estimatedLag);
                }
            }

            batch.setLoaded();

            addRequestItems(requestItems, transport, encoding);

            return batch;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ExportProfilesServiceResponse
            getBatchResponse(
                    final ExportProfilesServiceRequest request,
                    final Batch<ProfileSampleValue> completedBatch,
                    final String transport,
                    final String encoding) {
        ExportProfilesServiceResponse.Builder response =
                ExportProfilesServiceResponse.newBuilder();
        String errorMessage = null;
        int numInvalid = 0;

        for (ProfileSampleValue p : completedBatch.getProcessedElements()) {
            if (!p.isValid()) {
                numInvalid++;
            }

            if (p.getErrorMessage() != null && errorMessage == null) {
                errorMessage = p.getErrorMessage();
            }
        }

        ExportProfilesPartialSuccess.Builder partialSuccess =
                ExportProfilesPartialSuccess.newBuilder();

        if (numInvalid > 0) {
            partialSuccess.setRejectedProfiles(numInvalid);
        }

        if (errorMessage != null) {
            partialSuccess.setErrorMessage(errorMessage);
        }

        if (numInvalid > 0 || errorMessage != null) {
            response.setPartialSuccess(partialSuccess);
        }

        if (numInvalid > 0) {
            addPartiallySucceededRequests(1, transport, encoding);
        } else {
            addSucceededRequests(1, transport, encoding);
        }

        return response.build();
    }

    /**
     * Returns "profiles-experimental" as the signal type for
     * this collector's own telemetry.
     */
    @Override
    protected String telemetrySignalType() {
        return "profiles-experimental";
    }
}
