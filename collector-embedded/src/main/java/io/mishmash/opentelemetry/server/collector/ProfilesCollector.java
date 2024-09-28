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

import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.MethodDescriptor;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.proto.collector.profiles.v1experimental.ExportProfilesPartialSuccess;
import io.opentelemetry.proto.collector.profiles.v1experimental.ExportProfilesServiceRequest;
import io.opentelemetry.proto.collector.profiles.v1experimental.ExportProfilesServiceResponse;
import io.opentelemetry.proto.profiles.v1experimental.Profile;
import io.opentelemetry.proto.profiles.v1experimental.ProfileContainer;
import io.opentelemetry.proto.profiles.v1experimental.ResourceProfiles;
import io.opentelemetry.proto.profiles.v1experimental.Sample;
import io.opentelemetry.proto.profiles.v1experimental.ScopeProfiles;
import io.vertx.core.Vertx;

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
            final MethodDescriptor<
                ExportProfilesServiceRequest,
                ExportProfilesServiceResponse> exportMethod,
            final Instrumentation otel) {
        super("/v1experimental/profiles", exportMethod, otel,
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

            long timestamp = System.currentTimeMillis();
            String uuid = UUID.randomUUID().toString();

            int requestItems = 0;

            for (int rpidx = 0;
                    rpidx < request.getResourceProfilesCount();
                    rpidx++) {
                ResourceProfiles resourceProfile =
                        request.getResourceProfiles(rpidx);

                for (int spidx = 0;
                        spidx < resourceProfile.getScopeProfilesCount();
                        spidx++) {
                    ScopeProfiles scope =
                        resourceProfile.getScopeProfiles(spidx);

                    for (int pcidx = 0;
                            pcidx < scope.getProfilesCount();
                            pcidx++) {
                        ProfileContainer container = scope.getProfiles(pcidx);
                        Profile prof = container.getProfile();

                        for (int sidx = 0;
                                sidx < prof.getSampleCount();
                                sidx++) {
                            Sample sample = prof.getSample(sidx);

                            for (int vidx = 0;
                                    vidx < sample.getValueCount();
                                    vidx++) {
                                if (batch.isCancelled()) {
                                    return batch;
                                }

                                Span recordSpan = getInstrumentation()
                                        .startNewSpan("otel.record");

                                ProfileSampleValue lv =
                                        new ProfileSampleValue(
                                                batch,
                                                Context.current(),
                                                Vertx.currentContext()
                                                    .get(VCTX_EMITTER));
                                lv.setFrom(
                                        timestamp,
                                        uuid,
                                        rpidx,
                                        resourceProfile,
                                        spidx,
                                        scope,
                                        pcidx,
                                        container,
                                        prof,
                                        sidx,
                                        vidx);

                                requestItems++;

                                /*
                                 * FIXME: check if is valid and add an
                                 * error message (but still allow it to
                                 * go to subscribers)
                                 */

                                lv.addAll(getSubscribers());
                                lv.setLoaded();
                                batch.add(lv);

                                recordSpan.addEvent("Request item loaded");

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
                                                    Profiles batch %s has \
                                                    %d drop(s)""",
                                                    uuid,
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
                                            Batch id: """
                                                + uuid);

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
                        }
                    }
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
