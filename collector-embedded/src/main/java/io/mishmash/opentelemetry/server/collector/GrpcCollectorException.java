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

import io.vertx.grpc.common.GrpcStatus;

/**
 * An {@link java.lang.Exception} that also contains the
 * {@link io.vertx.grpc.common.GrpcStatus} that should be returned
 * to the client.
 */
public class GrpcCollectorException extends Exception {

    /**
     * A serial version UUID for serializing.
     */
    private static final long serialVersionUID = -9046166068921544819L;

    /**
     * The gRPC status.
     */
    private GrpcStatus status;

    /**
     * Instantiate a new {@link GrpcCollectorException}.
     *
     * @param grpcStatus the gRPC status to be sent to the client
     */
    public GrpcCollectorException(final GrpcStatus grpcStatus) {
        this(grpcStatus, null, null);
    }

    /**
     * Instantiate a new {@link GrpcCollectorException}.
     *
     * @param grpcStatus the gRPC status to be sent to the client
     * @param message a descriptive message of the error encountered
     */
    public GrpcCollectorException(
            final GrpcStatus grpcStatus,
            final String message) {
        this(grpcStatus, message, null);
    }

    /**
     * Instantiate a new {@link GrpcCollectorException}.
     *
     * @param grpcStatus the gRPC status to be sent to the client
     * @param cause the exception that caused this
     */
    public GrpcCollectorException(
            final GrpcStatus grpcStatus,
            final Throwable cause) {
        this(grpcStatus, cause.getMessage(), cause);
    }

    /**
     * Instantiate a new {@link GrpcCollectorException}.
     *
     * @param grpcStatus the gRPC status to be sent to the client
     * @param message a descriptive message of the error encountered
     * @param cause the exception that caused this
     */
    public GrpcCollectorException(
            final GrpcStatus grpcStatus,
            final String message,
            final Throwable cause) {
        super(message, cause);

        this.status = grpcStatus;
    }

    /**
     * Get the gRPC status that should be returned to the client.
     *
     * @return the gRPC status
     */
    public GrpcStatus getGrpcStatus() {
        return status;
    }
}
