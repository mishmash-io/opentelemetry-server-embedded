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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.util.JsonFormat;

import io.grpc.MethodDescriptor;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.grpc.common.GrpcStatus;
import io.vertx.grpc.server.GrpcServer;
import io.vertx.grpc.server.GrpcServerRequest;

/**
 * <p>
 * This class handles the OTLP protocol for gRPC and HTTP transports for
 * a collector of a given signal type (like logs, metrics or traces).
 * </p>
 * <p>
 * As each collector attaches to own gRPC methods and own HTTP endpoints,
 * the common functionality of receiving, parsing, 'queuing' signals
 * for subscribing classes to process and responding back to clients
 * is kept here.
 * </p>
 * <p>
 * This class provides methods to bind to Vert.x gRPC and HTTP servers
 * and to process the OTLP requests and responses, leaving only the
 * task of breaking up the data into individual items to be submitted
 * to the subscribers. See {@link LogsCollector}, {@link MetricsCollector}
 * and {@link TracesCollector}.
 * </p>
 *
 * @param <REQ> the type of OTLP request being handled by this collector
 * @param <RESP> the type of OTLP response
 * @param <RECORD> the type of data items a request is broken into
 */
public abstract class AbstractCollector<
            REQ extends AbstractMessage,
            RESP extends AbstractMessage,
            RECORD>
        extends SubmissionPublisher<RECORD> {

    /**
     * The {@link java.util.logging.Logger} used by this class.
     */
    private static final Logger LOG =
            Logger.getLogger(AbstractCollector.class.getName());

    /**
     * Default timeout (in sec) to process a single OTLP packet.
     */
    public static final int DEFAULT_TIMEOUT_SEC = 10;
    /**
     * Default timeout (in sec) to complete a call to close() this collector.
     */
    public static final int DEFAULT_CLOSE_TIMETOUT_SEC = 5;
    /**
     * Sleep interval when waiting to close().
     */
    private static final int DEFAULT_CLOSE_SLEEP_MS = 500;
    /**
     * Default maximum HTTP request body size in bytes.
     */
    public static final int DEFAULT_MAX_BODY_LEN = 10 * 1024 * 1024;

    /**
     * Default size of a subscriber queue.
     */
    public static final int DEFAULT_SUBSCRIBER_QUEUE_SIZE = 4096;

    /**
     * Routing context parameter key for 'isJson'.
     */
    private static final String CTX_IS_JSON = "isJson";
    /**
     * Routing context parameter key for 'isError'.
     */
    private static final String CTX_IS_ERROR = "isError";
    /**
     * Routing context parameter key for 'parsedRequest'.
     */
    private static final String CTX_REQUEST = "parsedRequest";
    /**
     * Routing context parameter key for 'response'.
     */
    private static final String CTX_RESPONSE = "response";
    /**
     * Routing context parameter key for 'errorCode'.
     */
    private static final String CTX_ERROR_CODE = "errorCode";
    /**
     * Routing context parameter key for 'otelContext'.
     */
    private static final String CTX_OTEL_CONTEXT = "otelContext";

    /**
     * The helper object for own telemetry.
     */
    private Instrumentation otel;
    /**
     * The HTTP path where this collector should be bound.
     */
    private String httpPath;
    /**
     * The gRPC method that this collector handles.
     */
    private MethodDescriptor<REQ, RESP> grpcMethod;

    /**
     * A parser for the request object.
     */
    private Supplier<Parser<REQ>> reqParser;
    /**
     * A builder for the request object.
     */
    private Supplier<Message.Builder> reqBuilder;
    /**
     * A parser for the response object.
     */
    private Supplier<Parser<RESP>> respParser;
    /**
     * A builder for the response object.
     */
    private Supplier<Message.Builder> respBuilder;

    /**
     * Counts the number of successful requests.
     */
    private LongCounter succeededRequests;
    /**
     * Counts the number of partially successful requests.
     */
    private LongCounter partiallySucceededRequests;
    /**
     * Counts the number of failed requests.
     */
    private LongCounter failedRequests;
    /**
     * Counts the number of requests currently being processed.
     */
    private LongUpDownCounter requestsInProcess;
    /**
     * Counts the number of requested items.
     */
    private LongCounter requestItems;
    /**
     * Counts the number of dropped items.
     */
    private LongCounter droppedRequestItems;
    /**
     * Reports maximum processing lag.
     */
    private LongHistogram maxLag;
    /**
     * Reports the demand for items.
     */
    private LongHistogram minDemand;
    /**
     * The number of current subscribers to this collector.
     */
    private ObservableLongGauge currentSubscribers;

    /**
     * Thread pool to be used for processing.
     */
    private ForkJoinPool executor;

    /**
     * HTTP status code 200.
     */
    public static final int HTTP_OK = 200;
    /**
     * HTTP status code 400.
     */
    protected static final int HTTP_BAD_REQUEST = 400;
    /**
     * HTTP status code 413.
     */
    protected static final int HTTP_PAYLOAD_TOO_LARGE = 413;
    /**
     * HTTP status code 417.
     */
    protected static final int HTTP_EXPECTATION_FAILED = 417;
    /**
     * HTTP status code 429.
     */
    protected static final int HTTP_TOO_MANY_REQUESTS = 429;
    /**
     * HTTP status code 500.
     */
    protected static final int HTTP_INTERNAL_ERROR = 500;
    /**
     * HTTP status code 502.
     */
    protected static final int HTTP_BAD_GATEWAY = 502;
    /**
     * HTTP status code 503.
     */
    protected static final int HTTP_SERVICE_UNAVAILABLE = 503;
    /**
     * HTTP status code 504.
     */
    protected static final int HTTP_GATEWAY_TIMEOUT = 504;

    /**
     * Create a new collector.
     *
     * @param httpUrlPath the URL path for HTTP requests
     * @param exportMethod the gRPC method to bind to
     * @param otelHelper a helper for own instrumentation
     * @param requestParser to get a parser for the request type
     * @param requestBuilder to get a builder for the request type
     * @param responseParser to get a parser for the response type
     * @param responseBuilder to get a builder for the response type
     * @param threadPool the thread pool to use for processing
     */
    public AbstractCollector(
            final String httpUrlPath,
            final MethodDescriptor<REQ, RESP> exportMethod,
            final Instrumentation otelHelper,
            final Supplier<Parser<REQ>> requestParser,
            final Supplier<Message.Builder> requestBuilder,
            final Supplier<Parser<RESP>> responseParser,
            final Supplier<Message.Builder> responseBuilder,
            final ForkJoinPool threadPool) {
        super(Context.taskWrapping(threadPool), DEFAULT_SUBSCRIBER_QUEUE_SIZE);

        this.httpPath = httpUrlPath;
        this.grpcMethod = exportMethod;
        this.otel = otelHelper;

        this.reqParser = requestParser;
        this.reqBuilder = requestBuilder;
        this.respParser = responseParser;
        this.respBuilder = responseBuilder;

        this.executor = threadPool;

        initInstrumentation();
    }

    /**
     * Creates a new batch from a client's request.
     *
     * @param request the client's OTLP request
     * @param transport the OTLP transport used
     * @param encoding the encoding of the OTLP packet
     * @param otelContext context to use for processing
     * @return a new {@link Batch} with all items of the request
     */
    protected abstract Batch<RECORD> loadBatch(
            REQ request,
            String transport,
            String encoding,
            Context otelContext);
    /**
     * Called to compute the correct response to the client after an
     * entire batch (or packet) of OpenTelemetry logs, metrics or spans
     * has been processed.
     *
     * @param request the original request from the client
     * @param completedBatch the completed batch of records
     * @param transport the OTLP transport used - "grpc" or "http"
     * @param encoding - the OTLP encoding - "protobuf" or "json"
     * @return the correct OTLP reposne to be sent back to the client
     */
    protected abstract RESP getBatchResponse(
            REQ request,
            Batch<RECORD> completedBatch,
            String transport,
            String encoding);
    /**
     * Get the type of telemetry signals processed by this collector.
     * Used to format some strings like metric names, logs, etc.
     *
     * @return the signal type
     */
    protected abstract String telemetrySignalType();

    /**
     * Create own instrumentation metrics.
     */
    protected void initInstrumentation() {
        succeededRequests = otel.newLongCounter(
                telemetrySignalType()
                    + "_succeeded_requests",
                "count",
                "The count of succeeded "
                        + telemetrySignalType()
                        + " requests");

        partiallySucceededRequests = otel.newLongCounter(
                telemetrySignalType()
                    + "_partially_succeeded_requests",
                "count",
                "The count of partially succeeded "
                        + telemetrySignalType()
                        + " requests");

        failedRequests = otel.newLongCounter(
                telemetrySignalType()
                    + "_failed_requests",
                "count",
                "The count of failed "
                        + telemetrySignalType()
                        + " requests");

        requestsInProcess = otel.newLongUpDownCounter(
                telemetrySignalType()
                    + "_requests_in_process",
                "count",
                "The count of "
                        + telemetrySignalType()
                        + " requests that are currently in process");

        requestItems = otel.newLongCounter(
                telemetrySignalType()
                    + "_request_items",
                "count",
                "The count of "
                        + telemetrySignalType()
                        + " request items");

        droppedRequestItems = otel.newLongCounter(
                telemetrySignalType()
                    + "_dropped_request_items",
                "count",
                "The count of "
                        + telemetrySignalType()
                        + " dropped request items");

        maxLag = otel.newLongHistogram(
                telemetrySignalType()
                    + "_submission_max_lag",
                "count", """
                The estimate of the maximum number of items produced \
                but not yet consumed""",
                List.of(Long.MIN_VALUE, Long.MAX_VALUE));

        minDemand = otel.newLongHistogram(
                telemetrySignalType()
                    + "_submission_min_demand",
                "count", """
                The estimate of the minimum number of items requested \
                but not yet produced""",
                List.of(Long.MIN_VALUE, Long.MAX_VALUE));

        currentSubscribers = otel.newLongGauge(
                telemetrySignalType()
                    + "_current_subscribers",
                "count",
                "The number of current subscribers",
                (measurement) -> measurement.record(getNumberOfSubscribers()));
    }

    /**
     * Get the OpenTelemetry instrumentation helper.
     *
     * @return the instrumentation helper
     */
    protected Instrumentation getInstrumentation() {
        return otel;
    }

    /**
     * Parses an incoming HTTP protobuf-encoded request.
     *
     * @param is an input stream of the data
     * @return the parsed request object
     * @throws Exception if an error is encountered
     */
    protected REQ parseHttpProtobuf(final InputStream is) throws Exception {
        return reqParser.get().parseFrom(is);
    }

    /**
     * Parses an incoming HTTP json-encoded request.
     *
     * @param is an input stream of the data
     * @return the parsed request object
     * @throws Exception if an error is encountered
     */
    @SuppressWarnings("unchecked")
    protected REQ parseHttpJson(final InputStream is) throws Exception {
        Message.Builder builder = reqBuilder.get();

        JsonFormat.parser()
            .merge(
                    new InputStreamReader(is, StandardCharsets.UTF_8),
                    builder);

        return (REQ) builder.build();
    }

    /**
     * Converts a response object to JSON before sending back to client.
     *
     * @param response the response to format
     * @return a JSON string
     * @throws Exception if an error is encountered
     */
    protected String responseToJson(final RESP response) throws Exception {
        return JsonFormat.printer().print(response);
    }

    /**
     * Converts a response object to protobuf before sending back to client.
     *
     * @param response the response to format
     * @return a JSON string
     * @throws Exception if an error is encountered
     */
    protected byte[] responseToProtobuf(
            final RESP response)
                    throws Exception {
        return response.toByteArray();
    }

    /**
     * Publishes an OTLP packet (request) to all current subscribers.
     *
     * @param request the client's request
     * @param transport the OTLP transport used
     * @param encoding the encoding of the OTLP transport
     * @param otelContext a context to be used for own telemetry
     * @return a {@link Batch} of all items
     */
    protected Batch<RECORD> publish(
            final REQ request,
            final String transport,
            final String encoding,
            final Context otelContext) {
        try (Scope s = otelContext.makeCurrent()) {
            Span span = otel.startNewSpan("otel.collect");
            span.setAttribute("otel.transport", transport);
            span.setAttribute("otel.encoding", encoding);

            minDemand.record(estimateMinimumDemand());

            try {
                Batch<RECORD> batch = loadBatch(
                        request,
                        transport,
                        encoding,
                        Context.current());

                batch.future().whenComplete((v, t) -> {
                    if (t != null) {
                        otel.addErrorEvent(span, t);
                    }

                    span.end();
                });

                return batch;
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Collector failed to publish data", e);

                otel.addErrorEvent(span, e);
                span.end();

                throw e;
            } finally {
                maxLag.record(estimateMaximumLag());
            }
        }
    }

    /**
     * Responds to a processed gRPC OTLP request.
     *
     * @param grpcRequest the gRPC request
     * @param request the OTLP request
     * @param batch the processing {@link Batch}
     * @param err set if an error occurred during processing
     */
    protected void respond(
            final GrpcServerRequest<REQ, RESP> grpcRequest,
            final REQ request,
            final Batch<RECORD> batch,
            final Throwable err) {
        if (err == null) {
            try {
                grpcRequest.response()
                    .end(getBatchResponse(request, batch, "grpc", "protobuf"));
            } catch (Exception e) {
                LOG.log(Level.SEVERE, """
                        Collector failed to build grpc response, returning \
                        status UNAVAILABLE""",
                        e);

                GrpcStatus errorCode = GrpcStatus.UNAVAILABLE;
                grpcRequest.response().status(errorCode).end();

                addGrpcFailedRequests(1, errorCode);
            }
        } else if (err instanceof TimeoutException) {
            LOG.log(Level.SEVERE,
                    "Timeout in collector, returning grpc status UNAVAILABLE",
                    err);

            GrpcStatus errorCode = GrpcStatus.UNAVAILABLE;
            grpcRequest.response().status(errorCode).end();

            addGrpcFailedRequests(1, errorCode);
        } else {
            GrpcStatus errorCode = (err instanceof GrpcCollectorException)
                    ? ((GrpcCollectorException) err).getGrpcStatus()
                    : GrpcStatus.INTERNAL;

            LOG.log(Level.SEVERE, """
                    Collector failed to process message, returning \
                    grpc status """
                        + errorCode,
                    err);

            grpcRequest.response().status(errorCode).end();

            addGrpcFailedRequests(1, errorCode);
        }
    }

    /**
     * Handles an incoming gRPC request.
     *
     * @param request the gRPC request
     */
    protected void handle(final GrpcServerRequest<REQ, RESP> request) {
        request.handler(r -> {
            Span span = otel.startNewSpan("otel." + telemetrySignalType());
            span.setAttribute("otel.transport", "grpc");
            span.setAttribute("otel.encoding", "protobuf");

            try (Scope s = otel.withSpan(span)) {
                if (!this.hasSubscribers()) {
                    respond(
                            request,
                            null,
                            null,
                            new GrpcCollectorException(
                                    GrpcStatus.UNAVAILABLE, """
                                        Collector has no subscribers, \
                                        returning service unavailable"""));

                    otel.addErrorEvent(
                            span,
                            new GrpcCollectorException(
                                    GrpcStatus.UNAVAILABLE,
                                    "Collector has no subscribers"));

                    span.end();

                    return;
                }

                addRequestsInProcess(1, "grpc", "protobuf");
                Batch<RECORD> batch = publish(
                        r,
                        "grpc",
                        "protobuf",
                        Context.current());

                batch.future()
                        .orTimeout(DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS)
                        .whenComplete((v, err) -> {
                            respond(request, r, batch, err);

                            addRequestsInProcess(-1, "grpc", "protobuf");

                            if (err != null) {
                                otel.addErrorEvent(span, err);
                            }

                            span.end();
                        });
            }
        });
    }

    /**
     * Binds this collector to the gRPC server.
     *
     * @param server the gRPC server to bind to
     */
    public void bind(final GrpcServer server) {
        LOG.log(Level.INFO,
                "Attaching collector at grpc method "
                        + "\"" + grpcMethod + "\""
                        + " with request timeout of "
                        + DEFAULT_TIMEOUT_SEC + " seconds");

        server.callHandler(grpcMethod, this::handle);
    }

    /**
     * Checks the Content-Type header of an incoming HTTP request for
     * validity.
     *
     * @param ctx the routing context of the request
     */
    protected void checkContentType(final RoutingContext ctx) {
        HttpServerRequest request = ctx.request();

        request.pause();

        Span span = otel.startNewSpan("otel." + telemetrySignalType());
        span.setAttribute("otel.transport", "http");

        try (Scope s = otel.withSpan(span)) {
            if (isProtobufContent(request)) {
                addRequestsInProcess(1, "http", "protobuf");

                span.setAttribute("otel.encoding", "protobuf");

                setJsonRequest(ctx, false);
                setOtelContext(ctx, Context.current());

                ctx.next();
            } else if (isJsonContent(request)) {
                addRequestsInProcess(1, "http", "json");

                span.setAttribute("otel.encoding", "json");

                setJsonRequest(ctx, true);
                setOtelContext(ctx, Context.current());

                ctx.next();
            } else {
                LOG.log(Level.WARNING, """
                        Collector received a message of unexpected \
                        content-type, returning error to client""");

                int errorCode = HTTP_BAD_REQUEST;

                ctx.response()
                    .setStatusMessage(
                        "Unexpected content-type: "
                                + ctx.request()
                                    .getHeader(HttpHeaders.CONTENT_TYPE));
                ctx.fail(errorCode);

                addHttpFailedRequests(1, errorCode, null);
                otel.addErrorEvent(span,
                    new RuntimeException("""
                        Collector received a message of unexpected \
                        content-type"""));

                span.end();
            }
        }
    }

    /**
     * Checks the body of an incoming HTTP request for
     * validity.
     *
     * @param context the routing context of the request
     */
    protected void checkBody(final RoutingContext context) {
        // FIXME: must be a config option
        long bodyLimit = DEFAULT_MAX_BODY_LEN;

        HttpServerRequest request = context.request();

        // taken from vertx BodyHandlerImpl

        // Check if a request has a request body.
        // A request with a body __must__ either have `transfer-encoding`
        // or `content-length` headers set.
        // http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.3
        final long parsedContentLength = parseContentLengthHeader(request);
        // http2 never transmits a `transfer-encoding` as frames are chunks.
        final boolean hasTransferEncoding =
                request.version() == HttpVersion.HTTP_2
                    || request.headers()
                            .contains(HttpHeaders.TRANSFER_ENCODING);

        if (!hasTransferEncoding && parsedContentLength == -1) {
            // there is no "body"
            int errorCode = HTTP_BAD_REQUEST;

            context.response().setStatusMessage("Request body is missing");
            context.fail(errorCode);

            addRequestsInProcess(-1, "http", httpEncoding(context));
            addHttpFailedRequests(1, errorCode, httpEncoding(context));

            otel.endCurrentSpan(
                    getOtelContext(context),
                    new RuntimeException("Request body is missing"));

            return;
        }

        /*
         * before parsing the body we can already discard a bad request
         * just by inspecting the content-length against
         *
         * the body limit, this will reduce load on the server by totally
         * skipping parsing the request body
         */
        if (bodyLimit != -1 && parsedContentLength != -1) {
            if (parsedContentLength > bodyLimit) {
                int errorCode = HTTP_PAYLOAD_TOO_LARGE;

                context.response()
                    .setStatusMessage(
                            "Request body limit: "
                                    + bodyLimit
                                    + " bytes");
                context.fail(errorCode);

                addRequestsInProcess(-1, "http", httpEncoding(context));
                addHttpFailedRequests(1, errorCode, httpEncoding(context));

                otel.endCurrentSpan(
                        getOtelContext(context),
                        new RuntimeException("Request body limit exceeded"));

                return;
            }
        }

        // handle expectations
        // https://httpwg.org/specs/rfc7231.html#header.expect
        final String expect = request.getHeader(HttpHeaders.EXPECT);
        if (expect != null) {
            // requirements validation
            if (expect.equalsIgnoreCase("100-continue")) {
                /*
                 * A server that receives a 100-continue expectation in an
                 * HTTP/1.0 request MUST ignore that expectation.
                 */
                if (request.version() != HttpVersion.HTTP_1_0) {
                    // signal the client to continue
                    context.response().writeContinue();
                }
            } else {
                /*
                 * the server cannot meet the expectation, we only know
                 * about 100-continue
                 */
                int errorCode = HTTP_EXPECTATION_FAILED;

                if (!expect.isBlank()) {
                    context.response()
                        .setStatusMessage(
                            "The server cannot meet the expectation: "
                                    + expect);
                }

                context.fail(errorCode);

                addRequestsInProcess(-1, "http", httpEncoding(context));
                addHttpFailedRequests(1, errorCode, httpEncoding(context));

                otel.endCurrentSpan(
                        getOtelContext(context),
                        new RuntimeException(
                                "The server cannot meet the expectation: "
                                        + expect));

                return;
            }
        }

        context.next();
    }

    /**
     * Fail an incoming request if we have no subscribers.
     *
     * @param ctx the request's context
     */
    protected void checkSubscribers(final RoutingContext ctx) {
        if (!this.hasSubscribers()) {
            LOG.log(Level.SEVERE, """
                    Collector has no subscribers, returning internal \
                            server error""");

            int errorCode = HTTP_INTERNAL_ERROR;

            ctx.response().setStatusMessage("Collector has no subscribers");

            ctx.fail(errorCode);

            addRequestsInProcess(-1, "http", httpEncoding(ctx));
            addHttpFailedRequests(1, errorCode, httpEncoding(ctx));

            otel.endCurrentSpan(
                    getOtelContext(ctx),
                    new RuntimeException("Collector has no subscribers"));

            return;
        }

        ctx.next();
    }

    /**
     * Parse an incoming HTTP request.
     *
     * @param ctx the request context
     */
    protected void parseHttpRequest(final RoutingContext ctx) {
        try (Scope s = getOtelContext(ctx).makeCurrent()) {
            Span parentSpan = Span.current();

            Span span = otel.startNewSpan("otel.collector.parse");
            span.setAttribute("otel.transport", "http");
            span.setAttribute("otel.encoding", httpEncoding(ctx));

            try {
                HttpServerRequest request = ctx.request();

                InputStream is = request.isEnded()
                        ? new ByteArrayInputStream(
                                request.body().result().getBytes())
                        : new VertxInputStream(
                                request,
                                parseContentLengthHeader(request));

                request.resume();

                setOtelRequest(ctx, isJsonRequest(ctx)
                        ? parseHttpJson(is)
                        : parseHttpProtobuf(is));

                ctx.next();
            } catch (Exception e) {
                LOG.log(Level.WARNING, """
                        Collector received unparsable message, returning \
                        error to client""",
                        e);

                otel.addErrorEvent(span,
                    new RuntimeException(
                        "Collector received unparsable message"));

                // return 'bad request' as we couldn't parse the data
                int errorCode = HTTP_BAD_REQUEST;

                ctx.response().setStatusMessage(e.getMessage());

                ctx.fail(errorCode);

                addRequestsInProcess(-1, "http", httpEncoding(ctx));
                addHttpFailedRequests(1, errorCode, httpEncoding(ctx));

                span.end();
                parentSpan.end();
            }
        }
    }

    /**
     * Publish an incoming HTTP request to subscribers.
     *
     * @param ctx the request's context
     * @param request the request to publish
     */
    protected void publish(final RoutingContext ctx, final REQ request) {
        CompletableFuture
            .supplyAsync(() -> publish(
                                    request,
                                    "http",
                                    httpEncoding(ctx),
                                    getOtelContext(ctx)))
            .orTimeout(DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS)
            .whenComplete((batch, err) -> {
                if (err == null) {
                    try {
                        setOtelResponse(ctx,
                                getBatchResponse(
                                        request,
                                        batch,
                                        "http",
                                        httpEncoding(ctx)));
                    } catch (Exception e) {
                        LOG.log(Level.SEVERE, """
                                Collector failed to build http response, \
                                returning service unavailable""",
                                e);

                        setErrorCode(ctx, HTTP_SERVICE_UNAVAILABLE);

                        otel.addErrorEvent(getOtelContext(ctx),
                            new RuntimeException(
                                "Collector failed to build http response"));
                    }
                } else if (err instanceof TimeoutException) {
                    LOG.log(Level.SEVERE, """
                            Collector failed to publish records within \
                            allowed time, returning service unavailable""",
                            err);

                    setErrorCode(ctx, HTTP_SERVICE_UNAVAILABLE);

                    otel.addErrorEvent(getOtelContext(ctx),
                        new RuntimeException("""
                            Collector failed to publish records within \
                            allowed time"""));
                } else {
                    LOG.log(Level.SEVERE, """
                            Collector failed to publish records, returning \
                            server error""",
                            err);

                    // FIXME: get some info from the exception if possible
                    setErrorCode(ctx, HTTP_INTERNAL_ERROR);

                    otel.addErrorEvent(getOtelContext(ctx),
                        new RuntimeException(
                            "Collector failed to publish records"));
                }

                ctx.next();
            });
    }

    /**
     * Send the HTTP response back to the client.
     *
     * @param ctx the routing context of this HTTP request
     */
    protected void respond(final RoutingContext ctx) {
        RESP response = getOtelResponse(ctx);

        if (isError(ctx)) {
            // return an error
            int errorCode = getErrorCode(ctx);

            ctx.fail(errorCode);

            addHttpFailedRequests(
                    1,
                    errorCode,
                    httpEncoding(ctx));

            otel.endCurrentSpan(getOtelContext(ctx),
                    new RuntimeException(
                            "Collector responded with error "
                                    + errorCode));
        } else {
            try {
                Buffer buf = Buffer.buffer(isJsonRequest(ctx)
                        ? responseToJson(response)
                                .getBytes(StandardCharsets.UTF_8)
                        : responseToProtobuf(response));

                ctx.response()
                    .setStatusCode(HTTP_OK)
                    .end(buf);

                otel.endCurrentSpan(getOtelContext(ctx), null);
            } catch (Exception e) {
                LOG.log(Level.SEVERE,
                        "Collector failed to send response to client",
                        e);

                // return 'internal server error'
                int errorCode = HTTP_INTERNAL_ERROR;

                ctx.response().setStatusMessage(e.getMessage());

                ctx.fail(errorCode);

                addHttpFailedRequests(
                        1,
                        errorCode,
                        httpEncoding(ctx));

                otel.endCurrentSpan(getOtelContext(ctx),
                        new RuntimeException(
                            "Collector failed to send response to client"));
            }
        }

        addRequestsInProcess(-1, "http", httpEncoding(ctx));
    }

    /**
     * Bind this collector to the given HTTP {@link io.vertx.ext.web.Router}
     * and start accepting requests.
     *
     * @param router the router
     */
    public void bind(final Router router) {
        LOG.log(Level.INFO,
                "Attaching collector at http path "
                + "\"" + httpPath + "\""
                + " with request timeout of "
                + DEFAULT_TIMEOUT_SEC
                + " seconds"
                + " and maximum body size of "
                + DEFAULT_MAX_BODY_LEN
                + " bytes");

        /*
         * FIXME: do we need to handle client disconnect before the request
         * is over? To cancel the Batch<> for example?
         */
        router
            .route(HttpMethod.POST, httpPath)
            .handler(this::checkContentType)
            .handler(this::checkBody)
            .handler(this::checkSubscribers)
            .blockingHandler(this::parseHttpRequest)
            .handler(ctx -> publish(ctx, getOtelRequest(ctx)))
            .handler(this::respond);
    }

    /**
     * Check if the HTTP request is JSON formatted.
     *
     * @param request the request
     * @return true if JSON was used by the client
     */
    protected boolean isJsonContent(final HttpServerRequest request) {
        return request == null
                ? false
                : "application/json"
                    .equals(request.getHeader("Content-Type"));
    }

    /**
     * Check if the HTTP request is protobuf formatted.
     *
     * @param request the request
     * @return true if protobuf was used by the client
     */
    protected boolean isProtobufContent(final HttpServerRequest request) {
        return request == null
                ? false
                : "application/x-protobuf"
                    .equals(request.getHeader("Content-Type"));
    }

    /**
     * Get the OTLP request object from a HTTP request.
     *
     * @param ctx the request context
     * @return the OTLP request object
     */
    @SuppressWarnings("unchecked")
    protected REQ getOtelRequest(final RoutingContext ctx) {
        return (REQ) ctx.get(CTX_REQUEST);
    }

    /**
     * Sets the OTLP request object within the routing context
     * for later use.
     *
     * @param ctx the routing context
     * @param request the OTLP request object
     */
    protected void setOtelRequest(
            final RoutingContext ctx,
            final REQ request) {
        ctx.put(CTX_REQUEST, request);
    }

    /**
     * Get the OTLP response object from the routing context.
     *
     * @param ctx the context
     * @return the response object
     */
    @SuppressWarnings("unchecked")
    protected RESP getOtelResponse(final RoutingContext ctx) {
        return (RESP) ctx.get(CTX_RESPONSE);
    }

    /**
     * Sets the OTLP response object within the routing context
     * for later use.
     *
     * @param ctx the routing context
     * @param response the OTLP response object
     */
    protected void setOtelResponse(
            final RoutingContext ctx,
            final RESP response) {
        ctx.put(CTX_RESPONSE, response);
    }

    /**
     * Check if the HTTP routing context was previously marked
     * as for a JSON-encoded OTLP request.
     *
     * @param ctx the routing context
     * @return true if OTLP request was JSON-encoded
     */
    protected boolean isJsonRequest(final RoutingContext ctx) {
        return (boolean) ctx.get(CTX_IS_JSON);
    }

    /**
     * Mark the HTTP routing context as handling a JSON-encoded request.
     *
     * @param ctx the routing context
     * @param isJson set to true to mark this request as JSON-encoded
     */
    protected void setJsonRequest(
            final RoutingContext ctx,
            final boolean isJson) {
        ctx.put(CTX_IS_JSON, isJson);
    }

    /**
     * Gets the encoding of a HTTP OTLP request.
     *
     * @param ctx the routing context
     * @return "json" or "protobuf"
     */
    protected String httpEncoding(final RoutingContext ctx) {
        return isJsonRequest(ctx)
                ? "json"
                : "protobuf";
    }

    /**
     * Checks if the processing of a HTTP request encountered an error.
     *
     * @param ctx the routing context
     * @return true if an error was encountered
     */
    protected boolean isError(final RoutingContext ctx) {
        Boolean res = ctx.get(CTX_IS_ERROR);

        return res != null && res;
    }

    /**
     * Sets an error in a HTTP routing context so that a proper
     * response can be returned to the client.
     *
     * @param ctx the routing context
     * @param errorCode the error code
     */
    protected void setErrorCode(
            final RoutingContext ctx,
            final int errorCode) {
        ctx.put(CTX_IS_ERROR, true);
        ctx.put(CTX_ERROR_CODE, errorCode);
    }

    /**
     * Returns the previously set error code for the given HTTP request.
     *
     * @param ctx the routing context
     * @return the error code previously set
     */
    protected int getErrorCode(final RoutingContext ctx) {
        return (int) ctx.get(CTX_ERROR_CODE);
    }

    /**
     * Configure the telemetry context to be used when processing
     * this HTTP request.
     *
     * @param ctx the HTTP routing context
     * @param otelContext the telemetry context to be set
     */
    protected void setOtelContext(
            final RoutingContext ctx,
            final Context otelContext) {
        ctx.put(CTX_OTEL_CONTEXT, otelContext);
    }

    /**
     * Get the telemetry context previously associated with this
     * HTTP routing context.
     *
     * @param ctx the routing context
     * @return the associated telemetry context
     */
    protected Context getOtelContext(final RoutingContext ctx) {
        return ctx.get(CTX_OTEL_CONTEXT);
    }

    /**
     * Parses the Content-Length header of an incoming HTTP request.
     *
     * @param request the HTTP request
     * @return the parsed value or -1 if header is missing or unparseable
     */
    protected long parseContentLengthHeader(
            final HttpServerRequest request) {
        // taken from vertx BodyHandlerImpl
        String contentLength = request.getHeader(HttpHeaders.CONTENT_LENGTH);

        if (contentLength == null || contentLength.isEmpty()) {
            return -1;
        }

        try {
            long parsedContentLength = Long.parseLong(contentLength);
            return parsedContentLength < 0 ? -1 : parsedContentLength;
        } catch (NumberFormatException ex) {
            return -1;
        }
    }

    /**
     * Updates the number of successful requests counter.
     *
     * @param count a count of how many to add
     * @param transport the OTLP transport used
     * @param encoding the OTLP transport encoding of the requests
     */
    protected void addSucceededRequests(
            final long count,
            final String transport,
            final String encoding) {
        succeededRequests.add(count,
                Attributes.builder()
                    .put(AttributeKey.stringKey("transport"), transport)
                    .put(AttributeKey.stringKey("encoding"), encoding)
                    .build());
    }

    /**
     * Updates the number of partially successful requests counter.
     *
     * @param count a count of how many to add
     * @param transport the OTLP transport used
     * @param encoding the OTLP transport encoding of the requests
     */
    protected void addPartiallySucceededRequests(
            final long count,
            final String transport,
            final String encoding) {
        partiallySucceededRequests.add(count,
                Attributes.builder()
                    .put(AttributeKey.stringKey("transport"), transport)
                    .put(AttributeKey.stringKey("encoding"), encoding)
                    .build());
    }

    /**
     * Check if a given gRPC error can be retried by the client.
     *
     * @param status the gRPC error code
     * @return true if error is retriable
     */
    protected boolean isRetryable(final GrpcStatus status) {
        /*
         * https://github.com/open-telemetry/opentelemetry-proto/
         * blob/main/docs/specification.md#failures
         */
        switch (status) {
        case ABORTED:
        case CANCELLED:
        case DATA_LOSS:
        case DEADLINE_EXCEEDED:
        case OUT_OF_RANGE:
        case RESOURCE_EXHAUSTED:
        case UNAVAILABLE:
            return true;

        default:
            return false;
        }
    }

    /**
     * Check if a given HTTP error can be retried by the client.
     *
     * @param httpResponseStatusCode the HTTP error code
     * @return true if error is retriable
     */
    protected boolean isRetryable(final int httpResponseStatusCode) {
        /*
         * https://github.com/open-telemetry/opentelemetry-proto/
         * blob/main/docs/specification.md#failures-1
         */
        switch (httpResponseStatusCode) {
        case HTTP_TOO_MANY_REQUESTS:
        case HTTP_BAD_GATEWAY:
        case HTTP_SERVICE_UNAVAILABLE:
        case HTTP_GATEWAY_TIMEOUT:
            return true;

        default:
            return false;
        }
    }

    /**
     * Update the number of failed requests counter for a given
     * gRPC error code.
     *
     * @param count the count to add
     * @param status the gRPC error code
     */
    protected void addGrpcFailedRequests(
            final long count,
            final GrpcStatus status) {
        addFailedRequests(count, "grpc", "protobuf", isRetryable(status));
    }

    /**
     * Updates the number of failed requests counter.
     *
     * @param count a count of how many to add
     * @param transport the OTLP transport used
     * @param encoding the OTLP transport encoding of the requests
     * @param retryable true if the error allows the client to retry
     */
    protected void addFailedRequests(
            final long count,
            final String transport,
            final String encoding,
            final boolean retryable) {
        AttributesBuilder attrBuilder = Attributes.builder()
                .put(AttributeKey.stringKey("transport"), transport)
                .put(AttributeKey.booleanKey("retryable"), retryable);

        if (encoding != null) {
            attrBuilder.put(AttributeKey.stringKey("encoding"), encoding);
        }

        failedRequests.add(count, attrBuilder.build());
    }

    /**
     * Update the number of failed requests counter for a given
     * HTTP error code.
     *
     * @param count the count to add
     * @param httpResponseStatusCode the HTTP error code
     * @param encoding the OTLP encoding used in the requests
     */
    protected void addHttpFailedRequests(
            final long count,
            final int httpResponseStatusCode,
            final String encoding) {
        if (httpResponseStatusCode >= HTTP_BAD_REQUEST) {
            addFailedRequests(count,
                    "http",
                    encoding,
                    isRetryable(httpResponseStatusCode));
        }
    }

    /**
     * Update the counter of number of requests currently being processed.
     *
     * @param count the count of requests to add or subtract (if negative)
     * @param transport the transport used by the requests
     * @param encoding the transport encoding
     */
    protected void addRequestsInProcess(
            final long count,
            final String transport,
            final String encoding) {
        requestsInProcess.add(count,
                Attributes.builder()
                    .put(AttributeKey.stringKey("transport"), transport)
                    .put(AttributeKey.stringKey("encoding"), encoding)
                    .build());
    }

    /**
     * Update the counter of how many OTLP items (logs, spans, metrics)
     * have been processed so far.
     *
     * @param count the number of items to add
     * @param transport the OTLP transport used
     * @param encoding the OTLP encoding used
     */
    protected void addRequestItems(
            final long count,
            final String transport,
            final String encoding) {
        requestItems.add(count,
                Attributes.builder()
                    .put(AttributeKey.stringKey("transport"), transport)
                    .put(AttributeKey.stringKey("encoding"), encoding)
                    .build());
    }

    /**
     * Update the counter of how many OTLP items (logs, spans, metrics)
     * have been dropped so far.
     *
     * @param count the number of items to add
     * @param transport the OTLP transport used
     * @param encoding the OTLP encoding used
     */
    protected void addDroppedRequestItems(
            final long count,
            final String transport,
            final String encoding) {
        droppedRequestItems.add(count,
                Attributes.builder()
                    .put(AttributeKey.stringKey("transport"), transport)
                    .put(AttributeKey.stringKey("encoding"), encoding)
                    .build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        super.close();

        try {
            CompletableFuture.runAsync(() -> {
                while (executor.hasQueuedSubmissions()
                        || executor.getActiveThreadCount() > 0) {
                    try {
                        Thread.sleep(DEFAULT_CLOSE_SLEEP_MS);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            })
            // wait for all subscribers to complete
            .get(DEFAULT_CLOSE_TIMETOUT_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException
                | ExecutionException
                | TimeoutException e) {
            LOG.log(Level.SEVERE,
                    "Failed to close "
                            + telemetrySignalType()
                            + " collector",
                    e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void closeExceptionally(final Throwable error) {
        super.closeExceptionally(error);

        try {
            CompletableFuture.runAsync(() -> {
                while (executor.hasQueuedSubmissions()
                        || executor.getActiveThreadCount() > 0) {
                    try {
                        Thread.sleep(DEFAULT_CLOSE_SLEEP_MS);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            })
            // wait for all subscribers to complete
            .get(DEFAULT_CLOSE_TIMETOUT_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException
                | ExecutionException
                | TimeoutException e) {
            LOG.log(Level.SEVERE,
                    "Failed to close exceptionally "
                            + telemetrySignalType()
                            + " collector",
                    e);
        }
    }
}
