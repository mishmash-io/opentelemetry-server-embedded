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

import io.opentelemetry.context.Context;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;

/**
 * Holds the details of an individual OTLP log record
 * received from a client as a {@link Batch} of all subscribers
 * that are expected to process it.
 */
public class Log extends SubscribersBatch<Log> {

    /**
     * Timestamp when the client's OTLP packet was received (in ms).
     */
    private long batchTimestamp;
    /**
     * A unique id of the client's OTLP packet.
     */
    private String batchUUID;
    /**
     * The sequence number of this Log within the client's
     * OTLP packet (zero-based).
     */
    private int seqNo;
    /**
     * The OTLP Resource details.
     */
    private Resource resource;
    /**
     * The Schema URL of the OTLP Resource.
     */
    private String resourceSchemaUrl;
    /**
     * The OTLP Scope details.
     */
    private InstrumentationScope scope;
    /**
     * The OTLP Log record.
     */
    private LogRecord log;
    /**
     * The Schema URL of the OTLP Log record.
     */
    private String logSchemaUrl;

    /**
     * If this record is valid.
     */
    private boolean isValid;
    /**
     * An error message to be sent back if this packet has an error.
     */
    private String errorMessage;

    /**
     * Create a new empty log.
     *
     * @param parent parent {@link Batch}
     * @param otelContext {@link io.opentelemetry.context.Context} for own
     * telemetry
     */
    public Log(final Batch<Log> parent, final Context otelContext) {
        super(parent, otelContext);
    }

    /**
     * Set the details of this log. Also sets the validity of this record
     * and potentially an error message.
     *
     * @param batchTS the batch timestamp
     * @param batchID the batch id
     * @param sequenceNum the sequence number of this log
     * @param resourceLog used to fill-in OpenTelemetry Resource details
     * @param scopeLog used to fill-in OpenTelemetry Scope details
     * @param logRec the log record details
     */
    public void setFrom(
            final long batchTS,
            final String batchID,
            final int sequenceNum,
            final ResourceLogs resourceLog,
            final ScopeLogs scopeLog,
            final LogRecord logRec) {
        this.batchTimestamp = batchTS;
        this.batchUUID = batchID;
        this.seqNo = sequenceNum;
        this.resource = resourceLog.getResource();
        this.resourceSchemaUrl = resourceLog.getSchemaUrl();
        this.scope = scopeLog.getScope();
        this.log = logRec;
        this.logSchemaUrl = scopeLog.getSchemaUrl();

        // FIXME: add checks for validity and set isValid, errorMessage?
        isValid = true;
    }

    /**
     * Get the timestamp when the containing OTLP packet was received.
     *
     * @return the timestamp, in ms
     */
    public long getBatchTimestamp() {
        return batchTimestamp;
    }

    /**
     * Get the unique id of the containing OTLP packet.
     *
     * @return the UUID
     */
    public String getBatchUUID() {
        return batchUUID;
    }

    /**
     * Get the sequence number of this log within the containing
     * OTLP packet.
     *
     * @return the sequence number, zero-based
     */
    public int getSeqNo() {
        return seqNo;
    }

    /**
     * Get the OpenTelemetry Resource of this log.
     *
     * @return the resource details
     */
    public Resource getResource() {
        return resource;
    }

    /**
     * Get the schema URL of the OpenTelemetry Resouce of this log.
     *
     * @return the schema URL
     */
    public String getResourceSchemaUrl() {
        return resourceSchemaUrl;
    }

    /**
     * Get the OpenTelemetry Scope of this log.
     *
     * @return the scope details
     */
    public InstrumentationScope getScope() {
        return scope;
    }

    /**
     * Get the OpenTelemetry Log record.
     *
     * @return the log record details
     */
    public LogRecord getLog() {
        return log;
    }

    /**
     * Get the schema URL of the OpenTelemetry Log.
     *
     * @return the schema URL
     */
    public String getLogSchemaUrl() {
        return logSchemaUrl;
    }

    /**
     * Check if this record is valid.
     *
     * @return true if data is valid
     */
    public boolean isValid() {
        return isValid;
    }

    /**
     * Get the message associated with an error in the data.
     *
     * @return the error message or null if data is valid
     */
    public String getErrorMessage() {
        return errorMessage;
    }
}
