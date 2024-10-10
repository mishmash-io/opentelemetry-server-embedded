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

package io.mishmash.opentelemetry.druid.format;

import java.io.File;
import java.util.Objects;

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The main class of the OTLP Apache Druid OTLP {@link InputFormat}.
 */
public class OTLPInputFormat implements InputFormat {

    /**
     * Specifies the format of the input data.
     */
    public enum OTLPSignalType {
        /**
         * OTLP 'raw' logs.
         */
        @JsonProperty("logsRaw")
        LOGS_RAW,
        /**
         * OTLP 'flattened' log.
         */
        @JsonProperty("logsFlat")
        LOGS_FLAT,
        /**
         * OTLP 'raw' metrics.
         */
        @JsonProperty("metricsRaw")
        METRICS_RAW,
        /**
         * OTLP 'flattened' metric data point.
         */
        @JsonProperty("metricsFlat")
        METRICS_FLAT,
        /**
         * OTLP 'raw' traces.
         */
        @JsonProperty("tracesRaw")
        TRACES_RAW,
        /**
         * OTLP 'flattened' trace span.
         */
        @JsonProperty("tracesFlat")
        TRACES_FLAT,
        /**
         * OTLP 'raw' profiles.
         */
        @JsonProperty("profilesRaw")
        PROFILES_RAW,
        /**
         * OTLP 'flattened' profile sample value.
         */
        @JsonProperty("profilesFlat")
        PROFILES_FLAT
    }

    /**
     * The configured input signal data type.
     */
    private OTLPSignalType inputSignal;

    /**
     * Create a new {@link OTLPInputFormat} for a given
     * signal type ({@link OTLPSignalType}).
     *
     * @param otlpSignalType the type of input data to be used
     */
    @JsonCreator
    public OTLPInputFormat(
            @JsonProperty("otlpInputSignal")
                final OTLPSignalType otlpSignalType) {
        this.inputSignal = otlpSignalType;
    }

    /**
     * Get the configured {@link OTLPSignalType}.
     *
     * @return the type used by this {@link OTLPInputFormat}
     */
    @JsonProperty
    public OTLPSignalType getOtlpInputSignal() {
        return inputSignal;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSplittable() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputEntityReader createReader(
            final InputRowSchema inputRowSchema,
            final InputEntity source,
            final File temporaryDirectory) {
        switch (getOtlpInputSignal()) {
        case LOGS_FLAT:
            return new LogsReader(inputRowSchema, source, false);
        case LOGS_RAW:
            return new LogsReader(inputRowSchema, source, true);
        case METRICS_FLAT:
            return new MetricsReader(inputRowSchema, source, false);
        case METRICS_RAW:
            return new MetricsReader(inputRowSchema, source, true);
        case TRACES_FLAT:
            return new TracesReader(inputRowSchema, source, false);
        case TRACES_RAW:
            return new TracesReader(inputRowSchema, source, true);
        case PROFILES_FLAT:
            return new ProfilesReader(inputRowSchema, source, false);
        case PROFILES_RAW:
            return new ProfilesReader(inputRowSchema, source, true);
        default:
            // should not happen
            throw new UnsupportedOperationException("Internal error");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(getOtlpInputSignal());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        OTLPInputFormat o = (OTLPInputFormat) obj;

        return Objects.equals(getOtlpInputSignal(), o.getOtlpInputSignal());
    }
}
