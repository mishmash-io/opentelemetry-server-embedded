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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.jupiter.api.Test;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

public class MetricsFlattenerTests {

    @Test
    void emptyMetrics() {
        // test with completely empty request
        MetricsFlattener flattener = new MetricsFlattener(
                null,
                null,
                emptyRequest().build(),
                null);

        assertFalse(flattener.iterator().hasNext());

        // test with a request with an empty ResourceLogs
        flattener = new MetricsFlattener(
                null,
                null,
                emptyResourceMetrics()
                    .build(),
                null);

        assertFalse(flattener.iterator().hasNext());

        // test with a request with an empty ScopeLogs
        flattener = new MetricsFlattener(
                null,
                null,
                emptyScopeMetrics()
                    .build(),
                null);

        assertFalse(flattener.iterator().hasNext());

        // test with a request with an empty metric (no data points)
        flattener = new MetricsFlattener(
                null,
                null,
                emptyMetricDataPoints()
                    .build(),
                null);

        assertFalse(flattener.iterator().hasNext());
    }

    @Test
    void singleMetric() {
        // test with a single metric data point
        MetricsFlattener flattener = new MetricsFlattener(
                null,
                null,
                createMetricsRequest(
                        null,
                        null,
                        gauge(3))
                    .build(),
                null);
        Iterator<MetricDataPoint> it = flattener.iterator();

        assertTrue(it.hasNext());
        assertNotNull(it.next());
        assertFalse(it.hasNext());

        // test with a single gauge and then an empty resource
        flattener = new MetricsFlattener(
                null,
                null,
                addEmptyResourceMetrics(
                    createMetricsRequest(
                            null,
                            null,
                            gauge(3))
                ).build(),
                null);
        it = flattener.iterator();

        assertTrue(it.hasNext());
        assertNotNull(it.next());
        assertFalse(it.hasNext());

        // test with a single gauge and then an empty scope
        flattener = new MetricsFlattener(
                null,
                null,
                addEmptyScopeMetrics(
                    createMetricsRequest(
                            null,
                            null,
                            gauge(3))
                ).build(),
                null);
        it = flattener.iterator();

        assertTrue(it.hasNext());
        assertNotNull(it.next());
        assertFalse(it.hasNext());

        // test with a single gauge and then an empty resource and then an empty scope
        flattener = new MetricsFlattener(
                null,
                null,
                addEmptyScopeMetrics(
                    addEmptyResourceMetrics(
                        createMetricsRequest(
                                null,
                                null,
                                gauge(3)))
                ).build(),
                null);
        it = flattener.iterator();

        assertTrue(it.hasNext());
        assertNotNull(it.next());
        assertFalse(it.hasNext());

    }

    private ExportMetricsServiceRequest.Builder emptyRequest() {
        return createMetricsRequest();
    }

    private ExportMetricsServiceRequest.Builder emptyResourceMetrics() {
        return addEmptyResourceMetrics(createMetricsRequest());
    }

    private ExportMetricsServiceRequest.Builder addEmptyResourceMetrics(
            ExportMetricsServiceRequest.Builder builder) {
        return builder.addResourceMetrics(createResourceMetrics(null, null));
    }

    private ExportMetricsServiceRequest.Builder emptyScopeMetrics() {
        return addEmptyScopeMetrics(createMetricsRequest());
    }

    private ExportMetricsServiceRequest.Builder addEmptyScopeMetrics(
            ExportMetricsServiceRequest.Builder builder) {
        return builder.addResourceMetrics(
                addEmptyScopeMetrics(createResourceMetrics(null, null)));
    }

    private ResourceMetrics.Builder addEmptyScopeMetrics(ResourceMetrics.Builder builder) {
        return builder.addScopeMetrics(ScopeMetrics.newBuilder());
    }

    private ExportMetricsServiceRequest.Builder emptyMetricDataPoints() {
        return addEmptyMetricDataPoints(createMetricsRequest());
    }

    private ExportMetricsServiceRequest.Builder addEmptyMetricDataPoints(
            ExportMetricsServiceRequest.Builder builder) {
        return builder.addResourceMetrics(
                createResourceMetrics(null, null)
                    .addScopeMetrics(
                            addMetrics(null, emptyMetric())));
    }

    private Metric.Builder emptyMetric() {
        return Metric.newBuilder().setName("empty-metric");
    }

    private Metric.Builder gauge(long value) {
        return Metric.newBuilder()
                .setName("test-gauge")
                .setGauge(Gauge.newBuilder()
                        .addDataPoints(NumberDataPoint
                                .newBuilder()
                                .setAsInt(value)));
    }

    private ResourceMetrics.Builder createResourceMetrics(
            String schemaUrl,
            Integer droppedAttributesCnt,
            KeyValue.Builder...attributes) {
        ResourceMetrics.Builder res = ResourceMetrics.newBuilder();

        if (schemaUrl != null) {
            res = res.setSchemaUrl(schemaUrl);
        }

        if (attributes == null && droppedAttributesCnt == null) {
            return res;
        }

        Resource.Builder rb = Resource.newBuilder();

        if (droppedAttributesCnt != null) {
            rb = rb.setDroppedAttributesCount(droppedAttributesCnt);
        }

        if (attributes != null) {
            rb = rb.addAllAttributes(
                    Arrays.stream(attributes)
                        .map(a -> a.build())
                        .toList());
        }

        return res.setResource(rb);
    }

    private ExportMetricsServiceRequest.Builder createMetricsRequest() {
        return ExportMetricsServiceRequest.newBuilder();
    }

    private ExportMetricsServiceRequest.Builder createMetricsRequest(
            ResourceMetrics.Builder resource,
            ScopeMetrics.Builder scope,
            Metric.Builder...metrics) {
        if (resource == null) {
            resource = createResourceMetrics(null, null);
        }

        return createMetricsRequest()
                .addResourceMetrics(resource
                        .addScopeMetrics(
                                addMetrics(scope, metrics
                        )));
    }

    private ScopeMetrics.Builder addMetrics(
            ScopeMetrics.Builder scope,
            Metric.Builder...metrics) {
        if (scope == null) {
            scope = ScopeMetrics.newBuilder();
        }

        return scope.addAllMetrics(Arrays.stream(metrics)
            .map(l -> l.build())
            .toList());
    }
}
