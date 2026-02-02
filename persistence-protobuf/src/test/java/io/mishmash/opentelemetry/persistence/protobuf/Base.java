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

package io.mishmash.opentelemetry.persistence.protobuf;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.resource.v1.Resource;

public class Base {

    private static final long BATCH_TIMESTAMP = 1;
    private static final String BATCH_UUID = "test batch uuid";

    private static final String RESOURCE_SCHEMA_URL = "http://resource.schema.test";
    private static final int RESOURCE_DROPPED_ATTRIBUTES_COUNT = 10;
    private static final String RESOURCE_ATTR_KEY = "resource-attribute";
    private static final String RESOURCE_ATTR_VAL = "test RA";
    private static final String SCOPE_ATTR_KEY = "scope-attribute";
    private static final int SCOPE_ATTR_VAL = 3;
    private static final int SCOPE_DROPPED_ATTRIBUTES_COUNT = 11;
    private static final String SCOPE_NAME = "test-scope-name";
    private static final String SCOPE_VERSION = "1.0.0-test";

    public long batchTimestamp() {
        return BATCH_TIMESTAMP;
    }

    public void testBatchTimestamp(long actual) {
        testBatchTimestamp(batchTimestamp(), actual);
    }

    public void testBatchTimestamp(long expected, long actual) {
        assertEquals(expected, actual);
    }

    public String batchUUID() {
        return BATCH_UUID;
    }

    public void testBatchUUID(String actual) {
        testBatchUUID(batchUUID(), actual);
    }

    public void testBatchUUID(String expected, String actual) {
        assertEquals(expected, actual);
    }

    public String resourceSchemaUrl() {
        return RESOURCE_SCHEMA_URL;
    }

    public void testResourceSchemaUrl(String actual) {
        testResourceSchemaUrl(resourceSchemaUrl(), actual);
    }

    public void testResourceSchemaUrl(String expected, String actual) {
        assertEquals(expected, actual);
    }

    public Integer resourceDroppedAttributesCount() {
        return RESOURCE_DROPPED_ATTRIBUTES_COUNT;
    }

    public void testResourceDroppedAttributesCount(Integer actual) {
        testResourceDroppedAttributesCount(
                resourceDroppedAttributesCount(),
                actual);
    }

    public void testResourceDroppedAttributesCount(
            Integer expected,
            Integer actual) {
        assertEquals(expected, actual);
    }

    public Resource resource() {
        return resource(
                resourceDroppedAttributesCount(),
                resourceAttributes());
    }

    public Resource resource(
            Integer droppedAttrsCount,
            Iterable<KeyValue> attrs) {
        Resource.Builder builder = Resource.newBuilder();

        if (droppedAttrsCount != null) {
            builder = builder.setDroppedAttributesCount(droppedAttrsCount);
        }

        if (attrs != null) {
            builder = builder.addAllAttributes(attrs);
        }

        return builder.build();
    }

    public void testResource(Resource actual) {
        testResource(resource(), actual);
    }

    public void testResource(Resource expected, Resource actual) {
        assertEquals(
                expected.getDroppedAttributesCount(),
                actual.getDroppedAttributesCount());
        testAttributes(
                expected.getAttributesList(),
                actual.getAttributesList());
    }

    public void testResourceAttributes(Iterable<KeyValue> resourceAttributes) {
        testAttributes(
                resourceAttributes(),
                resourceAttributes);
    }

    public Iterable<KeyValue> resourceAttributes() {
        return attributes(
                Collections.singleton(
                        Map.entry(
                                RESOURCE_ATTR_KEY,
                                RESOURCE_ATTR_VAL)));
    }

    public Iterable<KeyValue> attributes(
            Collection<Map.Entry<String, Object>> a) {
        if (a == null) {
            return null;
        }

        List<KeyValue> res = new ArrayList<>(a.size());
        for (Map.Entry<String, Object> ent : a) {
            res.add(keyValue(ent.getKey(), ent.getValue()));
        }

        return res;
    }

    public void testAttributes(
            Iterable<KeyValue> expected,
            Iterable<KeyValue> actual) {
        assertIterableEquals(expected, actual);
    }

    public String scopeName() {
        return SCOPE_NAME;
    }

    public void testScopeName(String actual) {
        testScopeName(scopeName(), actual);
    }

    public void testScopeName(String expected, String actual) {
        assertEquals(expected, actual);
    }

    public String scopeVersion() {
        return SCOPE_VERSION;
    }

    public void testScopeVersion(String actual) {
        testScopeVersion(scopeVersion(), actual);
    }

    public void testScopeVersion(String expected, String actual) {
        assertEquals(expected, actual);
    }

    public Integer scopeDroppedAttributesCount() {
        return SCOPE_DROPPED_ATTRIBUTES_COUNT;
    }

    public void testScopeDroppedAttributesCount(Integer actual) {
        testScopeDroppedAttributesCount(scopeDroppedAttributesCount(), actual);
    }

    public void testScopeDroppedAttributesCount(
            Integer expected,
            Integer actual) {
        assertEquals(expected, actual);
    }

    public Iterable<KeyValue> scopeAttributes() {
        return attributes(
                Collections.singleton(
                        Map.entry(
                                SCOPE_ATTR_KEY,
                                SCOPE_ATTR_VAL)));
    }

    public InstrumentationScope scope() {
        return scope(scopeName(),
                scopeVersion(),
                scopeDroppedAttributesCount(),
                scopeAttributes());
    }

    public InstrumentationScope scope(
            String name,
            String version,
            Integer droppedAttrsCount,
            Iterable<KeyValue> attrs) {
        InstrumentationScope.Builder builder = InstrumentationScope
                .newBuilder();

        if (name != null) {
            builder = builder.setName(name);
        }

        if (version != null) {
            builder = builder.setVersion(version);
        }

        if (droppedAttrsCount != null) {
            builder = builder.setDroppedAttributesCount(droppedAttrsCount);
        }

        if (attrs != null) {
            builder = builder.addAllAttributes(attrs);
        }

        return builder.build();
    }

    public void testScope(InstrumentationScope actual) {
        testScope(scope(), actual);
    }

    public void testScope(
            InstrumentationScope expected,
            InstrumentationScope actual) {
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getVersion(), actual.getVersion());
        assertEquals(
                expected.getDroppedAttributesCount(),
                actual.getDroppedAttributesCount());
        testAttributes(
                expected.getAttributesList(),
                actual.getAttributesList());
    }

    public KeyValue keyValue(String key, Object value) {
        KeyValue.Builder builder = KeyValue.newBuilder()
                .setKey(key);

        if (value != null) {
            builder = builder.setValue(anyValue(value));
        }

        return builder.build();
    }

    public AnyValue anyValue(String valueType, Object value) {
        return anyValue(AnyValue.ValueCase.valueOf(valueType), value);
    }

    @SuppressWarnings("unchecked")
    public AnyValue anyValue(AnyValue.ValueCase vCase, Object value) {
        AnyValue.Builder builder = AnyValue.newBuilder();

        switch (vCase) {
        case ARRAY_VALUE:
            builder = builder.setArrayValue(
                    arrayValue((Collection<Object>) value));
            break;
        case BOOL_VALUE:
            builder = builder.setBoolValue((boolean) value);
            break;
        case BYTES_VALUE:
            builder = builder.setBytesValue(
                    ByteString.copyFrom((byte[]) value));
            break;
        case DOUBLE_VALUE:
            builder = builder.setDoubleValue((double) value);
            break;
        case INT_VALUE:
            builder = builder.setIntValue((long) value);
            break;
        case KVLIST_VALUE:
            builder = builder.setKvlistValue(
                    keyValueList(
                            (Collection<Map.Entry<String, Object>>) value));
            break;
        case STRING_VALUE:
            builder = builder.setStringValue((String) value);
            break;
        case VALUE_NOT_SET:
            break;
        default:
            throw new IllegalArgumentException("Unknown AnyValue case");
        }

        return builder.build();
    }

    public AnyValue anyValue(Object o) {
        AnyValue.Builder builder = AnyValue.newBuilder();

        if (o == null) {
            return builder.build();
        }

        if (o instanceof String s) {
            builder = builder.setStringValue(s);
        } else if (o instanceof Boolean b) {
            builder = builder.setBoolValue(b);
        } else if (o instanceof Long l) {
            builder = builder.setIntValue(l);
        } else if (o instanceof Integer i) {
            builder = builder.setIntValue(Long.valueOf(i));
        } else if (o instanceof Double d) {
            builder = builder.setDoubleValue(d);
        } else if (o instanceof Float f) {
            builder = builder.setDoubleValue(Double.valueOf(f));
        } else if (o instanceof Map m) {
            builder = builder.setKvlistValue(keyValueList(m.entrySet()));
        } else if (o instanceof byte[] bytes) {
            builder = builder.setBytesValue(ByteString.copyFrom(bytes));
        } else if (o instanceof ByteString byteString) {
            builder = builder.setBytesValue(byteString);
        } else {
            throw new IllegalArgumentException("Unknown AnyValue object type");
        }

        return builder.build();
    }

    public void testAnyValue(AnyValue expected, AnyValue actual) {
        assertEquals(expected.getValueCase(), actual.getValueCase());

        switch (expected.getValueCase()) {
        case STRING_VALUE:
            assertEquals(
                    expected.getStringValue(),
                    actual.getStringValue());
            break;
        case BOOL_VALUE:
            assertEquals(
                    expected.getBoolValue(),
                    actual.getBoolValue());
            break;
        case INT_VALUE:
            assertEquals(
                    expected.getIntValue(),
                    actual.getIntValue());
            break;
        case DOUBLE_VALUE:
            assertEquals(
                    expected.getDoubleValue(),
                    actual.getDoubleValue());
            break;
        case ARRAY_VALUE:
            testArrayValue(
                    expected.getArrayValue(),
                    actual.getArrayValue());
            break;
        case KVLIST_VALUE:
            testKeyValueList(
                    expected.getKvlistValue(),
                    actual.getKvlistValue());
            break;
        case BYTES_VALUE:
            assertArrayEquals(
                    expected.getBytesValue().toByteArray(),
                    actual.getBytesValue().toByteArray());
            break;
        case VALUE_NOT_SET:
            break;
        default:
        }
    }

    public ArrayValue arrayValue(Collection<Object> list) {
        if (list == null) {
            return null;
        }

        ArrayValue.Builder builder = ArrayValue.newBuilder();
        for (Object o : list) {
            builder = builder.addValues(anyValue(o));
        }

        return builder.build();
    }

    public void testArrayValue(ArrayValue expected, ArrayValue actual) {
        assertEquals(
                expected.getValuesCount(),
                actual.getValuesCount());

        for (int i = 0; i < expected.getValuesCount(); i++) {
            testAnyValue(
                    expected.getValues(i),
                    actual.getValues(i));
        }
    }

    public KeyValueList keyValueList(Collection<Map.Entry<String, Object>> kv) {
        if (kv == null) {
            return null;
        }

        KeyValueList.Builder builder = KeyValueList.newBuilder();
        for (Map.Entry<String, Object> ent : kv) {
            builder = builder.addValues(keyValue(ent.getKey(), ent.getValue()));
        }

        return builder.build();
    }

    public void testKeyValueList(KeyValueList expected, KeyValueList actual) {
        
    }
}
