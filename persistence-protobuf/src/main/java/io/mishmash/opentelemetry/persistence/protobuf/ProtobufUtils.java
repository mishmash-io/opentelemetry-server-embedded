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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

/**
 * Various helper methods for general protobuf message handling.
 */
public final class ProtobufUtils {

    /**
     * Custom converters for some protobuf message types.
     *
     * Helps with avoiding unnecessary nesting.
     */
    private static final Map<String, Function<Message, Object>> CONVERTERS =
            new HashMap<>();

    static {
        CONVERTERS.put(
                BoolValue.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        CONVERTERS.put(
                Int32Value.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        CONVERTERS.put(
                UInt32Value.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        CONVERTERS.put(
                Int64Value.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        CONVERTERS.put(
                UInt64Value.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        CONVERTERS.put(
                StringValue.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        CONVERTERS.put(
                BytesValue.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        CONVERTERS.put(
                FloatValue.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        CONVERTERS.put(
                DoubleValue.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);

    }

    private ProtobufUtils() {
        // constructor is hidden
    }

    /**
     * Convert a {@link Map} of fields into a JSON-friendly map.
     *
     * Use to convert all fields of a {@link Message}.
     *
     * @param entries the protobuf fields
     * @return a JSON-friendly {@link Map}
     */
    public static Map<String, Object> toJsonMap(
            final Map<FieldDescriptor, Object> entries) {
        Map<String, Object> res = new HashMap<>(entries.size());

        for (Map.Entry<FieldDescriptor, Object> ent : entries.entrySet()) {
            res.put(ent.getKey().getName(),
                    toJsonValue(ent.getKey(), ent.getValue()));
        }

        return res;
    }

    private static Object toJsonValue(
            final FieldDescriptor field,
            final Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Message) {
            Message msg = (Message) value;
            Function<Message, Object> converter =
                    CONVERTERS.get(msg.getDescriptorForType().getFullName());

            if (converter != null) {
                return converter.apply(msg);
            }
        }

        if (field.isMapField()) {
            FieldDescriptor k = field
                                    .getMessageType()
                                    .findFieldByName("key");
            FieldDescriptor v = field
                                    .getMessageType()
                                    .findFieldByName("value");

            if (k == null || v == null) {
                throw new IllegalArgumentException(
                        String.format("Malformed protobuf field '%s'",
                                field.getFullName()));
            }

            List<?> elementsList = (List<?>) value;
            Map<String, Object> resMap = new HashMap<>(elementsList.size());

            for (Object element : elementsList) {
                resMap.put(
                        (String) toJsonValueSingle(
                                k,
                                ((Message) element).getField(k)),
                        toJsonValueSingle(
                                v,
                                ((Message) element).getField(v)));
            }

            return resMap;
        } else if (field.isRepeated()) {
            if (FieldDescriptor.Type.MESSAGE.equals(field.getType())
                    && "opentelemetry.proto.common.v1.KeyValue".equals(
                            field.getMessageType().getFullName())) {
                @SuppressWarnings("unchecked")
                List<KeyValue> kvs = (List<KeyValue>) value;

                return toJsonValue(kvs);
            } else {
                List<?> valuesList = (List<?>) value;
                List<Object> resList = new ArrayList<>(valuesList.size());

                for (Object o : valuesList) {
                    resList.add(toJsonValueSingle(field, o));
                }

                return resList;
            }
        } else {
            return toJsonValueSingle(field, value);
        }
    }

    private static Object toJsonValue(final List<KeyValue> kvs) {
        Map<String, Object> res = new HashMap<>(kvs.size());

        for (KeyValue kv : kvs) {
            res.put(
                    kv.getKey(),
                    toJsonValue(kv.getValue()));
        }

        return res;
    }

    private static Object toJsonValue(final AnyValue av) {
        switch (av.getValueCase()) {
        case ARRAY_VALUE:
            List<Object> res = new ArrayList<>(
                    av.getArrayValue().getValuesCount());
            for (AnyValue a : av.getArrayValue().getValuesList()) {
                res.add(toJsonValue(a));
            }

            return res;
        case BOOL_VALUE:
            return av.getBoolValue();
        case BYTES_VALUE:
            return av.getBytesValue().toByteArray();
        case DOUBLE_VALUE:
            return av.getDoubleValue();
        case INT_VALUE:
            return av.getIntValue();
        case KVLIST_VALUE:
            return toJsonValue(av.getKvlistValue().getValuesList());
        case STRING_VALUE:
            return av.getStringValue();
        case VALUE_NOT_SET:
            return null;
        default:
            // should not happen!
            throw new IllegalArgumentException(
                    String.format("Unsupported AnyValue type '%s'",
                            av.getValueCase()));
        }
    }

    private static Object toJsonValueSingle(
            final FieldDescriptor field,
            final Object value) {
        switch (field.getType()) {
        case BYTES:
            return ((ByteString) value).toByteArray();
        case ENUM:
            if ("google.protobuf.NullValue"
                    .equals(field.getEnumType().getFullName())) {
                return null;
            } else if ("opentelemetry.proto.logs.v1.SeverityNumber"
                    .equals(field.getEnumType().getFullName())) {
                return ((EnumValueDescriptor) value).getNumber();
            }

            return ((EnumValueDescriptor) value).getName();
        case GROUP:
        case MESSAGE:
            if (value == null) {
                return null;
            }

            return toJsonMap(((Message) value).getAllFields());
        default:
            return value;
        }
    }

    private static Object toJsonNestedValue(final Message m) {
        FieldDescriptor v = m.getDescriptorForType().findFieldByName("value");

        if (v == null) {
            throw new IllegalArgumentException(
                    String.format("Malformed protobuf message '%s'",
                            m.getDescriptorForType().getFullName()));
        }

        return toJsonValueSingle(v, m.getField(v));
    }
}
