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

package io.mishmash.opentelemetry.persistence.proto;

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

/**
 * Various helper methods for general protobuf message handling.
 */
public final class ProtobufUtils {

    /**
     * Custom converters for some protobuf message types.
     *
     * Helps with avoiding unnecessary nesting.
     */
    private static Map<String, Function<Message, Object>> converters;

    static {
        converters.put(
                BoolValue.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        converters.put(
                Int32Value.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        converters.put(
                UInt32Value.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        converters.put(
                Int64Value.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        converters.put(
                UInt64Value.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        converters.put(
                StringValue.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        converters.put(
                BytesValue.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        converters.put(
                FloatValue.getDescriptor().getFullName(),
                ProtobufUtils::toJsonNestedValue);
        converters.put(
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
            res.put(ent.getKey().getJsonName(),
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
                    converters.get(msg.getDescriptorForType().getFullName());

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
            List<?> valuesList = (List<?>) value;
            List<Object> resList = new ArrayList<>(valuesList.size());

            for (Object o : valuesList) {
                resList.add(toJsonValueSingle(field, o));
            }

            return resList;
        } else {
            return toJsonValueSingle(field, value);
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
