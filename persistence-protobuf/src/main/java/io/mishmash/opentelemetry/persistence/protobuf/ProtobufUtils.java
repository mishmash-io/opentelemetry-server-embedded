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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

/**
 * Various helper methods for general protobuf message handling.
 */
public final class ProtobufUtils {

    private ProtobufUtils() {
        // constructor is hidden
    }

    /**
     * Prepare a {@link Collection} of entries suitable for
     * {@link #toJsonMap(Collection, boolean)} when defaults for
     * unset fields are also needed.
     *
     * @param msg the {@link Message}
     * @return a {@link Collection} to pass to
     * {@link #toJsonMap(Collection, boolean)}
     */
    public static Collection<Map.Entry<FieldDescriptor, Object>>
            withUnsetFields(final Message msg) {
        return msg.getDescriptorForType().getFields().stream()
                .map(field -> mapEntry(field,
                        field.hasPresence() && !msg.hasField(field)
                            ? null
                            : msg.getField(field)))
                .toList();
    }

    /**
     * Convert a {@link Collection} of fields into a JSON-friendly map.
     *
     * Use to convert all fields of a {@link Message}. Pass either
     * the return of {@link Message#getAllFields()} (only those fields
     * that are actually set in the message), or pass the result of
     * {@link #withUnsetFields(Message)} if you want to include defaults
     * for fields that are not set in a Message.
     *
     * @param entries the protobuf fields
     * @param withDefaults pass true when defaults for unset fields are
     * to be included
     * @return a JSON-friendly {@link Map}
     */
    public static Map<String, Object> toJsonMap(
            final Collection<Map.Entry<FieldDescriptor, Object>> entries,
            final boolean withDefaults) {
        Map<String, Object> res = new LinkedHashMap<>(entries.size());

        for (Map.Entry<FieldDescriptor, Object> ent : entries) {
            res.put(ent.getKey().getName(),
                    toJsonValue(ent.getKey(), ent.getValue(), withDefaults));
        }

        return res;
    }

    private static Map.Entry<FieldDescriptor, Object> mapEntry(
            final FieldDescriptor field,
            final Object value) {
        return new SimpleImmutableEntry<>(field, value);
    }

    private static Object toJsonValue(
            final FieldDescriptor field,
            final Object value,
            final boolean withDefaults) {
        if (field.isMapField()) {
            if (value == null && withDefaults) {
                return Collections.<String, Object>emptyMap();
            }

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
            Map<String, Object> resMap =
                    new LinkedHashMap<>(elementsList.size());

            for (Object element : elementsList) {
                resMap.put(
                        (String) toJsonValueSingle(
                                k,
                                ((Message) element).getField(k),
                                withDefaults),
                        toJsonValueSingle(
                                v,
                                ((Message) element).getField(v),
                                withDefaults));
            }

            return resMap;
        } else if (field.isRepeated()) {
            if (FieldDescriptor.Type.MESSAGE.equals(field.getType())
                    && "opentelemetry.proto.common.v1.KeyValue".equals(
                            field.getMessageType().getFullName())) {
                @SuppressWarnings("unchecked")
                List<KeyValue> kvs = (List<KeyValue>) value;

                return toJsonValue(kvs, withDefaults);
            } else {
                List<?> valuesList = (List<?>) value;
                List<Object> resList = new ArrayList<>(valuesList.size());

                for (Object o : valuesList) {
                    resList.add(toJsonValueSingle(field, o, withDefaults));
                }

                return resList;
            }
        } else {
            return toJsonValueSingle(field, value, withDefaults);
        }
    }

    private static Object toJsonValue(
            final List<KeyValue> kvs,
            final boolean withDefaults) {
        if (kvs == null) {
            return Collections.emptyList();
        }

        Map<String, Object> res = new LinkedHashMap<>(kvs.size());

        for (KeyValue kv : kvs) {
            res.put(
                    kv.getKey(),
                    toJsonValue(kv.getValue(), withDefaults));
        }

        return res;
    }

    private static Object toJsonValue(
            final AnyValue av,
            final boolean withDefaults) {
        if (av == null && withDefaults) {
            // return an object, the most 'complex' value type
            return Collections.<String, Object>emptyMap();
        }

        switch (av.getValueCase()) {
        case ARRAY_VALUE:
            List<Object> res = new ArrayList<>(
                    av.getArrayValue().getValuesCount());
            for (AnyValue a : av.getArrayValue().getValuesList()) {
                res.add(toJsonValue(a, withDefaults));
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
            return toJsonValue(
                    av.getKvlistValue().getValuesList(),
                    withDefaults);
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
            final Object value,
            final boolean withDefaults) {
        if (value == null && !withDefaults) {
            throw new IllegalArgumentException(
                    String.format("Unexpected null protobuf field '%s'",
                            field.getFullName()));
        }

        switch (field.getType()) {
        case BYTES:
            return value == null && withDefaults
                    ? new byte[] {}
                    : ((ByteString) value).toByteArray();
        case ENUM:
            if ("google.protobuf.NullValue"
                    .equals(field.getEnumType().getFullName())) {
                return null;
            } else if ("opentelemetry.proto.logs.v1.SeverityNumber"
                    .equals(field.getEnumType().getFullName())) {
                return value == null && withDefaults
                        ? Integer.valueOf(0)
                        : ((EnumValueDescriptor) value).getNumber();
            }

            return value == null && withDefaults
                    ? ""
                    : ((EnumValueDescriptor) value).getName();
        case GROUP:
        case MESSAGE:
            if (withDefaults) {
                if (value == null) {
                    return Collections.<String, Object>emptyMap();
                } else {
                    return toJsonMap(
                            withUnsetFields((Message) value),
                            withDefaults);
                }
            } else if (value == null) {
                return null;
            }

            return toJsonMap(
                    ((Message) value).getAllFields().entrySet(),
                    withDefaults);
        case BOOL:
            return value == null && withDefaults
                    ? Boolean.FALSE
                    : value;
        case DOUBLE:
            return value == null && withDefaults
                ? Double.valueOf(0.0)
                : value;
        case INT32:
        case SINT32:
        case UINT32:
        case FIXED32:
        case SFIXED32:
            return value == null && withDefaults
                ? Integer.valueOf(0)
                : value;
        case INT64:
        case SINT64:
        case UINT64:
        case FIXED64:
        case SFIXED64:
            return value == null && withDefaults
                ? Long.valueOf(0)
                : value;
        case FLOAT:
            return value == null && withDefaults
                ? Float.valueOf(0.0f)
                : value;
        case STRING:
            return value == null && withDefaults
                ? ""
                : value;
        default:
            throw new UnsupportedOperationException(
                    String.format("Unknown protobuf field type '%s'",
                            field.getType()));
        }
    }

    private static Object toJsonNestedValue(
            final Message m,
            final boolean withDefaults) {
        FieldDescriptor v = m.getDescriptorForType().findFieldByName("value");

        if (v == null && !withDefaults) {
            throw new IllegalArgumentException(
                    String.format("Malformed protobuf message '%s'",
                            m.getDescriptorForType().getFullName()));
        }

        return toJsonValueSingle(
                v,
                m.getField(v),
                withDefaults);
    }
}
