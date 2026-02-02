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

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.EntityRef;
import io.opentelemetry.proto.common.v1.KeyValue;

/**
 * Common methods for signals that contain Resource attributes,
 * entities, etc.
 *
 * Signals that implement this interface can be configured to
 * add additional, default values for attributes that were not
 * received with the signal. See the
 * {@link #computeResourceAttributes(Iterable)}
 * for more on the topic of configuration.
 */
public interface SignalResource {

    /**
     * The name of an environment variable that configures
     * default Resource attribute values.
     */
    String DEFAULT_RESOURCE_ATTRS_ENV =
            "DEFAULT_RESOURCE_ATTRIBUTES";
    /**
     * The name of an Java system property that configures
     * default Resource attribute values.
     */
    String DEFAULT_RESOURCE_ATTRS_PROP =
            "default.resource.attributes";

    /**
     * Returns the resource attributes associated with this signal.
     *
     * Implementations typically call
     * {@link #computeResourceAttributes(Iterable)}
     * to include configured defaults.
     *
     * @return an {@link Iterable} of the Resource attributes
     */
    Iterable<KeyValue> getResourceAttributes();

    /**
     * Get the number of Resource attributes that were dropped.
     *
     * @return number of dropped attributes or zero
     */
    int getResourceDroppedAttributesCount();

    /**
     * Return the Resource entities associated with this signal.
     *
     * @return the Resource entities
     */
    Iterable<EntityRef> getResourceEntityRefs();

    /**
     * Get the Resource schema URL of this signal.
     *
     * @return the schema URL
     */
    String getResourceSchemaUrl();

    /**
     * Computes the output Resource attributes, considering those that
     * were received and any configured defaults.
     *
     * Set the system property {@link #DEFAULT_RESOURCE_ATTRS_PROP}
     * or the environment variable {@link #DEFAULT_RESOURCE_ATTRS_ENV}
     * to a comma-separated string of 'attr_key=attr_value' pairs and
     * these will be used as defaults for Resource attributes. That is -
     * if an incoming signal does not contain a value for one or more of
     * them - it will be added.
     *
     * Default attribute values will be added as strings. Empty values
     * are allowed. The environment variable takes precedence over the system
     * property.
     *
     * @param attrs The received resource attributes
     * @return the attributes to use, including any defaults
     */
    default Iterable<KeyValue> computeResourceAttributes(
            final Iterable<KeyValue> attrs) {
        String defaultAttrsEnv = System.getenv(DEFAULT_RESOURCE_ATTRS_ENV);
        String defaultAttrsProp = System
                .getProperty(DEFAULT_RESOURCE_ATTRS_PROP);
        String defaultAttrs = null;

        if (defaultAttrsProp != null && !defaultAttrsProp.isBlank()) {
            defaultAttrs = defaultAttrsProp;
        }

        if (defaultAttrsEnv != null && !defaultAttrsEnv.isBlank()) {
            defaultAttrs = defaultAttrsEnv;
        }

        if (defaultAttrs == null) {
            return attrs;
        }

        // get a stream of the original attributes
        Stream<Map.Entry<String, KeyValue>> original = StreamSupport
                .stream(attrs.spliterator(), false)
                .map(kv -> Map.entry(kv.getKey(), kv));

        /*
         * get a stream of the configured defaults, missing
         * values will have empty KeyValue objects
         */
        Stream<Map.Entry<String, KeyValue>> defaults =
                Arrays.stream(defaultAttrs.split(","))
                    .filter(a -> a != null && !a.isBlank())
                    .map(a -> a.split("=", 2))
                    .filter(aa -> !aa[0].isBlank())
                    .map(aa -> Map.entry(
                            aa[0],
                            KeyValue.newBuilder()
                                .setKey(aa[0])
                                .setValue(aa.length > 1
                                        ? AnyValue.newBuilder()
                                                .setStringValue(aa[1])
                                        : AnyValue.newBuilder())
                                .build()));

        // concat the two streams and collect removing duplicates
        Map<String, KeyValue> result = Stream.concat(defaults, original)
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (a, b) -> b));

        return result.values();
    }
}
