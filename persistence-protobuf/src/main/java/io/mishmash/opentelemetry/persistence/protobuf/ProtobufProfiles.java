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

import java.util.List;
import java.util.Map;

import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.KeyValueUnit;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.PersistedProfile;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.StrFunction;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.StrLine;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.StrLocation;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.StrMapping;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.StrValueType;
import io.mishmash.opentelemetry.server.collector.ProfileSampleValue;
import io.opentelemetry.proto.profiles.v1development.Function;
import io.opentelemetry.proto.profiles.v1development.KeyValueAndUnit;
import io.opentelemetry.proto.profiles.v1development.Line;
import io.opentelemetry.proto.profiles.v1development.Link;
import io.opentelemetry.proto.profiles.v1development.Location;
import io.opentelemetry.proto.profiles.v1development.Mapping;
import io.opentelemetry.proto.profiles.v1development.Profile;
import io.opentelemetry.proto.profiles.v1development.ProfilesDictionary;
import io.opentelemetry.proto.profiles.v1development.Sample;
import io.opentelemetry.proto.profiles.v1development.ValueType;
import io.opentelemetry.proto.resource.v1.Resource;

/**
 * Utility class to help with protobuf serialization of
 * {@link ProfileSampleValue} instances.
 */
public final class ProtobufProfiles {

    private ProtobufProfiles() {
        // constructor is hidden
    }

    /**
     * Get a protobuf representation of a
     * {@link ProfileSampleValue}.
     *
     * @param profile the profile sample value
     * @return a populated {@link PersistedProfile.Builder}
     */
    public static PersistedProfile.Builder buildProfileSampleValue(
            final ProfileSampleValue profile) {
        PersistedProfile.Builder builder =
                PersistedProfile.newBuilder()
                    .setBatchTimestamp(profile.getBatchTimestamp())
                    .setBatchUUID(profile.getBatchUUID())
                    .setResourceSeqNo(
                            profile.getResourceProfileSeqNo())
                    .setScopeSeqNo(profile.getScopeProfileSeqNo())
                    .setProfileSeqNo(profile.getProfileSeqNo())
                    .setSampleSeqNo(profile.getSampleSeqNo())
                    .setValueSeqNo(profile.getValueSeqNo())
                    .setIsValid(profile.isValid());

        if (profile.getErrorMessage() != null) {
            builder = builder
                    .setErrorMessage(profile.getErrorMessage());
        }

        if (profile.getResource() != null) {
            Resource r = profile.getResource();

            builder = builder
                    .addAllResourceAttributes(r.getAttributesList())
                    .setResourceDroppedAttributesCount(
                            r.getDroppedAttributesCount())
                    .addAllResourceEntityRefs(r.getEntityRefsList());
        }

        if (profile.getResourceSchemaUrl() != null) {
            builder = builder
                    .setResourceSchemaUrl(
                            profile.getResourceSchemaUrl());
        }

        if (profile.getScope() != null) {
            builder = builder
                    .setScopeName(profile.getScope().getName())
                    .setScopeVersion(profile.getScope().getVersion())
                    .addAllScopeAttributes(
                            profile.getScope().getAttributesList())
                    .setScopeDroppedAttributesCount(
                            profile.getScope()
                                .getDroppedAttributesCount());
        }

        if (profile.getProfileSchemaUrl() != null) {
            builder = builder
                    .setProfileSchemaUrl(profile.getProfileSchemaUrl());
        }

        Profile p = profile.getProfile();
        ProfilesDictionary dict = profile.getDictionary();
        if (p != null) {
            builder = builder
                    .setProfileId(p.getProfileId())
                    .addAllProfileAttributes(
                            resolveAttributes(
                                    dict,
                                    p.getAttributeIndicesList()))
                    .setProfileDroppedAttributesCount(
                            p.getDroppedAttributesCount())
                    .setOriginalPayloadFormat(p.getOriginalPayloadFormat())
                    .setOriginalPayload(p.getOriginalPayload())
                    .setTimeUnixNano(p.getTimeUnixNano())
                    .setDurationNano(p.getDurationNano())
                    .setPeriodType(toStr(dict, p.getPeriodType()))
                    .setPeriod(p.getPeriod())
                    .setSampleType(toStr(dict, p.getSampleType()));
        }

        if (profile.getSample() != null) {
            Sample s = profile.getSample();

            builder = builder
                    .addAllStack(resolveLocations(dict, s, p))
                    .addAllAttributes(
                            resolveAttributes(
                                    dict,
                                    s.getAttributeIndicesList()));

            if (profile.getObservationTimestamp() != null) {
                builder = builder
                        .setTimestampUnixNano(
                                profile.getObservationTimestamp());
            }

            Link l = dict.getLinkTableCount() == 0
                    ? null
                    : dict.getLinkTable((int) s.getLinkIndex());
            if (l != null) {
                builder = builder
                        .setTraceId(l.getTraceId())
                        .setSpanId(l.getSpanId());
            }
        }

        if (profile.getValue() != null) {
            builder = builder.setValue(profile.getValue());
        }

        return builder;
    }

    /**
     * Return the string contained in this profile at a given index.
     *
     * @param dictionary the OTLP profiles dictionary
     * @param index the index
     * @return the string at that index
     */
    public static String getStrAt(
            final ProfilesDictionary dictionary,
            final int index) {
        return dictionary.getStringTable(index);
    }

    /**
     * Convert a ValueType to StrValueType, resolving all strings.
     *
     * @param dictionary an OTLP profiles dictionary
     * @param vt an OTLP ValueType
     * @return a converted value type
     */
    public static StrValueType.Builder toStr(
            final ProfilesDictionary dictionary,
            final ValueType vt) {
        return StrValueType.newBuilder()
                .setType(getStrAt(dictionary, vt.getTypeStrindex()))
                .setUnit(getStrAt(dictionary, vt.getUnitStrindex()));
    }

    /**
     * Resolve an OTLP Mapping.
     *
     * @param dictionary the OTLP profiles dictionary, used for resolution
     * @param mapping the OTLP Mapping
     * @return a resolved mapping
     */
    public static StrMapping.Builder resolve(
            final ProfilesDictionary dictionary,
            final Mapping mapping) {
        return StrMapping.newBuilder()
                .setMemoryStart(mapping.getMemoryStart())
                .setMemoryLimit(mapping.getMemoryLimit())
                .setFileOffset(mapping.getFileOffset())
                .setFilename(
                        getStrAt(dictionary, mapping.getFilenameStrindex()))
                .addAllAttributes(
                        resolveAttributes(
                                dictionary,
                                mapping.getAttributeIndicesList()));
    }

    /**
     * Resolve an OTLP Function.
     *
     * @param dictionary the OTLP profile dictionary, used for resolution
     * @param f the OTLP Function
     * @return a function with resolved strings and values
     */
    public static StrFunction.Builder resolve(
            final ProfilesDictionary dictionary,
            final Function f) {
        return StrFunction.newBuilder()
                .setName(getStrAt(dictionary, f.getNameStrindex()))
                .setSystemName(getStrAt(dictionary, f.getSystemNameStrindex()))
                .setFilename(getStrAt(dictionary, f.getFilenameStrindex()))
                .setStartLine(f.getStartLine());
    }

    /**
     * Resolve an OTLP Line.
     *
     * @param dictionary the OTLP profiles dictionary, used for resolution
     * @param l the OTLP Line
     * @return a line with resolved strings and values
     */
    public static StrLine.Builder resolve(
            final ProfilesDictionary dictionary,
            final Line l) {
        return StrLine.newBuilder()
                .setFunction(
                        resolve(
                            dictionary,
                            dictionary.getFunctionTable(
                                    (int) l.getFunctionIndex())))
                .setLine(l.getLine())
                .setColumn(l.getColumn());
    }

    /**
     * Convert an OTLP Location by resolving all indexes.
     *
     * @param dictionary the OTLP profile dictionary, used for resolution
     * @param location the OTLP Location
     * @return a resolved location
     */
    public static StrLocation.Builder resolve(
            final ProfilesDictionary dictionary,
            final Location location) {
        return StrLocation.newBuilder()
                .setMapping(
                        resolve(
                            dictionary,
                            dictionary.getMappingTable(
                                    (int) location.getMappingIndex())))
                .setAddress(location.getAddress())
                .addAllLines(resolveLines(dictionary, location.getLinesList()))
                .addAllAttributes(
                        resolveAttributes(
                                dictionary,
                                location.getAttributeIndicesList()));
    }

    /**
     * Resolve an OTLP Sample attribute, adding an attribute unit if found.
     *
     * @param dictionary the OTLP profile dictionary, used for lookup
     * @param attr the Sample attribute
     * @return an attribute with a unit (if present)
     */
    public static KeyValueUnit.Builder resolve(
            final ProfilesDictionary dictionary,
            final KeyValueAndUnit attr) {
        return KeyValueUnit.newBuilder()
                .setKey(dictionary.getStringTable(attr.getKeyStrindex()))
                .setValue(attr.getValue())
                .setUnit(dictionary.getStringTable(attr.getUnitStrindex()));
    }

    /**
     * Get all locations contained in an OTLP Sample and convert them.
     *
     * @param dictionary the OTLP profile dictionary, used for resolution
     * @param s the OTLP Sample
     * @param p the OTLP Profile where the location indices list is
     * @return a list of all contained locations, resolved
     */
    public static List<StrLocation> resolveLocations(
            final ProfilesDictionary dictionary,
            final Sample s,
            final Profile p) {
        List<Integer> indexes = dictionary
                                    .getStackTable(s.getStackIndex())
                                    .getLocationIndicesList();

        return indexes.stream()
                    .map(i -> dictionary.getLocationTable(i))
                    .map(l -> resolve(dictionary, l))
                    .map(sl -> sl.build())
                    .toList();
    }

    /**
     * Resolve a list of OTLP Sample attributes.
     *
     * @param dictionary the OTLP profile dictionary, used for resolution
     * @param attributes the list of attribute indexes
     * @return a list of resolved attributes
     */
    public static List<KeyValueUnit> resolveAttributes(
            final ProfilesDictionary dictionary,
            final List<Integer> attributes) {
        return attributes.stream()
                .map(a -> resolve(
                            dictionary,
                            dictionary.getAttributeTable(a)))
                .map(a -> a.build())
                .toList();
    }

    /**
     * Resolve a list of Sample Lines.
     *
     * @param dictionary the OTLP profile dictionary, used for resolution
     * @param lines the OTLP Lines
     * @return a list of resolved lines
     */
    public static List<StrLine> resolveLines(
            final ProfilesDictionary dictionary,
            final List<Line> lines) {
        return lines.stream()
                .map(l -> resolve(dictionary, l))
                .map(l -> l.build())
                .toList();
    }

    /**
     * Resolve a list of string indexes to the original strings.
     *
     * @param dictionary an OTLP profiles dictionary
     * @param indexes the list of indexes
     * @return a list of resolved strings
     */
    public static List<String> toStrLong(
            final ProfilesDictionary dictionary,
            final List<Long> indexes) {
        return indexes.stream()
                .map(l -> getStrAt(dictionary, l.intValue()))
                .toList();
    }

    /**
     * Resolve a list of string indexes to the original strings.
     *
     * @param dictionary an OTLP profiles dictionary
     * @param indexes the list of indexes
     * @return a list of resolved strings
     */
    public static List<String> toStrInt(
            final ProfilesDictionary dictionary,
            final List<Integer> indexes) {
        return indexes.stream()
                .map(l -> getStrAt(dictionary, l))
                .toList();
    }

    /**
     * Convert a {@link PersistedProfile} to a {@link Map} suitable for JSON
     * encoding.
     *
     * @param profile the persisted profile protobuf message
     * @param withDefaults true when defaults for unset fields should also
     * be included
     * @return the {@link Map}
     */
    public static Map<String, Object> toJsonMap(
            final PersistedProfile profile,
            final boolean withDefaults) {
        return ProtobufUtils.toJsonMap(
                withDefaults
                    ? ProtobufUtils.withUnsetFields(profile)
                    : profile.getAllFields().entrySet(),
                withDefaults);
    }
}
