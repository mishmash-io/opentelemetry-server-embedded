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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.KeyValueUnit;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.PersistedProfile;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.StrFunction;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.StrLabel;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.StrLine;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.StrLocation;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.StrMapping;
import io.mishmash.opentelemetry.persistence.proto.v1.ProfilesPersistenceProto.StrValueType;
import io.mishmash.opentelemetry.server.collector.ProfileSampleValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.profiles.v1experimental.AttributeUnit;
import io.opentelemetry.proto.profiles.v1experimental.Function;
import io.opentelemetry.proto.profiles.v1experimental.Label;
import io.opentelemetry.proto.profiles.v1experimental.Line;
import io.opentelemetry.proto.profiles.v1experimental.Link;
import io.opentelemetry.proto.profiles.v1experimental.Location;
import io.opentelemetry.proto.profiles.v1experimental.Mapping;
import io.opentelemetry.proto.profiles.v1experimental.Profile;
import io.opentelemetry.proto.profiles.v1experimental.ProfileContainer;
import io.opentelemetry.proto.profiles.v1experimental.Sample;
import io.opentelemetry.proto.profiles.v1experimental.ValueType;

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
            builder = builder
                    .addAllResourceAttributes(
                            profile.getResource().getAttributesList())
                    .setResourceDroppedAttributesCount(
                            profile.getResource()
                                .getDroppedAttributesCount());
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

        if (profile.getProfileContainer() != null) {
            ProfileContainer container = profile.getProfileContainer();

            builder = builder
                    .setProfileId(container.getProfileId())
                    .setStartTimeUnixNano(
                            container.getStartTimeUnixNano())
                    .setEndTimeUnixNano(container.getEndTimeUnixNano())
                    .addAllProfileAttributes(
                            container.getAttributesList())
                    .setProfileDroppedAttributesCount(
                            container.getDroppedAttributesCount())
                    .setOriginalPayloadFormat(
                            container.getOriginalPayloadFormat())
                    .setOriginalPayload(
                            container.getOriginalPayload());
        }

        Profile p = profile.getProfile();
        if (profile.getProfile() != null) {
            builder = builder
                    .setDropFrames(getStrAt(p, p.getDropFrames()))
                    .setKeepFrames(getStrAt(p, p.getKeepFrames()))
                    .setTimeNanos(p.getTimeNanos())
                    .setDurationNanos(p.getDurationNanos())
                    .setPeriodType(toStr(p, p.getPeriodType()))
                    .setPeriod(p.getPeriod())
                    .addAllComment(toStr(p, p.getCommentList()))
                    .setDefaultSampleType(
                            getStrAt(p, p.getDefaultSampleType()));
        }

        if (profile.getSample() != null) {
            Sample s = profile.getSample();

            builder = builder
                    .setStacktraceId(
                            getStrAt(p, s.getStacktraceIdIndex()))
                    .addAllLocations(resolveLocations(p, s))
                    .addAllLabels(resolveLabels(p, s.getLabelList()))
                    .addAllAttributes(
                            resolveAttributes(
                                    p,
                                    s.getAttributesList()))
                    .addAllTimestampsUnixNano(
                            s.getTimestampsUnixNanoList())
                    .setType(
                            toStr(
                                p,
                                p.getSampleType(
                                    profile.getValueSeqNo())));

            Link l = p.getLinkTableCount() == 0
                    ? null
                    : p.getLinkTable((int) s.getLink());
            if (l != null) {
                builder = builder
                        .setTraceId(l.getTraceId())
                        .setSpanId(l.getSpanId());
            }
        }

        builder = builder.setValue(profile.getValue());

        return builder;
    }

    /**
     * Return the string contained in this profile at a given index.
     *
     * @param profile the OTLP profile
     * @param index the index
     * @return the string at that index
     */
    public static String getStrAt(final Profile profile, final long index) {
        return profile.getStringTable((int) index);
    }

    /**
     * Convert a ValueType to StrValueType, resolving all strings.
     *
     * @param p an OTLP Profile
     * @param vt an OTLP ValueType
     * @return a converted value type
     */
    public static StrValueType.Builder toStr(
            final Profile p,
            final ValueType vt) {
        return StrValueType.newBuilder()
                .setType(getStrAt(p, vt.getType()))
                .setUnit(getStrAt(p, vt.getUnit()))
                .setAggregationTemporality(vt.getAggregationTemporality());
    }

    /**
     * Resolve an OTLP Mapping.
     *
     * @param p the OTLP Profile, used for resolution
     * @param mapping the OTLP Mapping
     * @return a resolved mapping
     */
    public static StrMapping.Builder resolve(
            final Profile p,
            final Mapping mapping) {
        return StrMapping.newBuilder()
                .setMemoryStart(mapping.getMemoryStart())
                .setMemoryLimit(mapping.getMemoryLimit())
                .setFileOffset(mapping.getFileOffset())
                .setFilename(getStrAt(p, mapping.getFilename()))
                .setBuildId(getStrAt(p, mapping.getBuildId()))
                .setBuildIdKind(mapping.getBuildIdKind())
                .addAllAttributes(
                        resolveAttributes(p, mapping.getAttributesList()))
                .setHasFunctions(mapping.getHasFunctions())
                .setHasFilenames(mapping.getHasFilenames())
                .setHasLineNumbers(mapping.getHasLineNumbers())
                .setHasInlineFrames(mapping.getHasInlineFrames());
    }

    /**
     * Resolve an OTLP Function.
     *
     * @param p the OTLP Profile, used for resolution
     * @param f the OTLP Function
     * @return a function with resolved strings and values
     */
    public static StrFunction.Builder resolve(
            final Profile p,
            final Function f) {
        return StrFunction.newBuilder()
                .setName(getStrAt(p, f.getName()))
                .setSystemName(getStrAt(p, f.getSystemName()))
                .setFilename(getStrAt(p, f.getFilename()))
                .setStartLine(f.getStartLine());
    }

    /**
     * Resolve an OTLP Line.
     *
     * @param p the OTLP Profile, used for resolution
     * @param l the OTLP Line
     * @return a line with resolved strings and values
     */
    public static StrLine.Builder resolve(
            final Profile p,
            final Line l) {
        return StrLine.newBuilder()
                .setFunction(
                        resolve(
                            p,
                            p.getFunction((int) l.getFunctionIndex())))
                .setLine(l.getLine())
                .setColumn(l.getColumn());
    }

    /**
     * Convert an OTLP Location by resolving all indexes.
     *
     * @param p the OTLP Profile, used for resolution
     * @param location the OTLP Location
     * @return a resolved location
     */
    public static StrLocation.Builder resolve(
            final Profile p,
            final Location location) {
        return StrLocation.newBuilder()
                .setMapping(
                        resolve(
                            p,
                            p.getMapping((int) location.getMappingIndex())))
                .setAddress(location.getAddress())
                .addAllLines(resolveLines(p, location.getLineList()))
                .setIsFolded(location.getIsFolded())
                .setType(getStrAt(p, location.getTypeIndex()))
                .addAllAttributes(
                        resolveAttributes(p, location.getAttributesList()));
    }

    /**
     * Resolve an OTLP Label.
     *
     * @param p the OTLP Profile used for resolution
     * @param label the OTLP Label
     * @return a resolved label
     */
    public static StrLabel.Builder resolve(
            final Profile p,
            final Label label) {
        StrLabel.Builder builder = StrLabel.newBuilder()
                .setKey(getStrAt(p, label.getKey()));

        if (label.getNum() > 0) {
            builder = builder
                    .setNum(label.getNum())
                    .setNumUnit(getStrAt(p, label.getNumUnit()));
        } else {
            builder = builder
                    .setStr(getStrAt(p, label.getStr()));
        }

        return builder;
    }

    /**
     * Resolve an OTLP Sample attribute, adding an attribute unit if found.
     *
     * @param p the OTLP Profile used for lookup
     * @param attr the Sample attribute
     * @return an attribute with a unit (if present)
     */
    public static KeyValueUnit.Builder resolve(
            final Profile p,
            final KeyValue attr) {
        String key = attr.getKey();
        KeyValueUnit.Builder builder = KeyValueUnit.newBuilder()
                .setKey(key)
                .setValue(attr.getValue());

        for (AttributeUnit u : p.getAttributeUnitsList()) {
            String unit = getStrAt(p, u.getAttributeKey());

            if (key.equals(unit)) {
                builder = builder
                        .setUnit(unit);

                break;
            }
        }

        return builder;
    }

    /**
     * Get all locations contained in an OTLP Sample and convert them.
     *
     * @param p the OTLP Profile, used for resolution
     * @param s the OTLP Sample
     * @return a list of all contained locations, resolved
     */
    public static List<StrLocation> resolveLocations(
            final Profile p,
            final Sample s) {
        if (s.getLocationIndexCount() > 0) {
            return s.getLocationIndexList().stream()
                    .map(l -> p.getLocation(l.intValue()))
                    .map(l -> resolve(p, l))
                    .map(l -> l.build())
                    .toList();
        } else if (s.getLocationsLength() > 0) {
            ArrayList<StrLocation> res =
                    new ArrayList<>((int) s.getLocationsLength());

            int startIdx = (int) s.getLocationsStartIndex();
            for (int idx = startIdx;
                    idx < startIdx + (int) s.getLocationsLength();
                    idx++) {
                res.add(
                        resolve(p, p.getLocation(idx))
                            .build());
            }

            return res;
        } else {
            // couldn't extract locations?
            return Collections.emptyList();
        }
    }

    /**
     * Resolve a list of OTLP Labels.
     *
     * @param p the OTLP Profile to use for resolution
     * @param labels the list of OTLP Labels
     * @return a list of resolved labels
     */
    public static List<StrLabel> resolveLabels(
            final Profile p,
            final List<Label> labels) {
        return labels.stream()
                .map(l -> resolve(p, l))
                .map(l -> l.build())
                .toList();
    }

    /**
     * Resolve a list of OTLP Sample attributes.
     *
     * @param p the OTLP Profile used for resolution
     * @param attributes the list of attribute indexes
     * @return a list of resolved attributes
     */
    public static List<KeyValueUnit> resolveAttributes(
            final Profile p,
            final List<Long> attributes) {
        return attributes.stream()
                .map(i -> p.getAttributeTable(i.intValue()))
                .map(a -> resolve(p, a))
                .map(a -> a.build())
                .toList();
    }

    /**
     * Resolve a list of Sample Lines.
     *
     * @param p the OTLP Profile used for resolution
     * @param lines the OTLP Lines
     * @return a list of resolved lines
     */
    public static List<StrLine> resolveLines(
            final Profile p,
            final List<Line> lines) {
        return lines.stream()
                .map(l -> resolve(p, l))
                .map(l -> l.build())
                .toList();
    }

    /**
     * Resolve a list of string indexes to the original strings.
     *
     * @param p an OTLP Profile
     * @param indexes the list of indexes
     * @return a list of resolved strings
     */
    public static List<String> toStr(
            final Profile p,
            final List<Long> indexes) {
        return indexes.stream()
                .map(l -> getStrAt(p, l))
                .toList();
    }

    /**
     * Convert a {@link PersistedProfile} to a {@link Map} suitable for JSON
     * encoding.
     *
     * @param profile the persisted profile protobuf message
     * @return the {@link Map}
     */
    public static Map<String, Object> toJsonMap(
            final PersistedProfile profile) {
        return ProtobufUtils.toJsonMap(profile.getAllFields());
    }
}
