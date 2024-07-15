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

import java.util.concurrent.Flow.Subscriber;

/**
 * A {@link java.util.concurrent.Flow.Subscriber} for
 * OpenTelemetry {@link ProfileSampleValue} records.
 */
public interface ProfilesSubscriber extends Subscriber<ProfileSampleValue> {

    /**
     * Method invoked for each profile entry of each OTLP packet.
     *
     * If this method throws an exception the subscription may
     * be cancelled.
     */
    @Override
    void onNext(ProfileSampleValue profile);
}
