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

import java.util.Collections;
import java.util.List;

import org.apache.druid.initialization.DruidModule;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;

/**
 * A {@link DruidModule} for the OTLP Input Format.
 *
 * Installs necessary modules, such as {@link OTLPInputFormat}.
 */
public class OTLPInputFormatModule implements DruidModule {

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Binder binder) {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<? extends Module> getJacksonModules() {
        return Collections.singletonList(
                new SimpleModule()
                    .registerSubtypes(
                            new NamedType(
                                    OTLPInputFormat.class,
                                    "otlp")
                    )
                );
    }
}
