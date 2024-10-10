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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.druid.java.util.common.parsers.CloseableIteratorWithMetadata;

/**
 * A base implementation of {@link CloseableIteratorWithMetadata}.
 *
 * Allows metadata to be computed before {@link #next()} returns.
 *
 * @param <T> the iterator element type
 */
public abstract class BaseIterator<T>
        implements CloseableIteratorWithMetadata<T> {

    /**
     * Holds the metadata.
     */
    private Map<String, Object> meta;

    /**
     * Construct a new {@link BaseIterator}.
     */
    public BaseIterator() {
        meta = new HashMap<>();
    }

    /**
     * Build the metadata for the element.
     *
     * @param element the next element to be returned by this iterator
     */
    public abstract void initMetaFor(T element);

    /**
     * Set a metadata field.
     *
     * @param key the metadata field key
     * @param value the value
     */
    public void setMeta(
            final String key,
            final Object value) {
        meta.put(key, value);
    }

    /**
     * Clear all metadata currently held by this iterator.
     */
    public void clearMeta() {
        meta.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        /*
         * nothing to do, input is closed immediately
         * when this object was instantiated.
         */
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> currentMetadata() {
        return meta;
    }
}
