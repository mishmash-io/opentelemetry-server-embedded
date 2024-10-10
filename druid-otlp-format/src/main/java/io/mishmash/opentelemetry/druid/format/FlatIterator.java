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

import java.util.NoSuchElementException;

/**
 * A {@link BaseIterator} that iterates over a single element.
 *
 * @param <T> the iterator element type
 */
public abstract class FlatIterator<T> extends BaseIterator<T> {

    /**
     * The single value to be returned by this iterator.
     */
    private T value;
    /**
     * A flag if the value was already returned or not.
     */
    private boolean hasNext = true;

    /**
     * Create a new {@link FlatIterator}.
     *
     * @param element the element that will be returned by this iterator
     */
    public FlatIterator(final T element) {
        value = element;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return hasNext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T next() {
        if (!hasNext) {
            throw new NoSuchElementException();
        }

        hasNext = false;

        initMetaFor(value);

        return value;
    }
}
