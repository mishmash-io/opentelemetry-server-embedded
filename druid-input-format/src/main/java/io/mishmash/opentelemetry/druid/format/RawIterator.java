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

import java.util.Iterator;

/**
 * A {@link BaseIterator} that wraps around an {@link Iterable} of
 * elements of a different type. Converts these elements before
 * returning them via {@link #next()}.
 *
 * @param <T> the type of elements of this iterator
 * @param <I> the {@link Iterable} elements type (to be converted)
 */
public abstract class RawIterator<T, I> extends BaseIterator<T> {

    /**
     * An iterator obtained from the wrapped {@link Iterable}.
     */
    private Iterator<I> elements;

    /**
     * Create a new {@link RawIterator}.
     *
     * @param wrap the {@link Iterable} to wrap
     */
    public RawIterator(final Iterable<I> wrap) {
        this.elements = wrap.iterator();
    }

    /**
     * Convert an input element to its output type.
     *
     * Called by {@link #next()} before returning.
     *
     * @param element the input element
     * @return the element to be returned by {@link #next()}
     */
    public abstract T convert(I element);

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return elements.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T next() {
        I n = elements.next();
        T t = convert(n);

        initMetaFor(t);

        return t;
    }
}
