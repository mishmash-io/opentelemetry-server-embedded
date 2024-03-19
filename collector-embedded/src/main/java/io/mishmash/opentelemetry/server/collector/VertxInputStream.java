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

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * An {@link java.io.InputStream} that takes a Vert.x
 * {@link io.vertx.core.streams.ReadStream} and reads it
 * up to a given number of bytes.
 *
 * Used to limit the number of bytes a client can upload
 * allowing for the upload to not be stored temporarily,
 * but rather to be processed on the fly.
 */
public class VertxInputStream extends InputStream {

    /**
     * Special constant to mark this input stream as
     * having reached the last input.
     */
    protected static final Buffer END_BUF = Buffer.buffer();
    /**
     * Special constant to mark this input stream as
     * having reached an error.
     */
    protected static final Buffer EXCEPTION_BUF = Buffer.buffer();

    /**
     * Used to mask byte values.
     */
    private static final int BYTE_MASK = 0x0FF;

    /**
     * Maximum number of bytes allowed to pass through.
     */
    private long maxBytes;
    /**
     * Number of bytes read so far.
     */
    private long bytesRead;
    /**
     * If this {@link java.io.InputStream} is closed.
     */
    private boolean closed = false;
    /**
     * Bytes available to read.
     */
    private AtomicInteger available = new AtomicInteger(0);
    /**
     * The {@link io.vertx.core.buffer.Buffer} we are
     * currently reading from.
     */
    private Buffer currentBuffer;
    /**
     * The position in the current {@link io.vertx.core.buffer.Buffer}.
     */
    private int pos;
    /**
     * A queue of pending buffers.
     */
    private BlockingQueue<Buffer> queue = new LinkedBlockingQueue<>();
    /**
     * A reference to the error that caused this stream to fail.
     */
    private Throwable cause;

    /**
     * Create a new stream.
     *
     * @param readStream the input to read from
     * @param maxBytesToRead the maximum number of input bytes allowed
     */
    public VertxInputStream(
            final ReadStream<Buffer> readStream,
            final long maxBytesToRead) {
        this.maxBytes = maxBytesToRead;

        readStream
            .handler(this::addBuffer)
            .endHandler(this::handleStreamEnd)
            .exceptionHandler(this::handleStreamException);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read() throws IOException {
        if (closed) {
            throw new IOException("Input stream closed");
        }

        if (currentBuffer == null) {
            try {
                currentBuffer = queue.take();
                pos = 0;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (currentBuffer == null) {
            throw new IOException("Got a null buffer");
        } else if (currentBuffer == END_BUF || bytesRead == maxBytes) {
            return -1;
        } else if (currentBuffer == EXCEPTION_BUF) {
            throw new IOException(cause.getMessage(), cause);
        } else {
            int b = currentBuffer.getByte(pos++) & BYTE_MASK;

            bytesRead++;
            available.decrementAndGet();

            if (pos == currentBuffer.length()) {
                pos = 0;
                currentBuffer = null;
            }

            return b;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int available() throws IOException {
        return available.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        closed = true;
    }

    /**
     * Queues the next input {@link io.vertx.core.buffer.Buffer}.
     *
     * @param b the input buffer
     */
    protected void addBuffer(final Buffer b) {
        queue.add(b);
        available.addAndGet(b.length());
    }

    /**
     * Called when there will be no more input buffers.
     *
     * @param v - a void
     */
    protected void handleStreamEnd(final Void v) {
        queue.add(END_BUF);
    }

    /**
     * Called upon errors.
     *
     * @param t the error encountered
     */
    protected void handleStreamException(final Throwable t) {
        queue.add(EXCEPTION_BUF);
        cause = t;
    }
}
