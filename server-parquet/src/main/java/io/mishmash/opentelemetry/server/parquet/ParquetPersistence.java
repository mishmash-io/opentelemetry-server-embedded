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

package io.mishmash.opentelemetry.server.parquet;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.proto.ProtoParquetWriter;

import com.google.protobuf.Message;

/**
 * Writes records of a given {@link com.google.protobuf.Message} implementation
 * to Apache Parquet files, rotating the output files if necessary.
 *
 * @param <T> the output message class
 */
public class ParquetPersistence<T extends Message> implements Closeable {

    /**
     * Parquet metadata environment variables prefix.
     */
    protected static final String ENV_META_PREFIX = "PARQUET_META_";

    /**
     * Default parquet file row group size.
     */
    protected static final long DEFAULT_ROW_GROUP_SIZE = 1024 * 1024;

    /**
     * Default maximum output file size before rotating to a new file.
     */
    protected static final long DEFAULT_ROTATION_SIZE = 10 * 1024 * 1024;
    /**
     * Default maximum number of records in output file before rotating to
     * a new file.
     */
    protected static final long DEFAULT_ROTATION_NUM_RECORDS = 10000;
    /**
     * Default maximum output file age (in ms) before rotating to a new file.
     */
    protected static final long DEFAULT_ROTATION_TIME_MS = 5 * 60 * 1000;

    /**
     * The file name prefix used to create new file names as output grows.
     */
    private String baseFileName;
    /**
     * Number of times an output file was closed and a new one opened.
     */
    private int rotations = 0;
    /**
     * The class of the data records written to the output files.
     */
    private Class<T> messageClass;
    /**
     * Number of records written to the current output file so far.
     */
    private long currentNumWritten;
    /**
     * Time (in ms) when the current output file was opened.
     */
    private long rotationStartTime;

    /**
     * The {@link org.apache.hadoop.conf.Configuration} used by the
     * parquet file writer.
     */
    private Configuration conf = new Configuration();
    /**
     * The {@link org.apache.parquet.hadoop.ParquetWriter} used to
     * write to the current output file.
     */
    private ParquetWriter<T> currentWriter;
    /**
     * Holds metadata to be stored in a file.
     */
    private Map<String, String> meta;
    /**
     * An object used for locking during rotaions.
     */
    private Object rotationLock = new Object();

    /**
     * Create a new parquet persister instance and open its first
     * output file.
     *
     * @param fileNamePrefix the file name prefix (including any path)
     * to be used to compute output file names
     * @param persistedMessageClass the class of the message that will be
     * used as parquet file records
     * @throws IOException if the file system fails to open the first
     * output file
     */
    public ParquetPersistence(
                final String fileNamePrefix,
                final Class<T> persistedMessageClass)
            throws IOException {
        this.baseFileName = fileNamePrefix;
        this.messageClass = persistedMessageClass;

        collectMeta();
        openWriter(rotations);
    }

    /**
     * Write a record to the output.
     *
     * @param message the record
     * @throws IOException if an I/O error is encountered
     */
    public void write(final T message) throws IOException {
        if (currentWriter == null) {
            throw new IllegalStateException(
                    "Cannot write to an already closed parquet persistence");
        }

        currentWriter.write(message);

        if (shouldRotate()) {
            rotate();
        }
    }

    /**
     * Opens a new parqet writer for the given output file sequence number.
     *
     * @param rotation the file sequence number
     * @throws IOException if the underlying file system fails
     */
    protected void openWriter(final int rotation) throws IOException {
        Path fp = getFilePath(rotation, true);

        // Static configuration for now
        currentWriter = ProtoParquetWriter
                .<T>builder(fp)
                .withMessage(messageClass)
                .withWriteMode(Mode.CREATE)
                .withRowGroupSize(DEFAULT_ROW_GROUP_SIZE)
                .withPageWriteChecksumEnabled(false)
                .withExtraMetaData(meta)
                .build();

        currentNumWritten = 0;
        rotationStartTime = System.currentTimeMillis();
    }

    /**
     * Closes the current output file and renames it from its staging
     * to its final name.
     *
     * NOTE: Must be called with a lock on {@link rotationLock}
     * until {@link currentWriter} is replaced with a new writer
     * or a null.
     *
     * @throws IOException if the file system encounters and error
     */
    protected void closeWriter() throws IOException {
        currentWriter.close();

        Path stagingPath = getFilePath(rotations, true);
        Path completedPath = getFilePath(rotations, false);
        stagingPath.getFileSystem(conf).rename(stagingPath, completedPath);
    }

    /**
     * Returns true if the current output file should be closed and
     * a new one should be used instead.
     *
     * This implementation only considers file size, number of records
     * written so far an age of the current file.
     *
     * At the moment the maximums of the size, number of records and age
     * cannot be configured at run time. Fixed constants are used instead:
     * {@link #DEFAULT_ROTATION_SIZE},
     * {@link #DEFAULT_ROTATION_NUM_RECORDS}
     * and {@link #DEFAULT_ROTATION_TIME_MS}.
     *
     * @return true if a new file should be opened
     */
    protected boolean shouldRotate() {
        // Static configuration for now
        return getCurrentDataSize() > DEFAULT_ROTATION_SIZE
                || getCurrentNumCompletedRecords()
                        > DEFAULT_ROTATION_NUM_RECORDS
                || (
                        getCurrentNumCompletedRecords() > 1
                        && System.currentTimeMillis()
                            > getCurrentRotationStartTimeMs()
                                + DEFAULT_ROTATION_TIME_MS
                   );
    }

    /**
     * Open the next output file for writing.
     *
     * @throws IOException if the current file cannot be closed
     * cleanly of the new one cannot be open for writing
     */
    protected void rotate() throws IOException {
        synchronized (rotationLock) {
            closeWriter();
            openWriter(++rotations);
        }
    }

    /**
     * Compute the output file path for a given rotation.
     *
     * @param rotation the sequence number of an output file
     * @param isStaging if the name should be that of a staging file or not
     *
     * @return the {@link org.apache.hadoop.fs.Path} of the desired file
     */
    protected Path getFilePath(final int rotation, final boolean isStaging) {
        return new Path(getFileNamePrefix()
                + "-" + getNumCompletedFiles()
                + ".parquet"
                + (isStaging ? ".staging" : ""));
    }

    /**
     * Get the time (in ms) when the current output file was opened.
     *
     * @return time when last output file was opened or 0 if no files
     * were opened so far
     */
    protected long getCurrentRotationStartTimeMs() {
        return rotationStartTime;
    }

    /**
     * Initializes the file metadata.
     */
    protected void collectMeta() {
        Map<String, String> env = System.getenv();

        this.meta = new HashMap<>();

        for (String varName : env.keySet()) {
            if (varName.startsWith(ENV_META_PREFIX)) {
                String metaKey = varName.substring(ENV_META_PREFIX.length());

                if (!metaKey.isBlank()) {
                    this.meta.put(metaKey, System.getenv(varName));
                }
            }
        }
    }

    /**
     * Closes this persister and releases the currently open
     * file (if any).
     *
     * No further writing is possible.
     */
    @Override
    public void close() throws IOException {
        if (currentWriter != null) {
            synchronized (rotationLock) {
                closeWriter();
                currentWriter = null;
            }
        }
    }

    /**
     * Get the prefix used to 'name' output files.
     * Includes path and file name prefix.
     *
     * @return prefix used to create consecutive file names
     */
    public String getFileNamePrefix() {
        return baseFileName;
    }

    /**
     * Get the number of completed files.
     *
     * @return number of files written so far
     */
    public int getNumCompletedFiles() {
        return rotations;
    }

    /**
     * Get the number of records in the currently open parquet file.
     *
     * @return number of records written so far to the current file only
     */
    public long getCurrentNumCompletedRecords() {
        return currentNumWritten;
    }

    /**
     * Get the size of the currently open parquet file.
     *
     * @return number of bytes written so far to the current file only
     */
    public long getCurrentDataSize() {
        synchronized (rotationLock) {
            return currentWriter == null
                    ? 0
                    : currentWriter.getDataSize();
        }
    }
}
