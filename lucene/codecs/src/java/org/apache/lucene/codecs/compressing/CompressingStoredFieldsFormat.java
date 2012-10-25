package org.apache.lucene.codecs.compressing;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * A {@link StoredFieldsFormat} that is very similar to
 * {@link Lucene40StoredFieldsFormat} but compresses documents in chunks in
 * order to improve compression ratio.
 * <p>
 * For optimal performance, you should use a {@link MergePolicy} that returns
 * segments that have the biggest byte size first.
 * @lucene.experimental
 */
public class CompressingStoredFieldsFormat extends StoredFieldsFormat {

  private final CompressingStoredFieldsIndex storedFieldsIndex;
  private final CompressionMode compressionMode;
  private final int chunkSize;

  /**
   * Create a new {@link CompressingStoredFieldsFormat}.
   * <p>
   * The <code>compressionMode</code> parameter allows you to choose between
   * compression algorithms that have various compression and uncompression
   * speeds so that you can pick the one that best fits your indexing and
   * searching throughput.
   * <p>
   * <code>chunkSize</code> is the minimum byte size of a chunk of documents.
   * A value of <code>1</code> can make sense if there is redundancy across
   * fields. In that case, both performance and compression ratio should be
   * better than with {@link Lucene40StoredFieldsFormat} with compressed
   * fields.
   * <p>
   * Higher values of <code>chunkSize</code> should improve the compression
   * atio but will require more memory at indexing time and might make document
   * loading a little slower (depending on the size of your OS cache compared
   * to the size of your index).
   * <p>
   * The <code>storedFieldsIndex</code> parameter allows you to choose between
   * several fields index implementations that offer various trade-offs between
   * memory usage and speed.
   *
   * @param compressionMode the {@link CompressionMode} to use
   * @param chunkSize the minimum number of bytes of a single chunk of stored documents
   * @param storedFieldsIndex the fields index impl to use
   * @see CompressionMode
   * @see CompressingStoredFieldsIndex
   */
  public CompressingStoredFieldsFormat(CompressionMode compressionMode, int chunkSize,
      CompressingStoredFieldsIndex storedFieldsIndex) {
    this.compressionMode = compressionMode;
    if (chunkSize < 1) {
      throw new IllegalArgumentException("chunkSize must be >= 1");
    }
    this.chunkSize = chunkSize;
    this.storedFieldsIndex = storedFieldsIndex;
  }

  /**
   * Create a new {@link CompressingStoredFieldsFormat} with an in-memory
   * {@link CompressingStoredFieldsIndex}.
   *
   * @see CompressingStoredFieldsFormat#CompressingStoredFieldsFormat(CompressionMode, int, CompressingStoredFieldsIndex)
   */
  public CompressingStoredFieldsFormat(CompressionMode compressionMode, int chunkSize) {
    this (compressionMode, chunkSize, chunkSize == 1
        ? CompressingStoredFieldsIndex.MEMORY_DOC
        : CompressingStoredFieldsIndex.MEMORY_CHUNK);
  }

  /**
   * Create a new {@link CompressingStoredFieldsFormat} with
   * {@link CompressionMode#FAST} compression and chunks of <tt>16 KB</tt>.
   *
   * @see CompressingStoredFieldsFormat#CompressingStoredFieldsFormat(CompressionMode, int)
   */
  public CompressingStoredFieldsFormat() {
    this(CompressionMode.FAST, 1 << 14);
  }

  @Override
  public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si,
      FieldInfos fn, IOContext context) throws IOException {
    return new CompressingStoredFieldsReader(directory, si, fn, context);
  }

  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si,
      IOContext context) throws IOException {
    return new CompressingStoredFieldsWriter(directory, si, context,
        compressionMode, chunkSize, storedFieldsIndex);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(compressionMode=" + compressionMode
        + ", chunkSize=" + chunkSize + ", storedFieldsIndex=" + storedFieldsIndex + ")";
  }

}
