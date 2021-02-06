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
package org.apache.lucene.codecs;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.Accountable;

/**
 * Codec API for reading stored fields.
 *
 * <p>You need to implement {@link #visitDocument(int, StoredFieldVisitor)} to read the stored
 * fields for a document, implement {@link #clone()} (creating clones of any IndexInputs used, etc),
 * and {@link #close()}
 *
 * @lucene.experimental
 */
public abstract class StoredFieldsReader implements Cloneable, Closeable, Accountable {
  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected StoredFieldsReader() {}

  /** Visit the stored fields for document <code>docID</code> */
  public abstract void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException;

  @Override
  public abstract StoredFieldsReader clone();

  /**
   * Checks consistency of this reader.
   *
   * <p>Note that this may be costly in terms of I/O, e.g. may involve computing a checksum value
   * against large data files.
   *
   * @lucene.internal
   */
  public abstract void checkIntegrity() throws IOException;

  /**
   * Returns an instance optimized for merging. This instance may not be cloned.
   *
   * <p>The default implementation returns {@code this}
   */
  public StoredFieldsReader getMergeInstance() {
    return this;
  }

  /**
   * An option that allows the caller to select the range of document ids in the current fetching
   * range so that the detailed implementation can perform some optimization internally. For
   * example, {@link org.apache.lucene.codecs.compressing.CompressingStoredFieldsFormat} can use
   * this information to avoid decompressing the same compressed block multiple times.
   *
   * @lucene.experimental
   */
  public interface PrefetchOption {
    /**
     * This method **might** be called by the detailed implementation of {@link StoredFieldsReader}
     * created via {@link #getInstanceWithPrefetchOptions(PrefetchOption)} when it starts fetching a
     * new range of documents.
     *
     * <p>The caller uses the parameters provided by this method and document ids that it will read
     * to calculate the preferred range. For example, if the stored field reader starts fetching a
     * new range [startRangeDocId=10, endRangeDocId=50] and the caller is reading (25, 28, 26, 27,
     * 100) then the caller can return a preferred range [25-28] to tell the reader to "prefetch"
     * only this range.
     *
     * @param startRangeDocId the lower bound of document id of the new fetching range (inclusive)
     * @param endRangeDocId the upper bound of document id of the new fetching range (inclusive)
     * @param currentDocId the document id that is being current visited
     * @return a sub-range of the current fetching range that is optimal for the caller's interest.
     *     The returned range must include the {@code currentDocId}.
     */
    PrefetchRange preferFetchRange(int startRangeDocId, int endRangeDocId, int currentDocId);

    // TODO:
    // Should we also pass the state of the fetching range (i.e., offsets and lengths) so the caller
    // can make a better trade off? For example, if the fetching range [10, 50] and the caller is
    // reading (25, 26, 27, 28, 30, 31, 100), then the caller might choose to prefetch [25-28] or
    // [25-31] depending on the cost of decompressing the doc-29, which will be skipped, and the
    // cost of re-decompressing of doc-30 and doc-31.
  }

  /**
   * Returns an instance optimized for sequential access.
   *
   * @lucene.experimental
   */
  public StoredFieldsReader getInstanceWithPrefetchOptions(PrefetchOption prefetchOption) {
    return this;
  }

  /**
   * A {@link PrefetchOption} that always fetches the whole range. It's used by {@link
   * #getMergeInstance()}.
   */
  public static PrefetchOption MERGE_PREFETCH_OPTION =
      (minDocId, maxDocId, currentDocId) -> new PrefetchRange(minDocId, maxDocId);

  /**
   * Presents the preferred range that used in {@link PrefetchOption#preferFetchRange(int, int,
   * int)}
   */
  public static final class PrefetchRange {
    private final int fromDocId; // inclusive
    private final int toDocId; // inclusive

    /**
     * Creates a prefetch range
     *
     * @param fromDocId the inclusive lower bound of the prefetch range
     * @param toDocId the inclusive upper bound of the prefetch range
     */
    public PrefetchRange(int fromDocId, int toDocId) {
      if (fromDocId < 0 || fromDocId > toDocId) {
        throw new IllegalArgumentException(
            "invalid values: fromDocId=" + fromDocId + ", toDocId=" + toDocId);
      }
      this.fromDocId = fromDocId;
      this.toDocId = toDocId;
    }

    /** Returns the inclusive lower bound of the prefetch range */
    public int getFromDocId() {
      return fromDocId;
    }

    /** Returns the inclusive upper bound of the prefetch range */
    public int getToDocId() {
      return toDocId;
    }

    @Override
    public String toString() {
      return "fromDocId=" + fromDocId + ", toDocId=" + toDocId;
    }
  }
}
