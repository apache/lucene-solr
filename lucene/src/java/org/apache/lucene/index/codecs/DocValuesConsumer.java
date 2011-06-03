package org.apache.lucene.index.codecs;

/**
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
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.values.IndexDocValues;
import org.apache.lucene.index.values.PerDocFieldValues;
import org.apache.lucene.index.values.Writer;
import org.apache.lucene.util.Bits;

/**
 * Abstract API that consumes {@link PerDocFieldValues}.
 * {@link DocValuesConsumer} are always associated with a specific field and
 * segments. Concrete implementations of this API write the given
 * {@link PerDocFieldValues} into a implementation specific format depending on
 * the fields meta-data.
 * 
 * @lucene.experimental
 */
public abstract class DocValuesConsumer {
  // TODO this might need to go in the codec package since is a direct relative
  // to TermsConsumer
  protected final AtomicLong bytesUsed;

  /**
   * Creates a new {@link DocValuesConsumer}.
   * 
   * @param bytesUsed
   *          bytes-usage tracking reference used by implementation to track
   *          internally allocated memory. All tracked bytes must be released
   *          once {@link #finish(int)} has been called.
   */
  protected DocValuesConsumer(AtomicLong bytesUsed) {
    this.bytesUsed = bytesUsed == null ? new AtomicLong(0) : bytesUsed;
  }

  /**
   * Adds the given {@link PerDocFieldValues} instance to this
   * {@link DocValuesConsumer}
   * 
   * @param docID
   *          the document ID to add the value for. The docID must always
   *          increase or be <tt>0</tt> if it is the first call to this method.
   * @param docValues
   *          the values to add
   * @throws IOException
   *           if an {@link IOException} occurs
   */
  public abstract void add(int docID, PerDocFieldValues docValues)
      throws IOException;

  /**
   * Called when the consumer of this API is doc with adding
   * {@link PerDocFieldValues} to this {@link DocValuesConsumer}
   * 
   * @param docCount
   *          the total number of documents in this {@link DocValuesConsumer}.
   *          Must be greater than or equal the last given docID to
   *          {@link #add(int, PerDocFieldValues)}.
   * @throws IOException
   */
  public abstract void finish(int docCount) throws IOException;

  /**
   * Gathers files associated with this {@link DocValuesConsumer}
   * 
   * @param files
   *          the of files to add the consumers files to.
   */
  public abstract void files(Collection<String> files) throws IOException;

  /**
   * Merges the given {@link org.apache.lucene.index.codecs.MergeState} into
   * this {@link DocValuesConsumer}.
   * 
   * @param mergeState
   *          the state to merge
   * @param values
   *          the docValues to merge in
   * @throws IOException
   *           if an {@link IOException} occurs
   */
  public void merge(org.apache.lucene.index.codecs.MergeState mergeState,
      IndexDocValues values) throws IOException {
    assert mergeState != null;
    // TODO we need some kind of compatibility notation for values such
    // that two slightly different segments can be merged eg. fixed vs.
    // variable byte len or float32 vs. float64
    int docBase = 0;
    boolean merged = false;
    /*
     * We ignore the given DocValues here and merge from the subReaders directly
     * to support bulk copies on the DocValues Writer level. if this gets merged
     * with MultiDocValues the writer can not optimize for bulk-copyable data
     */
    for (final IndexReader reader : mergeState.readers) {
      final IndexDocValues r = reader.docValues(mergeState.fieldInfo.name);
      if (r != null) {
        merged = true;
        merge(new Writer.MergeState(r, docBase, reader.maxDoc(), reader
            .getDeletedDocs()));
      }
      docBase += reader.numDocs();
    }
    if (merged) {
      finish(mergeState.mergedDocCount);
    }
  }

  /**
   * Merges the given {@link MergeState} into this {@link DocValuesConsumer}.
   * {@link MergeState#docBase} must always be increasing. Merging segments out
   * of order is not supported.
   * 
   * @param mergeState
   *          the {@link MergeState} to merge
   * @throws IOException
   *           if an {@link IOException} occurs
   */
  protected abstract void merge(MergeState mergeState) throws IOException;

  /**
   * Specialized auxiliary MergeState is necessary since we don't want to
   * exploit internals up to the codecs consumer. An instance of this class is
   * created for each merged low level {@link IndexReader} we are merging to
   * support low level bulk copies.
   */
  public static class MergeState {
    /**
     * the source reader for this MergeState - merged values should be read from
     * this instance
     */
    public final IndexDocValues reader;
    /** the absolute docBase for this MergeState within the resulting segment */
    public final int docBase;
    /** the number of documents in this MergeState */
    public final int docCount;
    /** the deleted bits for this MergeState */
    public final Bits bits;

    public MergeState(IndexDocValues reader, int docBase, int docCount, Bits bits) {
      assert reader != null;
      this.reader = reader;
      this.docBase = docBase;
      this.docCount = docCount;
      this.bits = bits;
    }
  }
}
