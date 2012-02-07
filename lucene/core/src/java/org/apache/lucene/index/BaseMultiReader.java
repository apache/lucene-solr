package org.apache.lucene.index;

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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ReaderUtil;

abstract class BaseMultiReader<R extends IndexReader> extends CompositeReader {
  protected final R[] subReaders;
  protected final int[] starts;       // 1st docno for each reader
  private final int maxDoc;
  private final int numDocs;
  private final boolean hasDeletions;
  
  protected BaseMultiReader(R[] subReaders) throws IOException {
    this.subReaders = subReaders;
    starts = new int[subReaders.length + 1];    // build starts array
    int maxDoc = 0, numDocs = 0;
    boolean hasDeletions = false;
    for (int i = 0; i < subReaders.length; i++) {
      starts[i] = maxDoc;
      maxDoc += subReaders[i].maxDoc();      // compute maxDocs
      numDocs += subReaders[i].numDocs();    // compute numDocs
      if (subReaders[i].hasDeletions()) {
        hasDeletions = true;
      }
    }
    starts[subReaders.length] = maxDoc;
    this.maxDoc = maxDoc;
    this.numDocs = numDocs;
    this.hasDeletions = hasDeletions;
  }

  @Override
  public final Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    final int i = readerIndex(docID);        // find segment num
    return subReaders[i].getTermVectors(docID - starts[i]); // dispatch to segment
  }

  @Override
  public final int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return numDocs;
  }

  @Override
  public final int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return maxDoc;
  }

  @Override
  public final void document(int docID, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
    ensureOpen();
    final int i = readerIndex(docID);                          // find segment num
    subReaders[i].document(docID - starts[i], visitor);    // dispatch to segment reader
  }

  @Override
  public final boolean hasDeletions() {
    // Don't call ensureOpen() here (it could affect performance)
    return hasDeletions;
  }

  @Override
  public final int docFreq(String field, BytesRef t) throws IOException {
    ensureOpen();
    int total = 0;          // sum freqs in segments
    for (int i = 0; i < subReaders.length; i++) {
      total += subReaders[i].docFreq(field, t);
    }
    return total;
  }

  /** Helper method for subclasses to get the corresponding reader for a doc ID */
  protected final int readerIndex(int docID) {
    if (docID < 0 || docID >= maxDoc) {
      throw new IllegalArgumentException("docID must be >= 0 and < maxDoc=" + maxDoc + " (got docID=" + docID + ")");
    }
    return ReaderUtil.subIndex(docID, this.starts);
  }
  
  @Override
  public final R[] getSequentialSubReaders() {
    return subReaders;
  }
}
