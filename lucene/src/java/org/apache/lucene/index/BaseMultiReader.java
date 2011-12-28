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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ReaderUtil;

abstract class BaseMultiReader<R extends IndexReader> extends IndexReader {
  protected final R[] subReaders;
  protected final int[] starts;       // 1st docno for each segment
  private final ReaderContext topLevelContext;
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
    topLevelContext = ReaderUtil.buildReaderContext(this);
  }
  
  @Override
  public Fields fields() throws IOException {
    throw new UnsupportedOperationException("please use MultiFields.getFields, or wrap your IndexReader with SlowMultiReaderWrapper, if you really need a top level Fields");
  }

  @Override
  protected abstract IndexReader doOpenIfChanged() throws CorruptIndexException, IOException;
  
  @Override
  public Bits getLiveDocs() {
    throw new UnsupportedOperationException("please use MultiFields.getLiveDocs, or wrap your IndexReader with SlowMultiReaderWrapper, if you really need a top level Bits liveDocs");
  }

  @Override
  public Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    final int i = readerIndex(docID);        // find segment num
    return subReaders[i].getTermVectors(docID - starts[i]); // dispatch to segment
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return numDocs;
  }

  @Override
  public final int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return maxDoc;
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
    ensureOpen();
    final int i = readerIndex(docID);                          // find segment num
    subReaders[i].document(docID - starts[i], visitor);    // dispatch to segment reader
  }

  @Override
  public boolean hasDeletions() {
    // Don't call ensureOpen() here (it could affect performance)
    return hasDeletions;
  }

  /** Helper method for subclasses to get the corresponding reader for a doc ID */
  protected final int readerIndex(int docID) {
    if (docID < 0 || docID >= maxDoc) {
      throw new IllegalArgumentException("docID must be >= 0 and < maxDoc=" + maxDoc + " (got docID=" + docID + ")");
    }
    return ReaderUtil.subIndex(docID, this.starts);
  }

  @Override
  public boolean hasNorms(String field) throws IOException {
    ensureOpen();
    for (int i = 0; i < subReaders.length; i++) {
      if (subReaders[i].hasNorms(field)) return true;
    }
    return false;
  }
  
  @Override
  public synchronized byte[] norms(String field) throws IOException {
    throw new UnsupportedOperationException("please use MultiNorms.norms, or wrap your IndexReader with SlowMultiReaderWrapper, if you really need a top level norms");
  }
  
  @Override
  public int docFreq(String field, BytesRef t) throws IOException {
    ensureOpen();
    int total = 0;          // sum freqs in segments
    for (int i = 0; i < subReaders.length; i++) {
      total += subReaders[i].docFreq(field, t);
    }
    return total;
  }

  @Override
  public Collection<String> getFieldNames (IndexReader.FieldOption fieldNames) {
    ensureOpen();
    // maintain a unique set of field names
    final Set<String> fieldSet = new HashSet<String>();
    for (IndexReader reader : subReaders) {
      fieldSet.addAll(reader.getFieldNames(fieldNames));
    }
    return fieldSet;
  }  

  @Override
  public IndexReader[] getSequentialSubReaders() {
    return subReaders;
  }
  
  @Override
  public ReaderContext getTopReaderContext() {
    return topLevelContext;
  }
  
  @Override
  public DocValues docValues(String field) throws IOException {
    throw new UnsupportedOperationException("please use MultiDocValues#getDocValues, or wrap your IndexReader with SlowMultiReaderWrapper, if you really need a top level DocValues");
  }
}
