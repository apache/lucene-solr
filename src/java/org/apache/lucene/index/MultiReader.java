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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;

import java.io.IOException;
import java.util.Collection;
import java.util.Hashtable;

import org.apache.lucene.index.MultiSegmentReader.MultiTermDocs;
import org.apache.lucene.index.MultiSegmentReader.MultiTermEnum;
import org.apache.lucene.index.MultiSegmentReader.MultiTermPositions;

/** An IndexReader which reads multiple indexes, appending their content.
 *
 * @version $Id$
 */
public class MultiReader extends IndexReader {
  protected IndexReader[] subReaders;
  private int[] starts;                           // 1st docno for each segment
  private Hashtable normsCache = new Hashtable();
  private int maxDoc = 0;
  private int numDocs = -1;
  private boolean hasDeletions = false;
  
 /**
  * <p>Construct a MultiReader aggregating the named set of (sub)readers.
  * Directory locking for delete, undeleteAll, and setNorm operations is
  * left to the subreaders. </p>
  * <p>Note that all subreaders are closed if this Multireader is closed.</p>
  * @param subReaders set of (sub)readers
  * @throws IOException
  */
  public MultiReader(IndexReader[] subReaders) {
    initialize(subReaders);
  }

  
  private void initialize(IndexReader[] subReaders) {
    this.subReaders = subReaders;
    starts = new int[subReaders.length + 1];    // build starts array
    for (int i = 0; i < subReaders.length; i++) {
      starts[i] = maxDoc;
      maxDoc += subReaders[i].maxDoc();      // compute maxDocs

      if (subReaders[i].hasDeletions())
        hasDeletions = true;
    }
    starts[subReaders.length] = maxDoc;
  }


  public TermFreqVector[] getTermFreqVectors(int n) throws IOException {
    ensureOpen();
    int i = readerIndex(n);        // find segment num
    return subReaders[i].getTermFreqVectors(n - starts[i]); // dispatch to segment
  }

  public TermFreqVector getTermFreqVector(int n, String field)
      throws IOException {
    ensureOpen();
    int i = readerIndex(n);        // find segment num
    return subReaders[i].getTermFreqVector(n - starts[i], field);
  }


  public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
    ensureOpen();
    int i = readerIndex(docNumber);        // find segment num
    subReaders[i].getTermFreqVector(docNumber - starts[i], field, mapper);
  }

  public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
    ensureOpen();
    int i = readerIndex(docNumber);        // find segment num
    subReaders[i].getTermFreqVector(docNumber - starts[i], mapper);
  }

  public boolean isOptimized() {
    return false;
  }
  
  public synchronized int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    if (numDocs == -1) {        // check cache
      int n = 0;                // cache miss--recompute
      for (int i = 0; i < subReaders.length; i++)
        n += subReaders[i].numDocs();      // sum from readers
      numDocs = n;
    }
    return numDocs;
  }

  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return maxDoc;
  }

  // inherit javadoc
  public Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    ensureOpen();
    int i = readerIndex(n);                          // find segment num
    return subReaders[i].document(n - starts[i], fieldSelector);    // dispatch to segment reader
  }

  public boolean isDeleted(int n) {
    // Don't call ensureOpen() here (it could affect performance)
    int i = readerIndex(n);                           // find segment num
    return subReaders[i].isDeleted(n - starts[i]);    // dispatch to segment reader
  }

  public boolean hasDeletions() {
    // Don't call ensureOpen() here (it could affect performance)
    return hasDeletions;
  }

  protected void doDelete(int n) throws CorruptIndexException, IOException {
    numDocs = -1;                             // invalidate cache
    int i = readerIndex(n);                   // find segment num
    subReaders[i].deleteDocument(n - starts[i]);      // dispatch to segment reader
    hasDeletions = true;
  }

  protected void doUndeleteAll() throws CorruptIndexException, IOException {
    for (int i = 0; i < subReaders.length; i++)
      subReaders[i].undeleteAll();

    hasDeletions = false;
    numDocs = -1;                                 // invalidate cache
  }

  private int readerIndex(int n) {    // find reader for doc n:
    return MultiSegmentReader.readerIndex(n, this.starts, this.subReaders.length);
  }
  
  public boolean hasNorms(String field) throws IOException {
    ensureOpen();
    for (int i = 0; i < subReaders.length; i++) {
      if (subReaders[i].hasNorms(field)) return true;
    }
    return false;
  }

  private byte[] ones;
  private byte[] fakeNorms() {
    if (ones==null) ones=SegmentReader.createFakeNorms(maxDoc());
    return ones;
  }
  
  public synchronized byte[] norms(String field) throws IOException {
    ensureOpen();
    byte[] bytes = (byte[])normsCache.get(field);
    if (bytes != null)
      return bytes;          // cache hit
    if (!hasNorms(field))
      return fakeNorms();

    bytes = new byte[maxDoc()];
    for (int i = 0; i < subReaders.length; i++)
      subReaders[i].norms(field, bytes, starts[i]);
    normsCache.put(field, bytes);      // update cache
    return bytes;
  }

  public synchronized void norms(String field, byte[] result, int offset)
    throws IOException {
    ensureOpen();
    byte[] bytes = (byte[])normsCache.get(field);
    if (bytes==null && !hasNorms(field)) bytes=fakeNorms();
    if (bytes != null)                            // cache hit
      System.arraycopy(bytes, 0, result, offset, maxDoc());

    for (int i = 0; i < subReaders.length; i++)      // read from segments
      subReaders[i].norms(field, result, offset + starts[i]);
  }

  protected void doSetNorm(int n, String field, byte value)
    throws CorruptIndexException, IOException {
    normsCache.remove(field);                         // clear cache
    int i = readerIndex(n);                           // find segment num
    subReaders[i].setNorm(n-starts[i], field, value); // dispatch
  }

  public TermEnum terms() throws IOException {
    ensureOpen();
    return new MultiTermEnum(subReaders, starts, null);
  }

  public TermEnum terms(Term term) throws IOException {
    ensureOpen();
    return new MultiTermEnum(subReaders, starts, term);
  }

  public int docFreq(Term t) throws IOException {
    ensureOpen();
    int total = 0;          // sum freqs in segments
    for (int i = 0; i < subReaders.length; i++)
      total += subReaders[i].docFreq(t);
    return total;
  }

  public TermDocs termDocs() throws IOException {
    ensureOpen();
    return new MultiTermDocs(subReaders, starts);
  }

  public TermPositions termPositions() throws IOException {
    ensureOpen();
    return new MultiTermPositions(subReaders, starts);
  }

  protected void doCommit() throws IOException {
    for (int i = 0; i < subReaders.length; i++)
      subReaders[i].commit();
  }

  protected synchronized void doClose() throws IOException {
    for (int i = 0; i < subReaders.length; i++)
      subReaders[i].close();
  }

  public Collection getFieldNames (IndexReader.FieldOption fieldNames) {
    ensureOpen();
    return MultiSegmentReader.getFieldNames(fieldNames, this.subReaders);
  }  
  
  /**
   * Checks recursively if all subreaders are up to date. 
   */
  public boolean isCurrent() throws CorruptIndexException, IOException {
    for (int i = 0; i < subReaders.length; i++) {
      if (!subReaders[i].isCurrent()) {
        return false;
      }
    }
    
    // all subreaders are up to date
    return true;
  }
  
  /** Not implemented.
   * @throws UnsupportedOperationException
   */
  public long getVersion() {
    throw new UnsupportedOperationException("MultiReader does not support this method.");
  }
}
