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

package org.apache.solr.search;


import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

/** Solr wrapper for IndexReader that contains extra context.
 * This is currently experimental, for internal use only, and subject to change.
 */
public class SolrIndexReader extends FilterIndexReader {
  private final SolrIndexReader[] subReaders;
  private final SolrIndexReader parent;
  private final int base; // docid offset of this reader within parent

  // top level searcher for this reader tree
  // a bit if a hack currently... searcher needs to set
  SolrIndexSearcher searcher;

  // Shared info about the wrapped reader.
  private SolrReaderInfo info;

  /** Recursively wrap an IndexReader in SolrIndexReader instances.
   * @param in  the reader to wrap
   * @param parent the parent, if any (null if none)
   * @param base the docid offset in the parent (0 if top level)
   */
  public SolrIndexReader(IndexReader in, SolrIndexReader parent, int base) {
    super(in);
    assert(!(in instanceof SolrIndexReader));
    this.parent = parent;
    this.base = base;
    IndexReader subs[] = in.getSequentialSubReaders();
    subReaders = subs == null ? null : new SolrIndexReader[subs.length];
    if (subs != null) {
      int b=0;
      for (int i=0; i<subReaders.length; i++) {
        subReaders[i] = new SolrIndexReader(subs[i], this, b);
        b += subReaders[i].maxDoc();
      }
    }
  }

  static String shortName(Object o) {
    return o.getClass().getSimpleName()+ "@" + Integer.toHexString(o.hashCode());
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SolrIndexReader{this=").append(Integer.toHexString(this.hashCode()));
    sb.append(",r=").append(shortName(in));
    sb.append(",segments=");
    sb.append(subReaders == null ? 1 : subReaders.length);
    if (parent != null) {
      sb.append(",parent=").append(parent.toString());
    }
    sb.append('}');
    return sb.toString();
  }

  static void setSearcher(SolrIndexReader sr, SolrIndexSearcher searcher) {
    sr.searcher = searcher;
    SolrIndexReader[] readers = sr.getSequentialSubReaders();
    if (readers == null) return;
    for (SolrIndexReader r : readers) {
      setSearcher(r, searcher);
    }
  }

   private static void buildInfoMap(SolrIndexReader other, HashMap<IndexReader, SolrReaderInfo> map) {
     if (other == null) return;
     map.put(other.getWrappedReader(), other.info);
     SolrIndexReader[] readers = other.getSequentialSubReaders();
     if (readers == null) return;
     for (SolrIndexReader r : readers) {
       buildInfoMap(r, map);
     }     
   }

   private static void setInfo(SolrIndexReader target, HashMap<IndexReader, SolrReaderInfo> map) {
     SolrReaderInfo info = map.get(target.getWrappedReader());
     if (info == null) info = new SolrReaderInfo(target.getWrappedReader());
     target.info = info;
     SolrIndexReader[] readers = target.getSequentialSubReaders();
     if (readers == null) return;
     for (SolrIndexReader r : readers) {
       setInfo(r, map);
     }     
   }

   /** Copies SolrReaderInfo instances from the source to this SolrIndexReader */
   public void associateInfo(SolrIndexReader source) {
     // seemed safer to not mess with reopen() but simply set
     // one set of caches from another reader tree.
     HashMap<IndexReader, SolrReaderInfo> map = new HashMap<IndexReader, SolrReaderInfo>();
     buildInfoMap(source, map);
     setInfo(this, map);
   }

  public IndexReader getWrappedReader() {
    return in;
  }

  /** returns the parent reader, or null of none */
  public SolrIndexReader getParent() {
    return parent;
  }

   /** returns the docid offset within the parent reader */
  public int getBase() {
    return base;
  }

  @Override
  public Directory directory() {
    return in.directory();
  }

  @Override
  public TermFreqVector[] getTermFreqVectors(int docNumber) throws IOException {
    return in.getTermFreqVectors(docNumber);
  }

  @Override
  public TermFreqVector getTermFreqVector(int docNumber, String field)
          throws IOException {
    return in.getTermFreqVector(docNumber, field);
  }

  @Override
  public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
    in.getTermFreqVector(docNumber, field, mapper);

  }

  @Override
  public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
    in.getTermFreqVector(docNumber, mapper);
  }

  @Override
  public int numDocs() {
    return in.numDocs();
  }

  @Override
  public int maxDoc() {
    return in.maxDoc();
  }

  @Override
  public Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    return in.document(n, fieldSelector);
  }

  @Override
  public boolean isDeleted(int n) {
    return in.isDeleted(n);
  }

  @Override
  public boolean hasDeletions() {
    return in.hasDeletions();
  }

  @Override
  protected void doUndeleteAll() throws CorruptIndexException, IOException {in.undeleteAll();}

  @Override
  public boolean hasNorms(String field) throws IOException {
    return in.hasNorms(field);
  }

  @Override
  public byte[] norms(String f) throws IOException {
    return in.norms(f);
  }

  @Override
  public void norms(String f, byte[] bytes, int offset) throws IOException {
    in.norms(f, bytes, offset);
  }

  @Override
  protected void doSetNorm(int d, String f, byte b) throws CorruptIndexException, IOException {
    in.setNorm(d, f, b);
  }

  @Override
  public TermEnum terms() throws IOException {
    return in.terms();
  }

  @Override
  public TermEnum terms(Term t) throws IOException {
    return in.terms(t);
  }

  @Override
  public int docFreq(Term t) throws IOException {
    ensureOpen();
    return in.docFreq(t);
  }

  @Override
  public TermDocs termDocs() throws IOException {
    ensureOpen();
    return in.termDocs();
  }

  @Override
  public TermDocs termDocs(Term term) throws IOException {
    ensureOpen();
    return in.termDocs(term);
  }

  @Override
  public TermPositions termPositions() throws IOException {
    ensureOpen();
    return in.termPositions();
  }

  @Override
  protected void doDelete(int n) throws  CorruptIndexException, IOException { in.deleteDocument(n); }

  // Let FilterIndexReader handle commit()... we cannot override commit()
  // or call in.commit() ourselves.
  // protected void doCommit() throws IOException { in.commit(); }

  @Override
  protected void doClose() throws IOException { in.close(); }

  @Override
  public Collection getFieldNames(IndexReader.FieldOption fieldNames) {
    return in.getFieldNames(fieldNames);
  }

  @Override
  public long getVersion() {
    return in.getVersion();
  }

  @Override
  public boolean isCurrent() throws CorruptIndexException, IOException {
    return in.isCurrent();
  }

  @Override
  public boolean isOptimized() {
    return in.isOptimized();
  }

  @Override
  public SolrIndexReader[] getSequentialSubReaders() {
    return subReaders;
  }

  @Override
  public int hashCode() {
    return in.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SolrIndexReader) {
      o = ((SolrIndexReader)o).in;
    }
    return in.equals(o);
  }

  @Override
  public SolrIndexReader reopen(boolean openReadOnly) throws IOException {
    IndexReader r = in.reopen(openReadOnly);
    if (r == in) {
      return this;
    }
    SolrIndexReader sr = new SolrIndexReader(r, null, 0);
    sr.associateInfo(this);
    return sr;
  }

  @Override
  public SolrIndexReader reopen() throws CorruptIndexException, IOException {
    return reopen(true);
  }

  @Override
  public void decRef() throws IOException {
    in.decRef();
  }

  @Override
  public void deleteDocument(int docNum) throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException {
    in.deleteDocument(docNum);
  }

  @Override
  public int deleteDocuments(Term term) throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException {
    return in.deleteDocuments(term);
  }

  @Override
  public Document document(int n) throws CorruptIndexException, IOException {
    return in.document(n);
  }

  @Override
  public String getCommitUserData() {
    return in.getCommitUserData();
  }

  @Override
  public IndexCommit getIndexCommit() throws IOException {
    return in.getIndexCommit();
  }

  @Override
  public int getTermInfosIndexDivisor() {
    return in.getTermInfosIndexDivisor();
  }

  @Override
  public void incRef() {
    in.incRef();
  }

  @Override
  public int numDeletedDocs() {
    return in.numDeletedDocs();
  }

  @Override
  public void setNorm(int doc, String field, byte value) throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException {
    in.setNorm(doc, field, value);
  }

  @Override
  public void setNorm(int doc, String field, float value) throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException {
    in.setNorm(doc, field, value);
  }

  @Override
  public void setTermInfosIndexDivisor(int indexDivisor) throws IllegalStateException {
    in.setTermInfosIndexDivisor(indexDivisor);
  }

  @Override
  public TermPositions termPositions(Term term) throws IOException {
    return in.termPositions(term);
  }

  @Override
  public void undeleteAll() throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException {
    in.undeleteAll();
  }
}



/** SolrReaderInfo contains information that is the same for
 * every SolrIndexReader that wraps the same IndexReader.
 * Multiple SolrIndexReader instances will be accessing this
 * class concurrently.
 */
class SolrReaderInfo {
  private final IndexReader reader;
  public SolrReaderInfo(IndexReader reader) {
    this.reader = reader;
  }
  public IndexReader getReader() { return reader; }

}