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

import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.MapBackedSet;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

/**  A <code>FilterIndexReader</code> contains another IndexReader, which it
 * uses as its basic source of data, possibly transforming the data along the
 * way or providing additional functionality. The class
 * <code>FilterIndexReader</code> itself simply implements all abstract methods
 * of <code>IndexReader</code> with versions that pass all requests to the
 * contained index reader. Subclasses of <code>FilterIndexReader</code> may
 * further override some of these methods and may also provide additional
 * methods and fields.
 */
public class FilterIndexReader extends IndexReader {

  /** Base class for filtering {@link Fields}
   *  implementations. */
  public static class FilterFields extends Fields {
    protected Fields in;

    public FilterFields(Fields in) {
      this.in = in;
    }

    @Override
    public FieldsEnum iterator() throws IOException {
      return in.iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      return in.terms(field);
    }

    @Override
    public int getUniqueFieldCount() throws IOException {
      return in.getUniqueFieldCount();
    }
  }

  /** Base class for filtering {@link Terms}
   *  implementations. */
  public static class FilterTerms extends Terms {
    protected Terms in;

    public FilterTerms(Terms in) {
      this.in = in;
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      return in.iterator(reuse);
    }

    @Override
    public Comparator<BytesRef> getComparator() throws IOException {
      return in.getComparator();
    }

    @Override
    public int docFreq(BytesRef text) throws IOException {
      return in.docFreq(text);
    }

    @Override
    public DocsEnum docs(Bits liveDocs, BytesRef text, DocsEnum reuse) throws IOException {
      return in.docs(liveDocs, text, reuse);
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, BytesRef text, DocsAndPositionsEnum reuse) throws IOException {
      return in.docsAndPositions(liveDocs, text, reuse);
    }

    @Override
    public long getUniqueTermCount() throws IOException {
      return in.getUniqueTermCount();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
      return in.getSumTotalTermFreq();
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return in.getSumDocFreq();
    }

    @Override
    public int getDocCount() throws IOException {
      return in.getDocCount();
    }
  }

  /** Base class for filtering {@link TermsEnum} implementations. */
  public static class FilterFieldsEnum extends FieldsEnum {
    protected FieldsEnum in;
    public FilterFieldsEnum(FieldsEnum in) {
      this.in = in;
    }

    @Override
    public String next() throws IOException {
      return in.next();
    }

    @Override
    public Terms terms() throws IOException {
      return in.terms();
    }
  }

  /** Base class for filtering {@link TermsEnum} implementations. */
  public static class FilterTermsEnum extends TermsEnum {
    protected TermsEnum in;

    public FilterTermsEnum(TermsEnum in) { this.in = in; }

    @Override
    public boolean seekExact(BytesRef text, boolean useCache) throws IOException {
      return in.seekExact(text, useCache);
    }

    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
      return in.seekCeil(text, useCache);
    }

    @Override
    public void seekExact(long ord) throws IOException {
      in.seekExact(ord);
    }

    @Override
    public BytesRef next() throws IOException {
      return in.next();
    }

    @Override
    public BytesRef term() throws IOException {
      return in.term();
    }

    @Override
    public long ord() throws IOException {
      return in.ord();
    }

    @Override
    public int docFreq() throws IOException {
      return in.docFreq();
    }

    @Override
    public long totalTermFreq() throws IOException {
      return in.totalTermFreq();
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse) throws IOException {
      return in.docs(liveDocs, reuse);
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse) throws IOException {
      return in.docsAndPositions(liveDocs, reuse);
    }

    @Override
    public Comparator<BytesRef> getComparator() throws IOException {
      return in.getComparator();
    }

    @Override
    public void seekExact(BytesRef term, TermState state) throws IOException {
      in.seekExact(term, state);
    }

    @Override
    public TermState termState() throws IOException {
      return in.termState();
    }
  }

  /** Base class for filtering {@link DocsEnum} implementations. */
  public static class FilterDocsEnum extends DocsEnum {
    protected DocsEnum in;

    public FilterDocsEnum(DocsEnum in) {
      this.in = in;
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int freq() {
      return in.freq();
    }

    @Override
    public int nextDoc() throws IOException {
      return in.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return in.advance(target);
    }

    @Override
    public BulkReadResult getBulkResult() {
      return in.getBulkResult();
    }

    @Override
    public int read() throws IOException {
      return in.read();
    }
  }

  /** Base class for filtering {@link DocsAndPositionsEnum} implementations. */
  public static class FilterDocsAndPositionsEnum extends DocsAndPositionsEnum {
    protected DocsAndPositionsEnum in;

    public FilterDocsAndPositionsEnum(DocsAndPositionsEnum in) {
      this.in = in;
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int freq() {
      return in.freq();
    }

    @Override
    public int nextDoc() throws IOException {
      return in.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return in.advance(target);
    }

    @Override
    public int nextPosition() throws IOException {
      return in.nextPosition();
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return in.getPayload();
    }

    @Override
    public boolean hasPayload() {
      return in.hasPayload();
    }
  }

  protected IndexReader in;

  /**
   * <p>Construct a FilterIndexReader based on the specified base reader.
   * Directory locking for delete, undeleteAll, and setNorm operations is
   * left to the base reader.</p>
   * <p>Note that base reader is closed if this FilterIndexReader is closed.</p>
   * @param in specified base reader.
   */
  public FilterIndexReader(IndexReader in) {
    super();
    this.in = in;
    readerFinishedListeners = new MapBackedSet<ReaderFinishedListener>(new ConcurrentHashMap<ReaderFinishedListener,Boolean>());
  }

  @Override
  public Directory directory() {
    ensureOpen();
    return in.directory();
  }
  
  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return in.getLiveDocs();
  }
  
  @Override
  public Fields getTermVectors(int docID)
          throws IOException {
    ensureOpen();
    return in.getTermVectors(docID);
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return in.numDocs();
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return in.maxDoc();
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
    ensureOpen();
    in.document(docID, visitor);
  }

  @Override
  public boolean hasDeletions() {
    ensureOpen();
    return in.hasDeletions();
  }

  @Override
  protected void doUndeleteAll() throws CorruptIndexException, IOException {in.undeleteAll();}

  @Override
  public boolean hasNorms(String field) throws IOException {
    ensureOpen();
    return in.hasNorms(field);
  }

  @Override
  public byte[] norms(String f) throws IOException {
    ensureOpen();
    return in.norms(f);
  }

  @Override
  protected void doSetNorm(int d, String f, byte b) throws CorruptIndexException, IOException {
    in.setNorm(d, f, b);
  }

  @Override
  public int docFreq(Term t) throws IOException {
    ensureOpen();
    return in.docFreq(t);
  }

  @Override
  public int docFreq(String field, BytesRef t) throws IOException {
    ensureOpen();
    return in.docFreq(field, t);
  }

  @Override
  protected void doDelete(int n) throws  CorruptIndexException, IOException { in.deleteDocument(n); }
  
  @Override
  protected void doCommit(Map<String,String> commitUserData) throws IOException {
    in.commit(commitUserData);
  }
  
  @Override
  protected void doClose() throws IOException {
    in.close();
  }

  @Override
  public Collection<String> getFieldNames(IndexReader.FieldOption fieldNames) {
    ensureOpen();
    return in.getFieldNames(fieldNames);
  }

  @Override
  public long getVersion() {
    ensureOpen();
    return in.getVersion();
  }

  @Override
  public boolean isCurrent() throws CorruptIndexException, IOException {
    ensureOpen();
    return in.isCurrent();
  }
  
  @Override
  public IndexReader[] getSequentialSubReaders() {
    return in.getSequentialSubReaders();
  }
  
  @Override
  public ReaderContext getTopReaderContext() {
    ensureOpen();
    return in.getTopReaderContext();
  }

  @Override
  public Map<String, String> getCommitUserData() { 
    return in.getCommitUserData();
  }
  
  @Override
  public Fields fields() throws IOException {
    ensureOpen();
    return in.fields();
  }

  /** If the subclass of FilteredIndexReader modifies the
   *  contents of the FieldCache, you must override this
   *  method to provide a different key */
  @Override
  public Object getCoreCacheKey() {
    return in.getCoreCacheKey();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("FilterReader(");
    buffer.append(in);
    buffer.append(')');
    return buffer.toString();
  }

  @Override
  public void addReaderFinishedListener(ReaderFinishedListener listener) {
    super.addReaderFinishedListener(listener);
    in.addReaderFinishedListener(listener);
  }

  @Override
  public void removeReaderFinishedListener(ReaderFinishedListener listener) {
    super.removeReaderFinishedListener(listener);
    in.removeReaderFinishedListener(listener);
  }

  @Override
  public PerDocValues perDocValues() throws IOException {
    ensureOpen();
    return in.perDocValues();
  }
}
