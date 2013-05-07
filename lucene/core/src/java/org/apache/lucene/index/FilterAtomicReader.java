package org.apache.lucene.index;

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
import java.util.Comparator;
import java.util.Iterator;

import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**  A <code>FilterAtomicReader</code> contains another AtomicReader, which it
 * uses as its basic source of data, possibly transforming the data along the
 * way or providing additional functionality. The class
 * <code>FilterAtomicReader</code> itself simply implements all abstract methods
 * of <code>IndexReader</code> with versions that pass all requests to the
 * contained index reader. Subclasses of <code>FilterAtomicReader</code> may
 * further override some of these methods and may also provide additional
 * methods and fields.
 * <p><b>NOTE</b>: If you override {@link #getLiveDocs()}, you will likely need
 * to override {@link #numDocs()} as well and vice-versa.
 * <p><b>NOTE</b>: If this {@link FilterAtomicReader} does not change the
 * content the contained reader, you could consider overriding
 * {@link #getCoreCacheKey()} so that {@link FieldCache} and
 * {@link CachingWrapperFilter} share the same entries for this atomic reader
 * and the wrapped one. {@link #getCombinedCoreAndDeletesKey()} could be
 * overridden as well if the {@link #getLiveDocs() live docs} are not changed
 * either.
 */
public class FilterAtomicReader extends AtomicReader {

  /** Base class for filtering {@link Fields}
   *  implementations. */
  public static class FilterFields extends Fields {
    /** The underlying Fields instance. */
    protected final Fields in;

    /**
     * Creates a new FilterFields.
     * @param in the underlying Fields instance.
     */
    public FilterFields(Fields in) {
      this.in = in;
    }

    @Override
    public Iterator<String> iterator() {
      return in.iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      return in.terms(field);
    }

    @Override
    public int size() {
      return in.size();
    }
  }

  /** Base class for filtering {@link Terms} implementations.
   * <p><b>NOTE</b>: If the order of terms and documents is not changed, and if
   * these terms are going to be intersected with automata, you could consider
   * overriding {@link #intersect} for better performance.
   */
  public static class FilterTerms extends Terms {
    /** The underlying Terms instance. */
    protected final Terms in;

    /**
     * Creates a new FilterTerms
     * @param in the underlying Terms instance.
     */
    public FilterTerms(Terms in) {
      this.in = in;
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      return in.iterator(reuse);
    }
    
    @Override
    public Comparator<BytesRef> getComparator() {
      return in.getComparator();
    }

    @Override
    public long size() throws IOException {
      return in.size();
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

    @Override
    public boolean hasOffsets() {
      return in.hasOffsets();
    }

    @Override
    public boolean hasPositions() {
      return in.hasPositions();
    }
    
    @Override
    public boolean hasPayloads() {
      return in.hasPayloads();
    }
  }

  /** Base class for filtering {@link TermsEnum} implementations. */
  public static class FilterTermsEnum extends TermsEnum {
    /** The underlying TermsEnum instance. */
    protected final TermsEnum in;

    /**
     * Creates a new FilterTermsEnum
     * @param in the underlying TermsEnum instance.
     */
    public FilterTermsEnum(TermsEnum in) { this.in = in; }

    @Override
    public AttributeSource attributes() {
      return in.attributes();
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
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
      return in.docs(liveDocs, reuse, flags);
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
      return in.docsAndPositions(liveDocs, reuse, flags);
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return in.getComparator();
    }
  }

  /** Base class for filtering {@link DocsEnum} implementations. */
  public static class FilterDocsEnum extends DocsEnum {
    /** The underlying DocsEnum instance. */
    protected final DocsEnum in;

    /**
     * Create a new FilterDocsEnum
     * @param in the underlying DocsEnum instance.
     */
    public FilterDocsEnum(DocsEnum in) {
      this.in = in;
    }

    @Override
    public AttributeSource attributes() {
      return in.attributes();
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int freq() throws IOException {
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
    public long cost() {
      return in.cost();
    }
  }

  /** Base class for filtering {@link DocsAndPositionsEnum} implementations. */
  public static class FilterDocsAndPositionsEnum extends DocsAndPositionsEnum {
    /** The underlying DocsAndPositionsEnum instance. */
    protected final DocsAndPositionsEnum in;

    /**
     * Create a new FilterDocsAndPositionsEnum
     * @param in the underlying DocsAndPositionsEnum instance.
     */
    public FilterDocsAndPositionsEnum(DocsAndPositionsEnum in) {
      this.in = in;
    }

    @Override
    public AttributeSource attributes() {
      return in.attributes();
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int freq() throws IOException {
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
    public int startOffset() throws IOException {
      return in.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return in.endOffset();
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return in.getPayload();
    }
    
    @Override
    public long cost() {
      return in.cost();
    }
  }

  /** The underlying AtomicReader. */
  protected final AtomicReader in;

  /**
   * <p>Construct a FilterAtomicReader based on the specified base reader.
   * <p>Note that base reader is closed if this FilterAtomicReader is closed.</p>
   * @param in specified base reader.
   */
  public FilterAtomicReader(AtomicReader in) {
    super();
    this.in = in;
    in.registerParentReader(this);
  }

  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return in.getLiveDocs();
  }
  
  @Override
  public FieldInfos getFieldInfos() {
    return in.getFieldInfos();
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
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    ensureOpen();
    in.document(docID, visitor);
  }

  @Override
  protected void doClose() throws IOException {
    in.close();
  }
  
  @Override
  public Fields fields() throws IOException {
    ensureOpen();
    return in.fields();
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("FilterAtomicReader(");
    buffer.append(in);
    buffer.append(')');
    return buffer.toString();
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    return in.getNumericDocValues(field);
  }
  
  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    return in.getBinaryDocValues(field);
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    return in.getSortedDocValues(field);
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    return in.getSortedSetDocValues(field);
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    return in.getNormValues(field);
  }

}
