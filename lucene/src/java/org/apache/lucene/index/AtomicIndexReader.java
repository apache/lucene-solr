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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.CompositeIndexReader.CompositeReaderContext;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.search.SearcherManager; // javadocs
import org.apache.lucene.store.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ReaderUtil;         // for javadocs

/** IndexReader is an abstract class, providing an interface for accessing an
 index.  Search of an index is done entirely through this abstract interface,
 so that any subclass which implements it is searchable.

 <p> Concrete subclasses of IndexReader are usually constructed with a call to
 one of the static <code>open()</code> methods, e.g. {@link
 #open(Directory)}.

 <p> For efficiency, in this API documents are often referred to via
 <i>document numbers</i>, non-negative integers which each name a unique
 document in the index.  These document numbers are ephemeral--they may change
 as documents are added to and deleted from an index.  Clients should thus not
 rely on a given document having the same number between sessions.

 <p>
 <b>NOTE</b>: for backwards API compatibility, several methods are not listed 
 as abstract, but have no useful implementations in this base class and 
 instead always throw UnsupportedOperationException.  Subclasses are 
 strongly encouraged to override these methods, but in many cases may not 
 need to.
 </p>

 <p>

 <a name="thread-safety"></a><p><b>NOTE</b>: {@link
 IndexReader} instances are completely thread
 safe, meaning multiple threads can call any of its methods,
 concurrently.  If your application requires external
 synchronization, you should <b>not</b> synchronize on the
 <code>IndexReader</code> instance; use your own
 (non-Lucene) objects instead.
*/
public abstract class AtomicIndexReader extends IndexReader {

  protected AtomicIndexReader() {
    super();
  }

  @Override
  public abstract AtomicReaderContext getTopReaderContext();

  /** Returns true if there are norms stored for this field. */
  public boolean hasNorms(String field) throws IOException {
    // backward compatible implementation.
    // SegmentReader has an efficient implementation.
    ensureOpen();
    return normValues(field) != null;
  }

  /**
   * Returns {@link Fields} for this reader.
   * This method may return null if the reader has no
   * postings.
   *
   * <p><b>NOTE</b>: if this is a multi reader ({@link
   * #getSequentialSubReaders} is not null) then this
   * method will throw UnsupportedOperationException.  If
   * you really need a {@link Fields} for such a reader,
   * use {@link MultiFields#getFields}.  However, for
   * performance reasons, it's best to get all sub-readers
   * using {@link ReaderUtil#gatherSubReaders} and iterate
   * through them yourself. */
  public abstract Fields fields() throws IOException;
  
  @Override
  public int docFreq(String field, BytesRef term) throws IOException {
    final Fields fields = fields();
    if (fields == null) {
      return 0;
    }
    final Terms terms = fields.terms(field);
    if (terms == null) {
      return 0;
    }
    final TermsEnum termsEnum = terms.iterator(null);
    if (termsEnum.seekExact(term, true)) {
      return termsEnum.docFreq();
    } else {
      return 0;
    }
  }

  /** Returns the number of documents containing the term
   * <code>t</code>.  This method returns 0 if the term or
   * field does not exists.  This method does not take into
   * account deleted documents that have not yet been merged
   * away. */
  public final long totalTermFreq(String field, BytesRef term) throws IOException {
    final Fields fields = fields();
    if (fields == null) {
      return 0;
    }
    final Terms terms = fields.terms(field);
    if (terms == null) {
      return 0;
    }
    final TermsEnum termsEnum = terms.iterator(null);
    if (termsEnum.seekExact(term, true)) {
      return termsEnum.totalTermFreq();
    } else {
      return 0;
    }
  }

  /** This may return null if the field does not exist.*/
  public final Terms terms(String field) throws IOException {
    final Fields fields = fields();
    if (fields == null) {
      return null;
    }
    return fields.terms(field);
  }

  /** Returns {@link DocsEnum} for the specified field &
   *  term.  This may return null, if either the field or
   *  term does not exist. */
  public final DocsEnum termDocsEnum(Bits liveDocs, String field, BytesRef term, boolean needsFreqs) throws IOException {
    assert field != null;
    assert term != null;
    final Fields fields = fields();
    if (fields != null) {
      final Terms terms = fields.terms(field);
      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator(null);
        if (termsEnum.seekExact(term, true)) {
          return termsEnum.docs(liveDocs, null, needsFreqs);
        }
      }
    }
    return null;
  }

  /** Returns {@link DocsAndPositionsEnum} for the specified
   *  field & term.  This may return null, if either the
   *  field or term does not exist, or needsOffsets is
   *  true but offsets were not indexed for this field. */
  public final DocsAndPositionsEnum termPositionsEnum(Bits liveDocs, String field, BytesRef term, boolean needsOffsets) throws IOException {
    assert field != null;
    assert term != null;
    final Fields fields = fields();
    if (fields != null) {
      final Terms terms = fields.terms(field);
      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator(null);
        if (termsEnum.seekExact(term, true)) {
          return termsEnum.docsAndPositions(liveDocs, null, needsOffsets);
        }
      }
    }
    return null;
  }
  
  /**
   * Returns {@link DocsEnum} for the specified field and
   * {@link TermState}. This may return null, if either the field or the term
   * does not exists or the {@link TermState} is invalid for the underlying
   * implementation.*/
  public final DocsEnum termDocsEnum(Bits liveDocs, String field, BytesRef term, TermState state, boolean needsFreqs) throws IOException {
    assert state != null;
    assert field != null;
    final Fields fields = fields();
    if (fields != null) {
      final Terms terms = fields.terms(field);
      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator(null);
        termsEnum.seekExact(term, state);
        return termsEnum.docs(liveDocs, null, needsFreqs);
      }
    }
    return null;
  }
  
  /**
   * Returns {@link DocsAndPositionsEnum} for the specified field and
   * {@link TermState}. This may return null, if either the field or the term
   * does not exists, the {@link TermState} is invalid for the underlying
   * implementation, or needsOffsets is true but offsets
   * were not indexed for this field. */
  public final DocsAndPositionsEnum termPositionsEnum(Bits liveDocs, String field, BytesRef term, TermState state, boolean needsOffsets) throws IOException {
    assert state != null;
    assert field != null;
    final Fields fields = fields();
    if (fields != null) {
      final Terms terms = fields.terms(field);
      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator(null);
        termsEnum.seekExact(term, state);
        return termsEnum.docsAndPositions(liveDocs, null, needsOffsets);
      }
    }
    return null;
  }

  /** Returns the number of unique terms (across all fields)
   *  in this reader.
   *
   *  @return number of unique terms or -1 if this count
   *  cannot be easily determined (eg Multi*Readers).
   *  Instead, you should call {@link
   *  #getSequentialSubReaders} and ask each sub reader for
   *  its unique term count. */
  public final long getUniqueTermCount() throws IOException {
    final Fields fields = fields();
    if (fields == null) {
      return 0;
    }
    return fields.getUniqueTermCount();
  }
  
  /**
   * Returns {@link DocValues} for this field.
   * This method may return null if the reader has no per-document
   * values stored.
   *
   * <p><b>NOTE</b>: if this is a multi reader ({@link
   * #getSequentialSubReaders} is not null) then this
   * method will throw UnsupportedOperationException.  If
   * you really need {@link DocValues} for such a reader,
   * use {@link MultiDocValues#getDocValues(IndexReader,String)}.  However, for
   * performance reasons, it's best to get all sub-readers
   * using {@link ReaderUtil#gatherSubReaders} and iterate
   * through them yourself. */
  public abstract DocValues docValues(String field) throws IOException;
  
  public abstract DocValues normValues(String field) throws IOException;

  /**
   * Get the {@link FieldInfos} describing all fields in
   * this reader.  NOTE: do not make any changes to the
   * returned FieldInfos!
   *
   * @lucene.experimental
   */
  public abstract FieldInfos getFieldInfos();
  
  /** Returns the {@link Bits} representing live (not
   *  deleted) docs.  A set bit indicates the doc ID has not
   *  been deleted.  If this method returns null it means
   *  there are no deleted documents (all documents are
   *  live).
   *
   *  The returned instance has been safely published for
   *  use by multiple threads without additional
   *  synchronization.
   */
  public abstract Bits getLiveDocs();
  
  /**
   * {@link ReaderContext} for {@link AtomicIndexReader} instances
   * @lucene.experimental
   */
  public static final class AtomicReaderContext extends ReaderContext {
    /** The readers ord in the top-level's leaves array */
    public final int ord;
    /** The readers absolute doc base */
    public final int docBase;
    
    private final AtomicIndexReader reader;

    /**
     * Creates a new {@link AtomicReaderContext} 
     */    
    public AtomicReaderContext(CompositeReaderContext parent, AtomicIndexReader reader,
        int ord, int docBase, int leafOrd, int leafDocBase) {
      super(parent, ord, docBase);
      this.ord = leafOrd;
      this.docBase = leafDocBase;
      this.reader = reader;
    }
    
    public AtomicReaderContext(AtomicIndexReader atomicReader) {
      this(null, atomicReader, 0, 0, 0, 0);
    }
    
    @Override
    public AtomicReaderContext[] leaves() {
      return null;
    }
    
    @Override
    public ReaderContext[] children() {
      return null;
    }
    
    @Override
    public AtomicIndexReader reader() {
      return reader;
    }
  }
}
