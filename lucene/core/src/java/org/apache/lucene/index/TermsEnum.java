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
import java.util.Comparator;

import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/** Iterator to seek ({@link #seekCeil(BytesRef)}, {@link
 * #seekExact(BytesRef,boolean)}) or step through ({@link
 * #next} terms to obtain frequency information ({@link
 * #docFreq}), {@link DocsEnum} or {@link
 * DocsAndPositionsEnum} for the current term ({@link
 * #docs}.
 * 
 * <p>Term enumerations are always ordered by
 * {@link #getComparator}.  Each term in the enumeration is
 * greater than the one before it.</p>
 *
 * <p>The TermsEnum is unpositioned when you first obtain it
 * and you must first successfully call {@link #next} or one
 * of the <code>seek</code> methods.
 *
 * @lucene.experimental */
public abstract class TermsEnum implements BytesRefIterator {

  private AttributeSource atts = null;

  /** Returns the related attributes. */
  public AttributeSource attributes() {
    if (atts == null) atts = new AttributeSource();
    return atts;
  }
  
  /** Represents returned result from {@link #seekCeil}.
   *  If status is FOUND, then the precise term was found.
   *  If status is NOT_FOUND, then a different term was
   *  found.  If the status is END, the end of the iteration
   *  was hit. */
  public static enum SeekStatus {END, FOUND, NOT_FOUND};

  /** Attemps to seek to the exact term, returning
   *  true if the term is found.  If this returns false, the
   *  enum is unpositioned.  For some codecs, seekExact may
   *  be substantially faster than {@link #seekCeil}. */
  public boolean seekExact(BytesRef text, boolean useCache) throws IOException {
    return seekCeil(text, useCache) == SeekStatus.FOUND;
  }

  /** Expert: just like {@link #seekCeil(BytesRef)} but allows
   *  you to control whether the implementation should
   *  attempt to use its term cache (if it uses one). */
  public abstract SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException;

  /** Seeks to the specified term, if it exists, or to the
   *  next (ceiling) term.  Returns SeekStatus to
   *  indicate whether exact term was found, a different
   *  term was found, or EOF was hit.  The target term may
   *  be before or after the current term.  If this returns
   *  SeekStatus.END, the enum is unpositioned. */
  public final SeekStatus seekCeil(BytesRef text) throws IOException {
    return seekCeil(text, true);
  }

  /** Seeks to the specified term by ordinal (position) as
   *  previously returned by {@link #ord}.  The target ord
   *  may be before or after the current ord, and must be
   *  within bounds. */
  public abstract void seekExact(long ord) throws IOException;

  /**
   * Expert: Seeks a specific position by {@link TermState} previously obtained
   * from {@link #termState()}. Callers should maintain the {@link TermState} to
   * use this method. Low-level implementations may position the TermsEnum
   * without re-seeking the term dictionary.
   * <p>
   * Seeking by {@link TermState} should only be used iff the enum the state was
   * obtained from and the enum the state is used for seeking are obtained from
   * the same {@link IndexReader}.
   * <p>
   * NOTE: Using this method with an incompatible {@link TermState} might leave
   * this {@link TermsEnum} in undefined state. On a segment level
   * {@link TermState} instances are compatible only iff the source and the
   * target {@link TermsEnum} operate on the same field. If operating on segment
   * level, TermState instances must not be used across segments.
   * <p>
   * NOTE: A seek by {@link TermState} might not restore the
   * {@link AttributeSource}'s state. {@link AttributeSource} states must be
   * maintained separately if this method is used.
   * @param term the term the TermState corresponds to
   * @param state the {@link TermState}
   * */
  public void seekExact(BytesRef term, TermState state) throws IOException {
    if (!seekExact(term, true)) {
      throw new IllegalArgumentException("term=" + term + " does not exist");
    }
  }

  /** Returns current term. Do not call this when the enum
   *  is unpositioned. */
  public abstract BytesRef term() throws IOException;

  /** Returns ordinal position for current term.  This is an
   *  optional method (the codec may throw {@link
   *  UnsupportedOperationException}).  Do not call this
   *  when the enum is unpositioned. */
  public abstract long ord() throws IOException;

  /** Returns the number of documents containing the current
   *  term.  Do not call this when the enum is unpositioned.
   *  {@link SeekStatus#END}.*/
  public abstract int docFreq() throws IOException;

  /** Returns the total number of occurrences of this term
   *  across all documents (the sum of the freq() for each
   *  doc that has this term).  This will be -1 if the
   *  codec doesn't support this measure.  Note that, like
   *  other term measures, this measure does not take
   *  deleted documents into account. */
  public abstract long totalTermFreq() throws IOException;

  /** Get {@link DocsEnum} for the current term.  Do not
   *  call this when the enum is unpositioned.  This method
   *  may return null (if needsFreqs is true but freqs were
   *  not indexed for this field).
   *  
   * @param liveDocs unset bits are documents that should not
   * be returned
   * @param reuse pass a prior DocsEnum for possible reuse
   * @param needsFreqs true if the caller intends to call
   * {@link DocsEnum#freq}.  If you pass false you must not
   * call {@link DocsEnum#freq} in the returned DocsEnum. */
  public abstract DocsEnum docs(Bits liveDocs, DocsEnum reuse, boolean needsFreqs) throws IOException;

  /** Get {@link DocsAndPositionsEnum} for the current term.
   *  Do not call this when the enum is unpositioned.
   *  This method will only return null if needsOffsets is
   *  true but offsets were not indexed.
   *  @param liveDocs unset bits are documents that should not
   *  be returned
   *  @param reuse pass a prior DocsAndPositionsEnum for possible reuse
   *  @param needsOffsets true if offsets are required */
  public abstract DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, boolean needsOffsets) throws IOException;

  /**
   * Expert: Returns the TermsEnums internal state to position the TermsEnum
   * without re-seeking the term dictionary.
   * <p>
   * NOTE: A seek by {@link TermState} might not capture the
   * {@link AttributeSource}'s state. Callers must maintain the
   * {@link AttributeSource} states separately
   * 
   * @see TermState
   * @see #seekExact(BytesRef, TermState)
   */
  public TermState termState() throws IOException {
    return new TermState() {
      @Override
      public void copyFrom(TermState other) {
      }
    };
  }

  /** An empty TermsEnum for quickly returning an empty instance e.g.
   * in {@link org.apache.lucene.search.MultiTermQuery}
   * <p><em>Please note:</em> This enum should be unmodifiable,
   * but it is currently possible to add Attributes to it.
   * This should not be a problem, as the enum is always empty and
   * the existence of unused Attributes does not matter.
   */
  public static final TermsEnum EMPTY = new TermsEnum() {    
    @Override
    public SeekStatus seekCeil(BytesRef term, boolean useCache) { return SeekStatus.END; }
    
    @Override
    public void seekExact(long ord) {}
    
    @Override
    public BytesRef term() {
      throw new IllegalStateException("this method should never be called");
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return null;
    }
      
    @Override
    public int docFreq() {
      throw new IllegalStateException("this method should never be called");
    }

    @Override
    public long totalTermFreq() {
      throw new IllegalStateException("this method should never be called");
    }
      
    @Override
    public long ord() {
      throw new IllegalStateException("this method should never be called");
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, boolean needsFreqs) {
      throw new IllegalStateException("this method should never be called");
    }
      
    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, boolean needsOffsets) {
      throw new IllegalStateException("this method should never be called");
    }
      
    @Override
    public BytesRef next() {
      return null;
    }
    
    @Override // make it synchronized here, to prevent double lazy init
    public synchronized AttributeSource attributes() {
      return super.attributes();
    }

    @Override
    public TermState termState() throws IOException {
      throw new IllegalStateException("this method should never be called");
    }

    @Override
    public void seekExact(BytesRef term, TermState state) throws IOException {
      throw new IllegalStateException("this method should never be called");
    }
  };
}
