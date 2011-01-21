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

/** Iterator to seek ({@link #seek}) or step through ({@link
 * #next} terms, obtain frequency information ({@link
 * #docFreq}), and obtain a {@link DocsEnum} or {@link
 * DocsAndPositionsEnum} for the current term ({@link
 * #docs}.
 * 
 * <p>Term enumerations are always ordered by
 * {@link #getComparator}.  Each term in the enumeration is
 * greater than all that precede it.</p>
 *
 * <p>On obtaining a TermsEnum, you must first call
 * {@link #next} or {@link #seek}.
 *
 * @lucene.experimental */
public abstract class TermsEnum {

  private AttributeSource atts = null;

  /** Returns the related attributes. */
  public AttributeSource attributes() {
    if (atts == null) atts = new AttributeSource();
    return atts;
  }
  
  /** Represents returned result from {@link #seek}.
   *  If status is FOUND, then the precise term was found.
   *  If status is NOT_FOUND, then a different term was
   *  found.  If the status is END, the end of the iteration
   *  was hit. */
  public static enum SeekStatus {END, FOUND, NOT_FOUND};

  /** Expert: just like {@link #seek(BytesRef)} but allows
   *  you to control whether the implementation should
   *  attempt to use its term cache (if it uses one). */
  public abstract SeekStatus seek(BytesRef text, boolean useCache) throws IOException;

  /** Seeks to the specified term.  Returns SeekStatus to
   *  indicate whether exact term was found, a different
   *  term was found, or EOF was hit.  The target term may
   *  be before or after the current term. */
  public final SeekStatus seek(BytesRef text) throws IOException {
    return seek(text, true);
  }

  /** Seeks to the specified term by ordinal (position) as
   *  previously returned by {@link #ord}.  The target ord
   *  may be before or after the current ord.  See {@link
   *  #seek(BytesRef)}. */
  public abstract SeekStatus seek(long ord) throws IOException;

  /**
   * Expert: Seeks a specific position by {@link TermState} previously obtained
   * from {@link #termState()}. Callers should maintain the {@link TermState} to
   * use this method. Low-level implementations may position the TermsEnum
   * without re-seeking the term dictionary.
   * <p>
   * Seeking by {@link TermState} should only be used iff the enum the state was
   * obtained from and the enum the state is used for seeking are obtained from
   * the same {@link IndexReader}, otherwise a {@link #seek(BytesRef, TermState)} call can
   * leave the enum in undefined state.
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
  public void seek(BytesRef term, TermState state) throws IOException {
    seek(term);
  }

  /** Increments the enumeration to the next element.
   *  Returns the resulting term, or null if the end was
   *  hit.  The returned BytesRef may be re-used across calls
   *  to next. */
  public abstract BytesRef next() throws IOException;

  /** Returns current term. Do not call this before calling
   *  next() for the first time, after next() returns null
   *  or after seek returns {@link SeekStatus#END}.*/
  public abstract BytesRef term() throws IOException;

  /** Returns ordinal position for current term.  This is an
   *  optional method (the codec may throw {@link
   *  UnsupportedOperationException}).  Do not call this
   *  before calling {@link #next} for the first time or after
   *  {@link #next} returns null or {@link #seek} returns
   *  END; */
  public abstract long ord() throws IOException;

  /** Returns the number of documents containing the current
   *  term.  Do not call this before calling next() for the
   *  first time, after next() returns null or seek returns
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
   *  call this before calling {@link #next} or {@link
   *  #seek} for the first time.  This method will not
   *  return null.
   *  
   * @param skipDocs set bits are documents that should not
   * be returned
   * @param reuse pass a prior DocsEnum for possible reuse */
  public abstract DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException;

  /** Get {@link DocsAndPositionsEnum} for the current term.
   *  Do not call this before calling {@link #next} or
   *  {@link #seek} for the first time.  This method will
   *  only return null if positions were not indexed into
   *  the postings by this codec. */
  public abstract DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException;

  /**
   * Expert: Returns the TermsEnums internal state to position the TermsEnum
   * without re-seeking the term dictionary.
   * <p>
   * NOTE: A seek by {@link TermState} might not capture the
   * {@link AttributeSource}'s state. Callers must maintain the
   * {@link AttributeSource} states separately
   * 
   * @see TermState
   * @see #seek(BytesRef, TermState)
   */
  public TermState termState() throws IOException {
    return new TermState() {
      @Override
      public void copyFrom(TermState other) {
      }
    };
  }
  
  /** Return the {@link BytesRef} Comparator used to sort
   *  terms provided by the iterator.  This may return
   *  null if there are no terms.  Callers may invoke this
   *  method many times, so it's best to cache a single
   *  instance & reuse it. */
  public abstract Comparator<BytesRef> getComparator() throws IOException;

  /** An empty TermsEnum for quickly returning an empty instance e.g.
   * in {@link org.apache.lucene.search.MultiTermQuery}
   * <p><em>Please note:</em> This enum should be unmodifiable,
   * but it is currently possible to add Attributes to it.
   * This should not be a problem, as the enum is always empty and
   * the existence of unused Attributes does not matter.
   */
  public static final TermsEnum EMPTY = new TermsEnum() {    
    @Override
    public SeekStatus seek(BytesRef term, boolean useCache) { return SeekStatus.END; }
    
    @Override
    public SeekStatus seek(long ord) { return SeekStatus.END; }
    
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
    public DocsEnum docs(Bits bits, DocsEnum reuse) {
      throw new IllegalStateException("this method should never be called");
    }
      
    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits bits, DocsAndPositionsEnum reuse) {
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
    public void seek(BytesRef term, TermState state) throws IOException {
      throw new IllegalStateException("this method should never be called");
    }
  };
}
