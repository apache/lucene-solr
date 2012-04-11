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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * Access to the terms in a specific field.  See {@link Fields}.
 * @lucene.experimental
 */

public abstract class Terms {

  /** Returns an iterator that will step through all
   *  terms. This method will not return null.  If you have
   *  a previous TermsEnum, for example from a different
   *  field, you can pass it for possible reuse if the
   *  implementation can do so. */
  public abstract TermsEnum iterator(TermsEnum reuse) throws IOException;

  /** Returns a TermsEnum that iterates over all terms that
   *  are accepted by the provided {@link
   *  CompiledAutomaton}.  If the <code>startTerm</code> is
   *  provided then the returned enum will only accept terms
   *  > <code>startTerm</code>, but you still must call
   *  next() first to get to the first term.  Note that the
   *  provided <code>startTerm</code> must be accepted by
   *  the automaton.
   *
   * <p><b>NOTE</b>: the returned TermsEnum cannot
   * seek</p>. */
  public TermsEnum intersect(CompiledAutomaton compiled, final BytesRef startTerm) throws IOException {
    // TODO: eventually we could support seekCeil/Exact on
    // the returned enum, instead of only being able to seek
    // at the start
    if (compiled.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
      throw new IllegalArgumentException("please use CompiledAutomaton.getTermsEnum instead");
    }
    if (startTerm == null) {
      return new AutomatonTermsEnum(iterator(null), compiled);
    } else {
      return new AutomatonTermsEnum(iterator(null), compiled) {
        @Override
        protected BytesRef nextSeekTerm(BytesRef term) throws IOException {
          if (term == null) {
            term = startTerm;
          }
          return super.nextSeekTerm(term);
        }
      };
    }
  }

  /** Return the BytesRef Comparator used to sort terms
   *  provided by the iterator.  This method may return null
   *  if there are no terms.  This method may be invoked
   *  many times; it's best to cache a single instance &
   *  reuse it. */
  public abstract Comparator<BytesRef> getComparator() throws IOException;

  /** Returns the number of terms for this field, or -1 if this 
   *  measure isn't stored by the codec. Note that, just like 
   *  other term measures, this measure does not take deleted 
   *  documents into account. */
  public abstract long size() throws IOException;
  
  /** Returns the sum of {@link TermsEnum#totalTermFreq} for
   *  all terms in this field, or -1 if this measure isn't
   *  stored by the codec (or if this fields omits term freq
   *  and positions).  Note that, just like other term
   *  measures, this measure does not take deleted documents
   *  into account. */
  public abstract long getSumTotalTermFreq() throws IOException;

  /** Returns the sum of {@link TermsEnum#docFreq()} for
   *  all terms in this field, or -1 if this measure isn't
   *  stored by the codec.  Note that, just like other term
   *  measures, this measure does not take deleted documents
   *  into account. */
  public abstract long getSumDocFreq() throws IOException;

  /** Returns the number of documents that have at least one
   *  term for this field, or -1 if this measure isn't
   *  stored by the codec.  Note that, just like other term
   *  measures, this measure does not take deleted documents
   *  into account. */
  public abstract int getDocCount() throws IOException;

  public final static Terms[] EMPTY_ARRAY = new Terms[0];
}
