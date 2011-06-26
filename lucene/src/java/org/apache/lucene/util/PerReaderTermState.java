package org.apache.lucene.util;

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
import java.util.Arrays;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;

/**
 * Maintains a {@link IndexReader} {@link TermState} view over
 * {@link IndexReader} instances containing a single term. The
 * {@link PerReaderTermState} doesn't track if the given {@link TermState}
 * objects are valid, neither if the {@link TermState} instances refer to the
 * same terms in the associated readers.
 * 
 * @lucene.experimental
 */
public final class PerReaderTermState {
  public final ReaderContext topReaderContext; // for asserting!
  private final TermState[] states;
  private int docFreq;

  /**
   * Creates an empty {@link PerReaderTermState} from a {@link ReaderContext}
   */
  public PerReaderTermState(ReaderContext context) {
    assert context != null && context.isTopLevel;
    topReaderContext = context;
    docFreq = 0;
    final int len;
    if (context.leaves() == null) {
      len = 1;
    } else {
      len = context.leaves().length;
    }
    states = new TermState[len];
  }
  
  /**
   * Creates a {@link PerReaderTermState} with an initial {@link TermState},
   * {@link IndexReader} pair.
   */
  public PerReaderTermState(ReaderContext context, TermState state, int ord, int docFreq) {
    this(context);
    register(state, ord, docFreq);
  }

  /**
   * Creates a {@link PerReaderTermState} from a top-level {@link ReaderContext} and the
   * given {@link Term}. This method will lookup the given term in all context's leaf readers 
   * and register each of the readers containing the term in the returned {@link PerReaderTermState}
   * using the leaf reader's ordinal.
   * <p>
   * Note: the given context must be a top-level context.
   */
  public static PerReaderTermState build(ReaderContext context, Term term, boolean cache)
      throws IOException {
    assert context != null && context.isTopLevel;
    final String field = term.field();
    final BytesRef bytes = term.bytes();
    final PerReaderTermState perReaderTermState = new PerReaderTermState(context);
    final AtomicReaderContext[] leaves = ReaderUtil.leaves(context);
    for (int i = 0; i < leaves.length; i++) {
      final Fields fields = leaves[i].reader.fields();
      if (fields != null) {
        final Terms terms = fields.terms(field);
        if (terms != null) {
          final TermsEnum termsEnum = terms.getThreadTermsEnum(); // thread-private don't share!
          if (termsEnum.seekExact(bytes, cache)) { 
            final TermState termState = termsEnum.termState();
            perReaderTermState.register(termState, leaves[i].ord, termsEnum.docFreq());
          }
        }
      }
    }
    return perReaderTermState;
  }

  /**
   * Clears the {@link PerReaderTermState} internal state and removes all
   * registered {@link TermState}s
   */
  public void clear() {
    docFreq = 0;
    Arrays.fill(states, null);
  }

  /**
   * Registers and associates a {@link TermState} with an leaf ordinal. The leaf ordinal
   * should be derived from a {@link ReaderContext}'s leaf ord.
   */
  public void register(TermState state, final int ord, final int docFreq) {
    assert state != null : "state must not be null";
    assert ord >= 0 && ord < states.length;
    assert states[ord] == null : "state for ord: " + ord
        + " already registered";
    this.docFreq += docFreq;
    states[ord] = state;
  }

  /**
   * Returns the {@link TermState} for an leaf ordinal or <code>null</code> if no
   * {@link TermState} for the ordinal was registered.
   * 
   * @param ord
   *          the readers leaf ordinal to get the {@link TermState} for.
   * @return the {@link TermState} for the given readers ord or <code>null</code> if no
   *         {@link TermState} for the reader was registered
   */
  public TermState get(int ord) {
    assert ord >= 0 && ord < states.length;
    return states[ord];
  }

  /**
   *  Returns the accumulated document frequency of all {@link TermState}
   *         instances passed to {@link #register(TermState, int, int)}.
   * @return the accumulated document frequency of all {@link TermState}
   *         instances passed to {@link #register(TermState, int, int)}.
   */
  public int docFreq() {
    return docFreq;
  }
}