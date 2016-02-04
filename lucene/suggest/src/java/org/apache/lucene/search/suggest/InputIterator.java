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
package org.apache.lucene.search.suggest;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.search.suggest.Lookup.LookupResult; // javadocs
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester; // javadocs
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester; // javadocs
import org.apache.lucene.search.suggest.analyzing.FuzzySuggester; // javadocs
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/**
 * Interface for enumerating term,weight,payload triples for suggester consumption;
 * currently only {@link AnalyzingSuggester}, {@link
 * FuzzySuggester} and {@link AnalyzingInfixSuggester} support payloads.
 */
public interface InputIterator extends BytesRefIterator {

  /** A term's weight, higher numbers mean better suggestions. */
  public long weight();
  
  /** An arbitrary byte[] to record per suggestion.  See
   *  {@link LookupResult#payload} to retrieve the payload
   *  for each suggestion. */
  public BytesRef payload();

  /** Returns true if the iterator has payloads */
  public boolean hasPayloads();
  
  /** 
   * A term's contexts context can be used to filter suggestions.
   * May return null, if suggest entries do not have any context
   * */
  public Set<BytesRef> contexts();
  
  /** Returns true if the iterator has contexts */
  public boolean hasContexts();
  
  /** Singleton InputIterator that iterates over 0 BytesRefs. */
  public static final InputIterator EMPTY = new InputIteratorWrapper(BytesRefIterator.EMPTY);
  
  /**
   * Wraps a BytesRefIterator as a suggester InputIterator, with all weights
   * set to <code>1</code> and carries no payload
   */
  public static class InputIteratorWrapper implements InputIterator {
    private final BytesRefIterator wrapped;
    
    /** 
     * Creates a new wrapper, wrapping the specified iterator and 
     * specifying a weight value of <code>1</code> for all terms 
     * and nullifies associated payloads.
     */
    public InputIteratorWrapper(BytesRefIterator wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public long weight() {
      return 1;
    }

    @Override
    public BytesRef next() throws IOException {
      return wrapped.next();
    }

    @Override
    public BytesRef payload() {
      return null;
    }

    @Override
    public boolean hasPayloads() {
      return false;
    }

    @Override
    public Set<BytesRef> contexts() {
      return null;
    }

    @Override
    public boolean hasContexts() {
      return false;
    }
  }
}
