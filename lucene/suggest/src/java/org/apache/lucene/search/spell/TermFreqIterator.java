package org.apache.lucene.search.spell;

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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/**
 * Interface for enumerating term,weight pairs.
 */
public interface TermFreqIterator extends BytesRefIterator {

  /** A term's weight, higher numbers mean better suggestions. */
  public long weight();
  
  /**
   * Wraps a BytesRefIterator as a TermFreqIterator, with all weights
   * set to <code>1</code>
   */
  public static class TermFreqIteratorWrapper implements TermFreqIterator {
    private BytesRefIterator wrapped;
    
    /** 
     * Creates a new wrapper, wrapping the specified iterator and 
     * specifying a weight value of <code>1</code> for all terms.
     */
    public TermFreqIteratorWrapper(BytesRefIterator wrapped) {
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
    public Comparator<BytesRef> getComparator() {
      return wrapped.getComparator();
    }
  }
}
