package org.apache.lucene.search;

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

/**
 * A DocIdSet contains a set of doc ids. Implementing classes must
 * only implement {@link #iterator} to provide access to the set. 
 */
public abstract class DocIdSet {

  /** An empty {@code DocIdSet} instance for easy use, e.g. in Filters that hit no documents. */
  public static final DocIdSet EMPTY_DOCIDSET = new DocIdSet() {
    
    private final DocIdSetIterator iterator = new DocIdSetIterator() {
      @Override
      public int advance(int target) throws IOException { return NO_MORE_DOCS; }
      @Override
      public int docID() { return NO_MORE_DOCS; }
      @Override
      public int nextDoc() throws IOException { return NO_MORE_DOCS; }
    };
    
    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }
    
    @Override
    public boolean isCacheable() {
      return true;
    }
  };
    
  /** Provides a {@link DocIdSetIterator} to access the set.
   * This implementation can return <code>null</code> or
   * <code>{@linkplain #EMPTY_DOCIDSET}.iterator()</code> if there
   * are no docs that match. */
  public abstract DocIdSetIterator iterator() throws IOException;

  /**
   * This method is a hint for {@link CachingWrapperFilter}, if this <code>DocIdSet</code>
   * should be cached without copying it into a BitSet. The default is to return
   * <code>false</code>. If you have an own <code>DocIdSet</code> implementation
   * that does its iteration very effective and fast without doing disk I/O,
   * override this method and return <code>true</here>.
   */
  public boolean isCacheable() {
    return false;
  }
}
