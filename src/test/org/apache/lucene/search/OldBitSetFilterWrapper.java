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
import java.util.BitSet;

import org.apache.lucene.index.IndexReader;

  /**
   *  Helper class used for testing compatibility with old BitSet-based filters.
   *  Does not override {@link Filter#getDocIdSet(IndexReader)} and thus ensures
   *  that {@link #bits(IndexReader)} is called.
   *  
   *  @deprecated This class will be removed together with the 
   *  {@link Filter#bits(IndexReader)} method in Lucene 3.0.
   */
  public class OldBitSetFilterWrapper extends Filter {
    private Filter filter;
    
    public OldBitSetFilterWrapper(Filter filter) {
      this.filter = filter;
    }
    
    public BitSet bits(IndexReader reader) throws IOException {
      BitSet bits = new BitSet(reader.maxDoc());
      DocIdSetIterator it = filter.getDocIdSet(reader).iterator();
      while(it.next()) {
        bits.set(it.doc());
      }
      return bits;
    }
  }
