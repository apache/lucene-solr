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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;

/**
 * Base class for DocIdSet to be used with FieldCache. The implementation
 * of its iterator is very stupid and slow if the implementation of the
 * {@link #matchDoc} method is not optimized, as iterators simply increment
 * the document id until {@code matchDoc(int)} returns true. Because of this
 * {@code matchDoc(int)} must be as fast as possible and in no case do any
 * I/O.
 * @lucene.internal
 */
public abstract class FieldCacheDocIdSet extends DocIdSet {

  protected final IndexReader reader;
  
  public FieldCacheDocIdSet(IndexReader reader) {
    this.reader = reader;
  }

  /**
   * this method checks, if a doc is a hit
   */
  protected abstract boolean matchDoc(int doc);

  /** this DocIdSet is cacheable, if it works solely with FieldCache and no TermDocs */
  @Override
  public final boolean isCacheable() {
    return !reader.hasDeletions();
  }

  @Override
  public final DocIdSetIterator iterator() throws IOException {
    if (!reader.hasDeletions()) {
      // Specialization optimization disregard deleted docs
      final int maxDoc = reader.maxDoc();
      return new DocIdSetIterator() {
        private int doc = -1;
        
        @Override
        public int docID() {
          return doc;
        }
      
        @Override
        public int nextDoc() {
          do {
            doc++;
            if (doc >= maxDoc) {
              return doc = NO_MORE_DOCS;
            }
          } while (!matchDoc(doc));
          return doc;
        }
      
        @Override
        public int advance(int target) {
          for(doc=target; doc<maxDoc; doc++) {
            if (matchDoc(doc)) {
              return doc;
            }
          }
          return doc = NO_MORE_DOCS;
        }
      };
    } else {
      // Stupid consultation of acceptDocs and matchDoc()
      final TermDocs termDocs = reader.termDocs(null);
      return new DocIdSetIterator() {
        private int doc = -1;
        
        @Override
        public int docID() {
          return doc;
        }
      
        @Override
        public int nextDoc() throws IOException {
          do {
            if (!termDocs.next())
              return doc = NO_MORE_DOCS;
          } while (!matchDoc(doc = termDocs.doc()));
          return doc;
        }
      
        @Override
        public int advance(int target) throws IOException {
          if (!termDocs.skipTo(target))
            return doc = NO_MORE_DOCS;
          while (!matchDoc(doc = termDocs.doc())) { 
            if (!termDocs.next())
              return doc = NO_MORE_DOCS;
          }
          return doc;
        }
      };
    }
  }
}
