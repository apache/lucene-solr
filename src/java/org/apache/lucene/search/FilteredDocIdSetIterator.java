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
 * Abstract decorator class of a DocIdSetIterator
 * implementation that provides on-demand filter/validation
 * mechanism on an underlying DocIdSetIterator.  See {@link
 * FilteredDocIdSet}.
 */
public abstract class FilteredDocIdSetIterator extends DocIdSetIterator {
  protected DocIdSetIterator _innerIter;
  private int doc;
	
  /**
   * Constructor.
   * @param innerIter Underlying DocIdSetIterator.
   */
  public FilteredDocIdSetIterator(DocIdSetIterator innerIter) {
    if (innerIter == null) {
      throw new IllegalArgumentException("null iterator");
    }
    _innerIter = innerIter;
    doc = -1;
  }
	
  /**
   * Validation method to determine whether a docid should be in the result set.
   * @param doc docid to be tested
   * @return true if input docid should be in the result set, false otherwise.
   * @see #FilteredDocIdSetIterator(DocIdSetIterator).
   */
  abstract protected boolean match(int doc) throws IOException;
	
  /** @deprecated use {@link #docID()} instead. */
  public final int doc() {
    return doc;
  }

  public int docID() {
    return doc;
  }
  
  /** @deprecated use {@link #nextDoc()} instead. */
  public final boolean next() throws IOException{
    return nextDoc() != NO_MORE_DOCS;
  }

  public int nextDoc() throws IOException {
    while ((doc = _innerIter.nextDoc()) != NO_MORE_DOCS) {
      if (match(doc)) {
        return doc;
      }
    }
    return doc;
  }
  
  /** @deprecated use {@link #advance(int)} instead. */
  public final boolean skipTo(int n) throws IOException{
    return advance(n) != NO_MORE_DOCS;
  }
  
  public int advance(int target) throws IOException {
    doc = _innerIter.advance(target);
    if (doc != NO_MORE_DOCS) {
      if (match(doc)) {
        return doc;
      } else {
        while ((doc = _innerIter.nextDoc()) != NO_MORE_DOCS) {
          if (match(doc)) {
            return doc;
          }
        }
        return doc;
      }
    }
    return doc;
  }
  
}
