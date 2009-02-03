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
  private int _currentDoc;
	
  /**
   * Constructor.
   * @param innerIter Underlying DocIdSetIterator.
   */
  public FilteredDocIdSetIterator(DocIdSetIterator innerIter) {
    if (innerIter == null) {
      throw new IllegalArgumentException("null iterator");
    }
    _innerIter = innerIter;
    _currentDoc = -1;
  }
	
  /**
   * Validation method to determine whether a docid should be in the result set.
   * @param docid docid to be tested
   * @return true if input docid should be in the result set, false otherwise.
   * @see #FilteredDocIdSetIterator(DocIdSetIterator).
   */
  abstract protected boolean match(int doc);
	
  // @Override
  public final int doc() {
    return _currentDoc;
  }

  // @Override
  public final boolean next() throws IOException{
    while (_innerIter.next()) {
      int doc = _innerIter.doc();
      if (match(doc)) {
        _currentDoc = doc;
        return true;
      }
    }
    return false;
  }

  // @Override
  public final boolean skipTo(int n) throws IOException{
    boolean flag = _innerIter.skipTo(n);
    if (flag) {
      int doc = _innerIter.doc();
      if (match(doc)) {
        _currentDoc = doc;
        return true;
      } else {
        while (_innerIter.next()) {
          int docid = _innerIter.doc();
          if (match(docid)) {
            _currentDoc = docid;
            return true;
          }
        }
        return false;
      }
    }
    return flag;
  }
}
