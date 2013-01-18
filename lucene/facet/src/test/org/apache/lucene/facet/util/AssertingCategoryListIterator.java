package org.apache.lucene.facet.util;

import java.io.IOException;

import org.apache.lucene.facet.search.CategoryListIterator;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.IntsRef;

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

/**
 * A {@link CategoryListIterator} which asserts that
 * {@link #getOrdinals(int, IntsRef)} is not called before
 * {@link #setNextReader(AtomicReaderContext)} and that if
 * {@link #setNextReader(AtomicReaderContext)} returns false,
 * {@link #getOrdinals(int, IntsRef)} isn't called.
 */
public class AssertingCategoryListIterator implements CategoryListIterator {
 
  private final CategoryListIterator delegate;
  private boolean setNextReaderCalled = false;
  private boolean validSegment = false;
  private int maxDoc;
  
  public AssertingCategoryListIterator(CategoryListIterator delegate) {
    this.delegate = delegate;
  }
  
  @Override
  public boolean setNextReader(AtomicReaderContext context) throws IOException {
    setNextReaderCalled = true;
    maxDoc = context.reader().maxDoc();
    return validSegment = delegate.setNextReader(context);
  }
  
  @Override
  public void getOrdinals(int docID, IntsRef ints) throws IOException {
    if (!setNextReaderCalled) {
      throw new RuntimeException("should not call getOrdinals without setNextReader first");
    }
    if (!validSegment) {
      throw new RuntimeException("should not call getOrdinals if setNextReader returned false");
    }
    if (docID >= maxDoc) {
      throw new RuntimeException("docID is larger than current maxDoc; forgot to call setNextReader?");
    }
    delegate.getOrdinals(docID, ints);
  }
  
}
