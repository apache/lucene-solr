package org.apache.lucene.facet.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
 * Iterates over multiple {@link CategoryListIterator}s, consuming the provided
 * iterators in order.
 * 
 * @lucene.experimental
 */
public class MultiCategoryListIterator implements CategoryListIterator {

  private final CategoryListIterator[] iterators;
  private final List<CategoryListIterator> validIterators;

  /** Receives the iterators to iterate on */
  public MultiCategoryListIterator(CategoryListIterator... iterators) {
    this.iterators = iterators;
    this.validIterators = new ArrayList<CategoryListIterator>();
  }

  @Override
  public boolean setNextReader(AtomicReaderContext context) throws IOException {
    validIterators.clear();
    for (CategoryListIterator cli : iterators) {
      if (cli.setNextReader(context)) {
        validIterators.add(cli);
      }
    }
    return !validIterators.isEmpty();
  }
  
  @Override
  public void getOrdinals(int docID, IntsRef ints) throws IOException {
    IntsRef tmp = new IntsRef(ints.length);
    for (CategoryListIterator cli : validIterators) {
      cli.getOrdinals(docID, tmp);
      if (ints.ints.length < ints.length + tmp.length) {
        ints.grow(ints.length + tmp.length);
      }
      ints.length += tmp.length;
    }
  }

}
