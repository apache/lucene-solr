package org.apache.lucene.facet.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.search.CategoryListIterator;

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
  private final List<CategoryListIterator> perDocValidIterators;

  /** Receives the iterators to iterate on */
  public MultiCategoryListIterator(CategoryListIterator... iterators) {
    this.iterators = iterators;
    this.validIterators = new ArrayList<CategoryListIterator>();
    this.perDocValidIterators = new ArrayList<CategoryListIterator>();
  }

  /** Fails if all given iterators fail to init */
  public boolean init() throws IOException {
    for (CategoryListIterator cli : iterators) {
      if (cli.init()) {
        validIterators.add(cli);
      }
    }
    return !validIterators.isEmpty();
  }

  /**
   * Return a value larger than {@link Integer#MAX_VALUE} only if all
   * iterators are exhausted
   */
  public long nextCategory() throws IOException {
    while (!perDocValidIterators.isEmpty()) {
      long value = perDocValidIterators.get(0).nextCategory();
      if (value <= Integer.MAX_VALUE) {
        return value;
      }
      perDocValidIterators.remove(0);
    }
    return 0x100000000L;
  }

  /**
   * Fails only if skipTo on all the provided iterators returned {@code false}
   */
  public boolean skipTo(int docId) throws IOException {
    perDocValidIterators.clear();
    for (CategoryListIterator cli : validIterators) {
      if (cli.skipTo(docId)) {
        perDocValidIterators.add(cli);
      }
    }
    return !perDocValidIterators.isEmpty();
  }

}
