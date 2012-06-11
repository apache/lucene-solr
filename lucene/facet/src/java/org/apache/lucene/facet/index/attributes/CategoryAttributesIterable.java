package org.apache.lucene.facet.index.attributes;

import java.util.Iterator;

import org.apache.lucene.facet.index.streaming.CategoryAttributesStream;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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
 * This class transforms an {@link Iterable} of {@link CategoryPath} objects
 * into an {@link Iterable} of {@link CategoryAttribute} objects, which can be
 * used to construct a {@link CategoryAttributesStream}.
 * 
 * @lucene.experimental
 */
public class CategoryAttributesIterable implements Iterable<CategoryAttribute> {

  private Iterable<CategoryPath> inputIterable;

  public CategoryAttributesIterable(Iterable<CategoryPath> inputIterable) {
    this.inputIterable = inputIterable;
  }

  public Iterator<CategoryAttribute> iterator() {
    return new CategoryAttributesIterator(this.inputIterable);
  }

  private static class CategoryAttributesIterator implements Iterator<CategoryAttribute> {

    private Iterator<CategoryPath> internalIterator;
    private CategoryAttributeImpl categoryAttributeImpl;

    public CategoryAttributesIterator(Iterable<CategoryPath> inputIterable) {
      this.internalIterator = inputIterable.iterator();
      this.categoryAttributeImpl = new CategoryAttributeImpl();
    }

    public boolean hasNext() {
      return this.internalIterator.hasNext();
    }

    public CategoryAttribute next() {
      this.categoryAttributeImpl.setCategoryPath(this.internalIterator
          .next());
      return this.categoryAttributeImpl;
    }

    public void remove() {
      this.internalIterator.remove();
    }

  }
}
