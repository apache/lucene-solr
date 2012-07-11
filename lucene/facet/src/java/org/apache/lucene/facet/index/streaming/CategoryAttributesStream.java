package org.apache.lucene.facet.index.streaming;

import java.util.Iterator;

import org.apache.lucene.analysis.TokenStream;

import org.apache.lucene.facet.index.attributes.CategoryAttribute;

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
 * An attribute stream built from an {@link Iterable} of
 * {@link CategoryAttribute}. This stream should then be passed through several
 * filters (see {@link CategoryParentsStream}, {@link CategoryListTokenizer} and
 * {@link CategoryTokenizer}) until a token stream is produced that can be
 * indexed by Lucene.
 * <P>
 * A CategoryAttributesStream object can be reused for producing more than one
 * stream. To do that, the user should cause the underlying
 * Iterable<CategoryAttribute> object to return a new set of categories, and
 * then call {@link #reset()} to allow this stream to be used again.
 * 
 * @lucene.experimental
 */
public class CategoryAttributesStream extends TokenStream {

  protected CategoryAttribute categoryAttribute;

  private Iterable<CategoryAttribute> iterable;
  private Iterator<CategoryAttribute> iterator;

  /**
   * Constructor
   * 
   * @param iterable
   *            {@link Iterable} of {@link CategoryAttribute}, from which
   *            categories are taken.
   */
  public CategoryAttributesStream(Iterable<CategoryAttribute> iterable) {
    this.iterable = iterable;
    this.iterator = null;
    this.categoryAttribute = this.addAttribute(CategoryAttribute.class);
  }

  @Override
  public final boolean incrementToken() {
    if (iterator == null) {
      if (iterable == null) {
        return false;
      }
      iterator = iterable.iterator();
    }
    if (iterator.hasNext()) {
      categoryAttribute.set(iterator.next());
      return true;
    }
    return false;
  }

  @Override
  public void reset() {
    this.iterator = null;
  }

}
