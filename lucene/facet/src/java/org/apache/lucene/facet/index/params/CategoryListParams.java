package org.apache.lucene.facet.index.params;

import java.io.IOException;
import java.io.Serializable;

import org.apache.lucene.facet.search.CategoryListIterator;
import org.apache.lucene.facet.search.PayloadCategoryListIteraor;
import org.apache.lucene.facet.util.PartitionsUtils;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.encoding.DGapIntEncoder;
import org.apache.lucene.util.encoding.IntDecoder;
import org.apache.lucene.util.encoding.IntEncoder;
import org.apache.lucene.util.encoding.SortingIntEncoder;
import org.apache.lucene.util.encoding.UniqueValuesIntEncoder;
import org.apache.lucene.util.encoding.VInt8IntEncoder;

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
 * Contains parameters for a category list *
 * 
 * @lucene.experimental
 */
public class CategoryListParams implements Serializable {

  /** The default term used to store the facets information. */
  public static final Term DEFAULT_TERM = new Term("$facets", "$fulltree$");

  private final Term term;

  private final int hashCode;

  /**
   * Constructs a default category list parameters object, using
   * {@link #DEFAULT_TERM}.
   */
  public CategoryListParams() {
    this(DEFAULT_TERM);
  }

  /**
   * Constructs a category list parameters object, using the given {@link Term}.
   * @param term who's payload hold the category-list.
   */
  public CategoryListParams(Term term) {
    this.term = term;
    // Pre-compute the hashCode because these objects are immutable.  Saves
    // some time on the comparisons later.
    this.hashCode = term.hashCode();
  }
  
  /** 
   * A {@link Term} who's payload holds the category-list. 
   */
  public final Term getTerm() {
    return term;
  }

  /**
   * Allows to override how categories are encoded and decoded. A matching
   * {@link IntDecoder} is provided by the {@link IntEncoder}.
   * <p>
   * Default implementation creates a new Sorting(<b>Unique</b>(DGap)) encoder.
   * Uniqueness in this regard means when the same category appears twice in a
   * document, only one appearance would be encoded. This has effect on facet
   * counting results.
   * <p>
   * Some possible considerations when overriding may be:
   * <ul>
   * <li>an application "knows" that all categories are unique. So no need to
   * pass through the unique filter.</li>
   * <li>Another application might wish to count multiple occurrences of the
   * same category, or, use a faster encoding which will consume more space.</li>
   * </ul>
   * In any event when changing this value make sure you know what you are
   * doing, and test the results - e.g. counts, if the application is about
   * counting facets.
   */
  public IntEncoder createEncoder() {
    return new SortingIntEncoder(new UniqueValuesIntEncoder(new DGapIntEncoder(new VInt8IntEncoder())));
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof CategoryListParams)) {
      return false;
    }
    CategoryListParams other = (CategoryListParams) o;
    if (this.hashCode != other.hashCode) {
      return false;
    }
    // The above hashcodes might equal each other in the case of a collision,
    // so at this point only directly term equality testing will settle
    // the equality test.
    return this.term.equals(other.term);
  }

  @Override
  public int hashCode() {
    return this.hashCode;
  }

  /** Create the {@link CategoryListIterator} for the specified partition. */
  public CategoryListIterator createCategoryListIterator(int partition) throws IOException {
    String categoryListTermStr = PartitionsUtils.partitionName(this, partition);
    Term payloadTerm = new Term(term.field(), categoryListTermStr);
    return new PayloadCategoryListIteraor(payloadTerm, createEncoder().createMatchingDecoder());
  }
  
}