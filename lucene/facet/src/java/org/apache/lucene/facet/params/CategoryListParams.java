package org.apache.lucene.facet.params;

import java.io.IOException;

import org.apache.lucene.facet.encoding.DGapVInt8IntEncoder;
import org.apache.lucene.facet.encoding.IntDecoder;
import org.apache.lucene.facet.encoding.IntEncoder;
import org.apache.lucene.facet.encoding.SortingIntEncoder;
import org.apache.lucene.facet.encoding.UniqueValuesIntEncoder;
import org.apache.lucene.facet.search.CategoryListIterator;
import org.apache.lucene.facet.search.DocValuesCategoryListIterator;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.util.PartitionsUtils;

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
public class CategoryListParams {

  /**
   * Defines which category ordinals are encoded for every document. This also
   * affects how category ordinals are aggregated, check the different policies
   * for more details.
   */
  public static enum OrdinalPolicy {
    /**
     * Encodes only the ordinals of leaf nodes. That is, for the category A/B/C,
     * the ordinals of A and A/B will not be encoded. This policy is efficient
     * for hierarchical dimensions, as it reduces the number of ordinals that
     * are visited per document. During faceted search, this policy behaves
     * exactly like {@link #ALL_PARENTS}, and the counts of all path components
     * will be computed as well.
     * 
     * <p>
     * <b>NOTE:</b> this {@link OrdinalPolicy} requires a special collector or
     * accumulator, which will fix the parents' counts.
     * 
     * <p>
     * <b>NOTE:</b> since only leaf nodes are encoded for the document, you
     * should use this policy when the same document doesn't share two
     * categories that have a mutual parent, or otherwise the counts will be
     * wrong (the mutual parent will be over-counted). For example, if a
     * document has the categories A/B/C and A/B/D, then with this policy the
     * counts of "A" and "B" will be 2, which is wrong. If you intend to index
     * hierarchical dimensions, with more than one category per document, you
     * should use either {@link #ALL_PARENTS} or {@link #ALL_BUT_DIMENSION}.
     */
    NO_PARENTS,
    
    /**
     * Encodes the ordinals of all path components. That is, the category A/B/C
     * will encode the ordinals of A and A/B as well. If you don't require the
     * dimension's count during search, consider using
     * {@link #ALL_BUT_DIMENSION}.
     */
    ALL_PARENTS,
    
    /**
     * Encodes the ordinals of all path components except the dimension. The
     * dimension of a category is defined to be the first components in
     * {@link CategoryPath#components}. For the category A/B/C, the ordinal of
     * A/B will be encoded as well, however not the ordinal of A.
     * 
     * <p>
     * <b>NOTE:</b> when facets are aggregated, this policy behaves exactly like
     * {@link #ALL_PARENTS}, except that the dimension is never counted. I.e. if
     * you ask to count the facet "A", then while in {@link #ALL_PARENTS} you
     * will get counts for "A" <u>and its children</u>, with this policy you
     * will get counts for <u>only its children</u>. This policy is the default
     * one, and makes sense for using with flat dimensions, whenever your
     * application does not require the dimension's count. Otherwise, use
     * {@link #ALL_PARENTS}.
     */
    ALL_BUT_DIMENSION
  }
  
  /** The default field used to store the facets information. */
  public static final String DEFAULT_FIELD = "$facets";

  /**
   * The default {@link OrdinalPolicy} that's used when encoding a document's
   * category ordinals.
   */
  public static final OrdinalPolicy DEFAULT_ORDINAL_POLICY = OrdinalPolicy.ALL_BUT_DIMENSION;
  
  public final String field;

  private final int hashCode;

  /** Constructs a default category list parameters object, using {@link #DEFAULT_FIELD}. */
  public CategoryListParams() {
    this(DEFAULT_FIELD);
  }

  /** Constructs a category list parameters object, using the given field. */
  public CategoryListParams(String field) {
    this.field = field;
    // Pre-compute the hashCode because these objects are immutable.  Saves
    // some time on the comparisons later.
    this.hashCode = field.hashCode();
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
    return new SortingIntEncoder(new UniqueValuesIntEncoder(new DGapVInt8IntEncoder()));
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
    if (hashCode != other.hashCode) {
      return false;
    }
    return field.equals(other.field);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  /** Create the {@link CategoryListIterator} for the specified partition. */
  public CategoryListIterator createCategoryListIterator(int partition) throws IOException {
    String categoryListTermStr = PartitionsUtils.partitionName(partition);
    String docValuesField = field + categoryListTermStr;
    return new DocValuesCategoryListIterator(docValuesField, createEncoder().createMatchingDecoder());
  }
  
  /**
   * Returns the {@link OrdinalPolicy} to use for the given dimension. This
   * {@link CategoryListParams} always returns {@link #DEFAULT_ORDINAL_POLICY}
   * for all dimensions.
   */
  public OrdinalPolicy getOrdinalPolicy(String dimension) {
    return DEFAULT_ORDINAL_POLICY;
  }
  
  @Override
  public String toString() {
    return "field=" + field + " encoder=" + createEncoder() + " ordinalPolicy=" + getOrdinalPolicy(null);
  }
  
}
