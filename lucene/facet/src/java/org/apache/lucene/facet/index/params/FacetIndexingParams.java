package org.apache.lucene.facet.index.params;

import java.io.Serializable;

import org.apache.lucene.facet.index.categorypolicy.OrdinalPolicy;
import org.apache.lucene.facet.index.categorypolicy.PathPolicy;
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
 * Parameters on how facets are to be written to the index. 
 * For example, which fields and terms are used to refer to the indexed posting list.
 * <P>
 * If non-default parameters were used during indexing, the same parameters
 * must also be passed during faceted search. This requirement is analogous
 * to the requirement during search to know which fields were indexed, and which
 * Analyzer was used on the text.
 * 
 * @lucene.experimental
 */
public interface FacetIndexingParams extends Serializable {

  /**
   * The name of the category-list to put this category in, or null if this
   * category should not be aggregatable.
   * <P>
   * By default, all categories are written to the same category list, but
   * applications which know in advance that in some situations only parts
   * of the category hierarchy needs to be counted can divide the categories
   * into two or more different category lists.
   * <P>
   * If null is returned for a category, it means that this category should
   * not appear in any category list, and thus counts for it cannot be
   * aggregated. This category can still be used for drill-down, even though
   * the count for it is not known.
   */
  public CategoryListParams getCategoryListParams(CategoryPath category);

  /**
   * Return info about all category lists in the index.
   * 
   * @see #getCategoryListParams(CategoryPath)
   */
  public Iterable<CategoryListParams> getAllCategoryListParams();

  // TODO (Facet): Add special cases of exact/non-exact category term-text

  /**
   * Return the drilldown Term-Text which does not need to do any allocations.
   * The number of chars set is returned.
   * <p>
   * Note: Make sure <code>buffer</code> is large enough.
   * @see CategoryPath#charsNeededForFullPath()
   */
  public int drillDownTermText(CategoryPath path, char[] buffer);

  /**
   * Get the partition size.
   * Same value should be used during the life time of an index.
   * At search time this value is compared with actual taxonomy size and their minimum is used.
   */
  public int getPartitionSize();

  /** 
   * Get the policy for indexing category <b>paths</b>, 
   * used for deciding how "high" to climb in taxonomy 
   * from a category when ingesting its category paths. 
   */
  public PathPolicy getPathPolicy();

  /** 
   * Get the policy for indexing category <b>ordinals</b>, 
   * used for deciding how "high" to climb in taxonomy 
   * from a category when ingesting its ordinals 
   */
  public OrdinalPolicy getOrdinalPolicy();
  
  /** 
   * Get the delimiter character used internally for drill-down terms 
   */ 
  public char getFacetDelimChar();
}
