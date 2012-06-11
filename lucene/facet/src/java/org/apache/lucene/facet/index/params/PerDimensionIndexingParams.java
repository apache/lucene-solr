package org.apache.lucene.facet.index.params;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
 * A FacetIndexingParams that utilizes different category lists, defined by the
 * dimension specified CategoryPaths (see
 * {@link PerDimensionIndexingParams#addCategoryListParams(CategoryPath, CategoryListParams)}
 * <p>
 * A 'dimension' is defined as the first or "zero-th" component in a
 * CategoryPath. For example, if a CategoryPath is defined as
 * "/Author/American/Mark Twain", then the dimension is "Author".
 * <p>
 * This class also uses the 'default' CategoryListParams (as specified by
 * {@link CategoryListParams#CategoryListParams()} when
 * {@link #getCategoryListParams(CategoryPath)} is called for a CategoryPath
 * whose dimension component has not been specifically defined.
 * 
 * @lucene.experimental
 */
public class PerDimensionIndexingParams extends DefaultFacetIndexingParams {

  // "Root" or "first component" of a Category Path maps to a
  // CategoryListParams
  private final Map<String, CategoryListParams> clParamsMap = new HashMap<String, CategoryListParams>();

  /**
   * Construct with the default {@link CategoryListParams} as the default
   * CategoryListParams for unspecified CategoryPaths.
   */
  public PerDimensionIndexingParams() {
    this(new CategoryListParams());
  }

  /**
   * Construct with the included categoryListParams as the default
   * CategoryListParams for unspecified CategoryPaths.
   * 
   * @param categoryListParams
   *            the default categoryListParams to use
   */
  public PerDimensionIndexingParams(CategoryListParams categoryListParams) {
    super(categoryListParams);
  }

  /**
   * Get all the categoryListParams, including the default.
   */
  @Override
  public Iterable<CategoryListParams> getAllCategoryListParams() {
    ArrayList<CategoryListParams> vals = 
      new ArrayList<CategoryListParams>(clParamsMap.values());
    for (CategoryListParams clp : super.getAllCategoryListParams()) {
      vals.add(clp);
    }
    return vals;
  }

  /**
   * Get the CategoryListParams based on the dimension or "zero-th category"
   * of the specified CategoryPath.
   */
  @Override
  public CategoryListParams getCategoryListParams(CategoryPath category) {
    if (category != null) {
      CategoryListParams clParams = clParamsMap.get(category.getComponent(0));
      if (clParams != null) {
        return clParams;
      }
    }
    return super.getCategoryListParams(category);
  }

  /**
   * Add a CategoryListParams for a given CategoryPath's dimension or
   * "zero-th" category.
   * 
   * @param category
   * @param clParams
   */
  public void addCategoryListParams(CategoryPath category, CategoryListParams clParams) {
    clParamsMap.put(category.getComponent(0), clParams);
  }
}
