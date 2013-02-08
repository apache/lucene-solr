package org.apache.lucene.facet.params;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
 * A {@link FacetIndexingParams} that utilizes different category lists, defined
 * by the dimension specified by a {@link CategoryPath category} (see
 * {@link #PerDimensionIndexingParams(Map, CategoryListParams)}.
 * <p>
 * A 'dimension' is defined as the first or "zero-th" component in a
 * {@link CategoryPath}. For example, if a category is defined as
 * "Author/American/Mark Twain", then the dimension would be "Author".
 * 
 * @lucene.experimental
 */
public class PerDimensionIndexingParams extends FacetIndexingParams {

  private final Map<String, CategoryListParams> clParamsMap;

  /**
   * Initializes a new instance with the given dimension-to-params mapping. The
   * dimension is considered as what's returned by
   * {@link CategoryPath#components cp.components[0]}.
   * 
   * <p>
   * <b>NOTE:</b> for any dimension whose {@link CategoryListParams} is not
   * defined in the mapping, a default {@link CategoryListParams} will be used.
   * 
   * @see #PerDimensionIndexingParams(Map, CategoryListParams)
   */
  public PerDimensionIndexingParams(Map<CategoryPath, CategoryListParams> paramsMap) {
    this(paramsMap, DEFAULT_CATEGORY_LIST_PARAMS);
  }

  /**
   * Same as {@link #PerDimensionIndexingParams(Map)}, only the given
   * {@link CategoryListParams} will be used for any dimension that is not
   * specified in the given mapping.
   */
  public PerDimensionIndexingParams(Map<CategoryPath, CategoryListParams> paramsMap, 
      CategoryListParams categoryListParams) {
    super(categoryListParams);
    clParamsMap = new HashMap<String,CategoryListParams>();
    for (Entry<CategoryPath, CategoryListParams> e : paramsMap.entrySet()) {
      clParamsMap.put(e.getKey().components[0], e.getValue());
    }
  }

  @Override
  public List<CategoryListParams> getAllCategoryListParams() {
    ArrayList<CategoryListParams> vals = new ArrayList<CategoryListParams>(clParamsMap.values());
    vals.add(clParams); // add the default too
    return vals;
  }

  /**
   * Returns the {@link CategoryListParams} for the corresponding dimension
   * which is returned by {@code category.getComponent(0)}. If {@code category}
   * is {@code null}, or was not specified in the map given to the constructor,
   * returns the default {@link CategoryListParams}.
   */
  @Override
  public CategoryListParams getCategoryListParams(CategoryPath category) {
    if (category != null) {
      CategoryListParams clParams = clParamsMap.get(category.components[0]);
      if (clParams != null) {
        return clParams;
      }
    }
    return clParams;
  }

}
