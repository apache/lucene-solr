package org.apache.lucene.facet.search.cache;

import java.io.IOException;
import java.util.HashMap;

import org.apache.lucene.index.IndexReader;

import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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
 * Cache for {@link CategoryListData}, per {@link CategoryListParams}. 
 * 
 * @lucene.experimental
 */
public class CategoryListCache {

  private HashMap<CategoryListParams, CategoryListData> 
    cldMap = new HashMap<CategoryListParams,CategoryListData>();

  /**
   * Fetch the cached {@link CategoryListData} for a given {@link CategoryListParams}.
   */
  public CategoryListData get(CategoryListParams clp) {
    return cldMap.get(clp);
  }
  
  /**
   * Register a pre-computed {@link CategoryListData}.
   */
  public void register(CategoryListParams clp, CategoryListData clData) {
    cldMap.put(clp,clData);
  }
  
  /**
   * Load and register {@link CategoryListData}.
   */
  public void loadAndRegister(CategoryListParams clp, 
      IndexReader reader, TaxonomyReader taxo, FacetIndexingParams iparams) throws IOException {
    CategoryListData clData = new CategoryListData(reader, taxo, iparams, clp);
    register(clp,clData);
  }
}
