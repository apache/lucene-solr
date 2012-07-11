package org.apache.lucene.facet.enhancements.params;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.enhancements.CategoryEnhancement;
import org.apache.lucene.facet.index.attributes.CategoryProperty;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.PerDimensionIndexingParams;

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
 * Default implementation of {@link EnhancementsIndexingParams} 
 * 
 * @lucene.experimental
 */
public class DefaultEnhancementsIndexingParams extends
    PerDimensionIndexingParams implements EnhancementsIndexingParams {

  private List<CategoryEnhancement> enhancedCategories;

  /**
   * Construct with a certain {@link CategoryEnhancement enhancement}
   * @throws IllegalArgumentException if no enhancements are provided
   */
  public DefaultEnhancementsIndexingParams(CategoryEnhancement... enhancements) {
    super();
    validateparams(enhancements);
    addCategoryEnhancements(enhancements);
  }

  private void validateparams(CategoryEnhancement... enhancements) {
    if (enhancements==null || enhancements.length<1) {
      throw new IllegalArgumentException("at least one enhancement is required");
    }
  }

  /**
   * Construct with certain {@link CategoryEnhancement enhancements}
   * and {@link CategoryListParams}
   * @throws IllegalArgumentException if no enhancements are provided
   */
  public DefaultEnhancementsIndexingParams(
      CategoryListParams categoryListParams,
      CategoryEnhancement... enhancements) {
    super(categoryListParams);
    validateparams(enhancements);
    addCategoryEnhancements(enhancements);
  }

  public void addCategoryEnhancements(CategoryEnhancement... enhancements) {
    if (enhancedCategories == null) {
      enhancedCategories = new ArrayList<CategoryEnhancement>();
    }
    for (CategoryEnhancement categoryEnhancement : enhancements) {
      enhancedCategories.add(categoryEnhancement);
    }
  }

  public List<CategoryEnhancement> getCategoryEnhancements() {
    if (enhancedCategories == null || enhancedCategories.isEmpty()) {
      return null;
    }
    return enhancedCategories;
  }

  public List<Class<? extends CategoryProperty>> getRetainableProperties() {
    if (enhancedCategories == null) {
      return null;
    }
    List<Class<? extends CategoryProperty>> retainableProperties = new ArrayList<Class<? extends CategoryProperty>>();
    for (CategoryEnhancement enhancement : enhancedCategories) {
      if (enhancement.getRetainableProperty() != null) {
        retainableProperties.add(enhancement.getRetainableProperty());
      }
    }
    if (retainableProperties.isEmpty()) {
      return null;
    }
    return retainableProperties;
  }
}
