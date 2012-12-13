package org.apache.lucene.facet.enhancements.params;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.facet.enhancements.CategoryEnhancement;
import org.apache.lucene.facet.enhancements.EnhancementsDocumentBuilder;
import org.apache.lucene.facet.index.attributes.CategoryProperty;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.PerDimensionIndexingParams;
import org.apache.lucene.facet.index.streaming.CategoryParentsStream;
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
 * A {@link PerDimensionIndexingParams} for defining {@link CategoryEnhancement
 * category enhancements}. Must contain at least one enhancement, and when there
 * are more than one, their order matters.
 * 
 * @see #getCategoryEnhancements()
 * @see EnhancementsDocumentBuilder

 * @lucene.experimental
 */
public class EnhancementsIndexingParams extends PerDimensionIndexingParams {

  private final List<CategoryEnhancement> enhancements;

  /**
   * Initializes with the given enhancements
   * 
   * @throws IllegalArgumentException
   *           if no enhancements are provided
   */
  public EnhancementsIndexingParams(CategoryEnhancement... enhancements) {
    this(DEFAULT_CATEGORY_LIST_PARAMS, Collections.<CategoryPath,CategoryListParams> emptyMap(), enhancements);
  }

  /**
   * Initializes with the given enhancements and category list params mappings.
   * 
   * @see PerDimensionIndexingParams#PerDimensionIndexingParams(Map, CategoryListParams)
   * @throws IllegalArgumentException
   *           if no enhancements are provided
   */
  public EnhancementsIndexingParams(CategoryListParams categoryListParams, 
      Map<CategoryPath,CategoryListParams> paramsMap, CategoryEnhancement... enhancements) {
    super(paramsMap, categoryListParams);
    validateparams(enhancements);
    this.enhancements = Arrays.asList(enhancements);
  }

  private void validateparams(CategoryEnhancement... enhancements) {
    if (enhancements == null || enhancements.length < 1) {
      throw new IllegalArgumentException("at least one enhancement is required");
    }
  }

  /**
   * Returns the list of {@link CategoryEnhancement} as were given at
   * intialization time. You are not expected to modify the list. The order of
   * the enhancements dictates the order in which they are written in the
   * document.
   */
  public List<CategoryEnhancement> getCategoryEnhancements() {
    return enhancements;
  }

  /**
   * Returns a list of {@link CategoryProperty} which should be retained when
   * creating {@link CategoryParentsStream}, or {@code null} if there are no
   * such properties.
   */
  public List<CategoryProperty> getRetainableProperties() {
    List<CategoryProperty> props = new ArrayList<CategoryProperty>();
    for (CategoryEnhancement enhancement : enhancements) {
      CategoryProperty prop = enhancement.getRetainableProperty();
      if (prop != null) {
        props.add(prop);
      }
    }
    if (props.isEmpty()) {
      return null;
    }
    return props;
  }

}
