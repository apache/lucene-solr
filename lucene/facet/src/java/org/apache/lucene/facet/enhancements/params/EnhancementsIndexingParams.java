package org.apache.lucene.facet.enhancements.params;

import java.util.List;

import org.apache.lucene.facet.enhancements.CategoryEnhancement;
import org.apache.lucene.facet.enhancements.EnhancementsDocumentBuilder;
import org.apache.lucene.facet.index.attributes.CategoryProperty;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.index.streaming.CategoryParentsStream;

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
 * {@link FacetIndexingParams Facet indexing parameters} for defining
 * {@link CategoryEnhancement category enhancements}. It must contain at least
 * one enhancement, otherwise nothing is "enhanced" about it. When there are
 * more than one, the order matters - see {@link #getCategoryEnhancements()}.
 * 
 * @see EnhancementsDocumentBuilder
 * @lucene.experimental
 */
public interface EnhancementsIndexingParams extends FacetIndexingParams {

  /**
   * Add {@link CategoryEnhancement}s to the indexing parameters
   * @param enhancements enhancements to add
   */
  public void addCategoryEnhancements(CategoryEnhancement... enhancements);

  /**
   * Get a list of the active category enhancements. If no enhancements exist
   * return {@code null}. The order of enhancements in the returned list
   * dictates the order in which the enhancements data appear in the category
   * tokens payload.
   * 
   * @return A list of the active category enhancements, or {@code null} if
   *         there are no enhancements.
   */
  public List<CategoryEnhancement> getCategoryEnhancements();

  /**
   * Get a list of {@link CategoryProperty} classes to be retained when
   * creating {@link CategoryParentsStream}.
   * 
   * @return the list of {@link CategoryProperty} classes to be retained when
   *         creating {@link CategoryParentsStream}, or {@code null} if there
   *         are no such properties.
   */
  public List<Class<? extends CategoryProperty>> getRetainableProperties();

}
