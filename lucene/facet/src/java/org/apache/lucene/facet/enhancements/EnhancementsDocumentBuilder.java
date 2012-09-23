package org.apache.lucene.facet.enhancements;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;

import org.apache.lucene.facet.enhancements.params.EnhancementsIndexingParams;
import org.apache.lucene.facet.index.CategoryDocumentBuilder;
import org.apache.lucene.facet.index.attributes.CategoryProperty;
import org.apache.lucene.facet.index.streaming.CategoryAttributesStream;
import org.apache.lucene.facet.index.streaming.CategoryListTokenizer;
import org.apache.lucene.facet.index.streaming.CategoryParentsStream;
import org.apache.lucene.facet.index.streaming.CategoryTokenizer;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;

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
 * An {@link EnhancementsDocumentBuilder} is a {@link CategoryDocumentBuilder}
 * which adds categories to documents according to the list of
 * {@link CategoryEnhancement}s from {@link EnhancementsIndexingParams}. The
 * additions over {@link CategoryDocumentBuilder} could be in both category
 * tokens, and additional category lists.
 * 
 * @lucene.experimental
 */
public class EnhancementsDocumentBuilder extends CategoryDocumentBuilder {

  /**
   * @param params
   *            Indexing params which include {@link CategoryEnhancement}s.
   */
  public EnhancementsDocumentBuilder(TaxonomyWriter taxonomyWriter,
      EnhancementsIndexingParams params) {
    super(taxonomyWriter, params);
  }

  @Override
  protected TokenStream getParentsStream(CategoryAttributesStream categoryAttributesStream) {
    List<Class<? extends CategoryProperty>> toRetainList = ((EnhancementsIndexingParams) indexingParams)
        .getRetainableProperties();
    if (toRetainList != null) {
      CategoryParentsStream categoryParentsStream = new CategoryParentsStream(
          categoryAttributesStream, taxonomyWriter, indexingParams);
      for (Class<? extends CategoryProperty> toRetain : toRetainList) {
        categoryParentsStream.addRetainableProperty(toRetain);
      }
      return categoryParentsStream;
    }
    return super.getParentsStream(categoryAttributesStream);
  }

  @Override
  protected CategoryListTokenizer getCategoryListTokenizer(TokenStream categoryStream) {
    CategoryListTokenizer tokenizer = super.getCategoryListTokenizer(categoryStream);
    // Add tokenizer for each enhancement that produces category list
    for (CategoryEnhancement enhancement : ((EnhancementsIndexingParams) indexingParams)
        .getCategoryEnhancements()) {
      if (enhancement.generatesCategoryList()) {
        tokenizer = enhancement.getCategoryListTokenizer(tokenizer,
            (EnhancementsIndexingParams) indexingParams,
            taxonomyWriter);
      }
    }
    return tokenizer;
  }

  @Override
  protected CategoryTokenizer getCategoryTokenizer(TokenStream categoryStream)
      throws IOException {
    return new EnhancementsCategoryTokenizer(categoryStream,
        (EnhancementsIndexingParams) indexingParams);
  }

}
