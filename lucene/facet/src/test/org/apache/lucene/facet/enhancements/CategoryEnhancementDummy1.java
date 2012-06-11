package org.apache.lucene.facet.enhancements;

import org.apache.lucene.analysis.TokenStream;

import org.apache.lucene.facet.enhancements.CategoryEnhancement;
import org.apache.lucene.facet.enhancements.params.EnhancementsIndexingParams;
import org.apache.lucene.facet.index.attributes.CategoryAttribute;
import org.apache.lucene.facet.index.attributes.CategoryProperty;
import org.apache.lucene.facet.index.streaming.CategoryListTokenizer;
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

public class CategoryEnhancementDummy1 implements CategoryEnhancement {

  public boolean generatesCategoryList() {
    return false;
  }

  public String getCategoryListTermText() {
    return null;
  }

  public CategoryListTokenizer getCategoryListTokenizer(
      TokenStream tokenizer, EnhancementsIndexingParams indexingParams,
      TaxonomyWriter taxonomyWriter) {
    return null;
  }

  public byte[] getCategoryTokenBytes(CategoryAttribute categoryAttribute) {
    return null;
  }

  public Object extractCategoryTokenData(byte[] buffer, int offset, int length) {
    return null;
  }

  public Class<? extends CategoryProperty> getRetainableProperty() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CategoryEnhancementDummy1) {
      return true;
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return super.hashCode();
  }
  
}
