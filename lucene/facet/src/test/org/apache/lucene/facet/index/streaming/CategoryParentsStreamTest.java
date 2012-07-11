package org.apache.lucene.facet.index.streaming;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import org.apache.lucene.facet.FacetException;
import org.apache.lucene.facet.index.CategoryContainerTestBase;
import org.apache.lucene.facet.index.DummyProperty;
import org.apache.lucene.facet.index.categorypolicy.NonTopLevelOrdinalPolicy;
import org.apache.lucene.facet.index.categorypolicy.NonTopLevelPathPolicy;
import org.apache.lucene.facet.index.categorypolicy.OrdinalPolicy;
import org.apache.lucene.facet.index.categorypolicy.PathPolicy;
import org.apache.lucene.facet.index.params.DefaultFacetIndexingParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;

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

public class CategoryParentsStreamTest extends CategoryContainerTestBase {

  /**
   * Verifies that a {@link CategoryParentsStream} can be constructed from
   * {@link CategoryAttributesStream} and produces the correct number of
   * tokens with default facet indexing params.
   * 
   * @throws IOException
   */
  @Test
  public void testStreamDefaultParams() throws IOException {
    Directory directory = newDirectory();
    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(
        directory);
    CategoryParentsStream stream = new CategoryParentsStream(
        new CategoryAttributesStream(categoryContainer),
        taxonomyWriter, new DefaultFacetIndexingParams());

    // count the number of tokens
    int nTokens;
    for (nTokens = 0; stream.incrementToken(); nTokens++) {
    }
    // should be 6 - all categories and parents
    assertEquals("Wrong number of tokens", 6, nTokens);

    taxonomyWriter.close();
    directory.close();
  }

  /**
   * Verifies that a {@link CategoryParentsStream} can be constructed from
   * {@link CategoryAttributesStream} and produces the correct number of
   * tokens with non top level facet indexing params.
   * 
   * @throws IOException
   */
  @Test
  public void testStreamNonTopLevelParams() throws IOException {
    Directory directory = newDirectory();
    final TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(
        directory);
    FacetIndexingParams indexingParams = new DefaultFacetIndexingParams() {
      @Override
      protected OrdinalPolicy fixedOrdinalPolicy() {
        return new NonTopLevelOrdinalPolicy();
      }
      @Override
      protected PathPolicy fixedPathPolicy() {
        return new NonTopLevelPathPolicy();
      }
    };
    
    CategoryParentsStream stream = new CategoryParentsStream(
        new CategoryAttributesStream(categoryContainer),
        taxonomyWriter, indexingParams);

    // count the number of tokens
    int nTokens;
    for (nTokens = 0; stream.incrementToken(); nTokens++) {
    }
    /*
     * should be 4: 3 non top level ("two", "three" and "six"), and one
     * explicit top level ("four")
     */
    assertEquals("Wrong number of tokens", 4, nTokens);

    taxonomyWriter.close();
    directory.close();
  }

  /**
   * Verifies the correctness when no attributes in parents are retained in
   * {@link CategoryParentsStream}.
   * 
   * @throws IOException
   * @throws FacetException 
   */
  @Test
  public void testNoRetainableAttributes() throws IOException {
    Directory directory = newDirectory();
    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(directory);

    new CategoryParentsStream(new CategoryAttributesStream(categoryContainer),
        taxonomyWriter, new DefaultFacetIndexingParams());

    // add DummyAttribute, but do not retain, only one expected
    categoryContainer.addCategory(initialCatgeories[0], new DummyProperty());

    CategoryParentsStream stream = new CategoryParentsStream(new CategoryAttributesStream(
        categoryContainer), taxonomyWriter,
        new DefaultFacetIndexingParams());

    int nAttributes = 0;
    while (stream.incrementToken()) {
      if (stream.categoryAttribute.getProperty(DummyProperty.class) != null) {
        nAttributes++;
      }
    }
    assertEquals("Wrong number of tokens with attributes", 1, nAttributes);

    taxonomyWriter.close();
    directory.close();
  }

  /**
   * Verifies the correctness when attributes in parents are retained in
   * {@link CategoryParentsStream}.
   * 
   * @throws IOException
   * @throws FacetException 
   */
  @Test
  public void testRetainableAttributes() throws IOException {
    Directory directory = newDirectory();
    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(
        directory);

    FacetIndexingParams indexingParams = new DefaultFacetIndexingParams();
    new CategoryParentsStream(new CategoryAttributesStream(
        categoryContainer), taxonomyWriter, indexingParams);

    // add DummyAttribute and retain it, three expected
    categoryContainer.clear();
    categoryContainer
        .addCategory(initialCatgeories[0], new DummyProperty());
    CategoryParentsStream stream = new CategoryParentsStream(
        new CategoryAttributesStream(categoryContainer),
        taxonomyWriter, new DefaultFacetIndexingParams());
    stream.addRetainableProperty(DummyProperty.class);

    MyCategoryListTokenizer tokenizer = new MyCategoryListTokenizer(stream,
        indexingParams);

    int nAttributes = 0;
    try {
      while (tokenizer.incrementToken()) {
        if (stream.categoryAttribute.getProperty(DummyProperty.class) != null) {
          nAttributes++;
        }
      }
    } catch (IOException e) {
      fail("Properties retained after stream closed");
    }
    assertEquals("Wrong number of tokens with attributes", 3, nAttributes);

    taxonomyWriter.close();
    directory.close();
  }

  private final class MyCategoryListTokenizer extends CategoryListTokenizer {

    public MyCategoryListTokenizer(TokenStream input,
        FacetIndexingParams indexingParams) {
      super(input, indexingParams);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        return true;
      }
      if (categoryAttribute != null) {
        if (categoryAttribute.getCategoryPath() == null) {
          if (categoryAttribute.getProperty(DummyProperty.class) != null) {
            throw new IOException(
                "Properties not cleared properly from parents stream");
          }
        }
      }
      return false;
    }

  }
}