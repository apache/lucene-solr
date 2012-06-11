package org.apache.lucene.facet.index.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import org.apache.lucene.facet.index.CategoryContainerTestBase;
import org.apache.lucene.facet.index.attributes.CategoryAttributesIterable;
import org.apache.lucene.facet.index.params.DefaultFacetIndexingParams;
import org.apache.lucene.facet.index.streaming.CategoryAttributesStream;
import org.apache.lucene.facet.index.streaming.CategoryTokenizer;
import org.apache.lucene.facet.taxonomy.CategoryPath;
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

public class CategoryTokenizerTest extends CategoryContainerTestBase {

  /**
   * Verifies that a {@link CategoryTokenizer} adds the correct
   * {@link CharTermAttribute}s to a {@link CategoryAttributesStream}.
   * 
   * @throws IOException
   */
  @Test
  public void testTokensDefaultParams() throws IOException {
    Directory directory = newDirectory();
    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(
        directory);
    DefaultFacetIndexingParams indexingParams = new DefaultFacetIndexingParams();
    CategoryTokenizer tokenizer = new CategoryTokenizer(
        new CategoryAttributesStream(categoryContainer),
        indexingParams);

    // count the number of tokens
    Set<String> categoryTerms = new HashSet<String>();
    for (int i = 0; i < initialCatgeories.length; i++) {
      categoryTerms.add(initialCatgeories[i]
          .toString(indexingParams.getFacetDelimChar()));
    }

    int nTokens;
    for (nTokens = 0; tokenizer.incrementToken(); nTokens++) {
      if (!categoryTerms.remove(tokenizer.termAttribute.toString())) {
        fail("Unexpected term: " + tokenizer.termAttribute.toString());
      }
    }
    assertTrue("all category terms should have been found", categoryTerms
        .isEmpty());

    // should be 6 - all categories and parents
    assertEquals("Wrong number of tokens", 3, nTokens);

    taxonomyWriter.close();
    directory.close();
  }

  /**
   * Verifies that {@link CategoryTokenizer} elongates the buffer in
   * {@link CharTermAttribute} for long categories.
   * 
   * @throws IOException
   */
  @Test
  public void testLongCategoryPath() throws IOException {
    Directory directory = newDirectory();
    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(
        directory);

    List<CategoryPath> longCategory = new ArrayList<CategoryPath>();
    longCategory.add(new CategoryPath("one", "two", "three", "four",
        "five", "six", "seven"));

    DefaultFacetIndexingParams indexingParams = new DefaultFacetIndexingParams();
    CategoryTokenizer tokenizer = new CategoryTokenizer(
        new CategoryAttributesStream(new CategoryAttributesIterable(
            longCategory)), indexingParams);

    // count the number of tokens
    String categoryTerm = longCategory.get(0).toString(
        indexingParams.getFacetDelimChar());

    assertTrue("Missing token", tokenizer.incrementToken());
    if (!categoryTerm.equals(tokenizer.termAttribute.toString())) {
      fail("Unexpected term: " + tokenizer.termAttribute.toString());
    }

    assertFalse("Unexpected token", tokenizer.incrementToken());

    taxonomyWriter.close();
    directory.close();
  }
}
