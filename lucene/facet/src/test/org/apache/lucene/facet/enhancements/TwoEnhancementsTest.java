package org.apache.lucene.facet.enhancements;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.enhancements.EnhancementsDocumentBuilder;
import org.apache.lucene.facet.enhancements.EnhancementsPayloadIterator;
import org.apache.lucene.facet.enhancements.params.DefaultEnhancementsIndexingParams;
import org.apache.lucene.facet.enhancements.params.EnhancementsIndexingParams;
import org.apache.lucene.facet.search.DrillDown;
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

public class TwoEnhancementsTest extends LuceneTestCase {

  @Test
  public void testTwoEmptyAndNonEmptyByteArrays() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    EnhancementsIndexingParams indexingParams = 
      new DefaultEnhancementsIndexingParams(
          new CategoryEnhancementDummy1(),
          new CategoryEnhancementDummy3());

    // add document with a category containing data for both enhancements
    List<CategoryPath> categoryPaths = new ArrayList<CategoryPath>();
    categoryPaths.add(new CategoryPath("a", "b"));

    RandomIndexWriter indexWriter = new RandomIndexWriter(random(), indexDir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    TaxonomyWriter taxo = new DirectoryTaxonomyWriter(taxoDir);

    // a category document builder will add the categories to a document
    // once build() is called
    Document doc = new Document();
    indexWriter.addDocument(new EnhancementsDocumentBuilder(taxo,
        indexingParams).setCategoryPaths(categoryPaths).build(doc));

    IndexReader indexReader = indexWriter.getReader();
    indexWriter.close();

    Term term = DrillDown.term(indexingParams, new CategoryPath("a","b"));
    EnhancementsPayloadIterator iterator = new EnhancementsPayloadIterator(
        indexingParams.getCategoryEnhancements(), indexReader, term);

    assertTrue("EnhancementsPayloadIterator failure", iterator.init());
    assertTrue("Missing document 0", iterator.setdoc(0));
    assertNull("Unexpected data for CategoryEnhancementDummy2", iterator
        .getCategoryData(new CategoryEnhancementDummy1()));
    byte[] dummy3 = (byte[]) iterator
        .getCategoryData(new CategoryEnhancementDummy3());
    assertTrue("Bad array returned for CategoryEnhancementDummy3", Arrays
        .equals(dummy3, CategoryEnhancementDummy3.CATEGORY_TOKEN_BYTES));
    indexReader.close();
    indexDir.close();
    taxo.close();
    taxoDir.close();
  }

  @Test
  public void testTwoNonEmptyByteArrays() throws Exception {
    // add document with a category containing data for both enhancements
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    EnhancementsIndexingParams indexingParams = 
      new DefaultEnhancementsIndexingParams(
          new CategoryEnhancementDummy2(),
          new CategoryEnhancementDummy3());

    List<CategoryPath> categoryPaths = new ArrayList<CategoryPath>();
    categoryPaths.add(new CategoryPath("a", "b"));

    RandomIndexWriter indexWriter = new RandomIndexWriter(random(), indexDir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    TaxonomyWriter taxo = new DirectoryTaxonomyWriter(taxoDir);

    // a category document builder will add the categories to a document
    // once build() is called
    Document doc = new Document();
    indexWriter.addDocument(new EnhancementsDocumentBuilder(taxo,
        indexingParams).setCategoryPaths(categoryPaths).build(doc));

    IndexReader indexReader = indexWriter.getReader();
    indexWriter.close();

    Term term = DrillDown.term(indexingParams, new CategoryPath("a","b"));
    EnhancementsPayloadIterator iterator = new EnhancementsPayloadIterator(
        indexingParams.getCategoryEnhancements(), indexReader, term);

    assertTrue("EnhancementsPayloadIterator failure", iterator.init());
    assertTrue("Missing document 0", iterator.setdoc(0));
    byte[] dummy2 = (byte[]) iterator
        .getCategoryData(new CategoryEnhancementDummy2());
    assertTrue("Bad array returned for CategoryEnhancementDummy2", Arrays
        .equals(dummy2, CategoryEnhancementDummy2.CATEGORY_TOKEN_BYTES));
    byte[] dummy3 = (byte[]) iterator
        .getCategoryData(new CategoryEnhancementDummy3());
    assertTrue("Bad array returned for CategoryEnhancementDummy3", Arrays
        .equals(dummy3, CategoryEnhancementDummy3.CATEGORY_TOKEN_BYTES));
    indexReader.close();
    taxo.close();
    indexDir.close();
    taxoDir.close();
  }
}
