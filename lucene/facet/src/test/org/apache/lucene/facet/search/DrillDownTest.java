package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.index.CategoryDocumentBuilder;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.PerDimensionIndexingParams;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
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

public class DrillDownTest extends LuceneTestCase {
  
  private FacetSearchParams defaultParams = new FacetSearchParams();
  private FacetSearchParams nonDefaultParams;
  private static IndexReader reader;
  private static DirectoryTaxonomyReader taxo;
  private static Directory dir;
  private static Directory taxoDir;
  
  public DrillDownTest() throws IOException {
    PerDimensionIndexingParams iParams = new PerDimensionIndexingParams();
    CategoryListParams aClParams = new CategoryListParams(new Term("testing_facets_a", "a"));
    CategoryListParams bClParams = new CategoryListParams(new Term("testing_facets_b", "b"));
    
    iParams.addCategoryListParams(new CategoryPath("a"), aClParams);
    iParams.addCategoryListParams(new CategoryPath("b"), bClParams);
    
    nonDefaultParams = new FacetSearchParams(iParams);
  }

  @BeforeClass
  public static void createIndexes() throws CorruptIndexException, LockObtainFailedException, IOException {
    dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.KEYWORD, false)));
    
    taxoDir = newDirectory();
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    
    for (int i = 0; i < 100; i++) {
      ArrayList<CategoryPath> paths = new ArrayList<CategoryPath>();
      Document doc = new Document();
      if (i % 2 == 0) { // 50
        doc.add(new TextField("content", "foo", Field.Store.NO));
      }
      if (i % 3 == 0) { // 33
        doc.add(new TextField("content", "bar", Field.Store.NO));
      }
      if (i % 4 == 0) { // 25
        paths.add(new CategoryPath("a"));
      }
      if (i % 5 == 0) { // 20
        paths.add(new CategoryPath("b"));
      }
      CategoryDocumentBuilder builder = new CategoryDocumentBuilder(taxoWriter);
      builder.setCategoryPaths(paths).build(doc);
      writer.addDocument(doc);
    }
    
    taxoWriter.close();
    reader = writer.getReader();
    writer.close();
    
    taxo = new DirectoryTaxonomyReader(taxoDir);
  }
  
  @Test
  public void testTermNonDefault() {
    Term termA = DrillDown.term(nonDefaultParams, new CategoryPath("a"));
    assertEquals(new Term("testing_facets_a", "a"), termA);
    
    Term termB = DrillDown.term(nonDefaultParams, new CategoryPath("b"));
    assertEquals(new Term("testing_facets_b", "b"), termB);
  }
  
  @Test
  public void testTermDefault() {
    String defaultField = CategoryListParams.DEFAULT_TERM.field();
    
    Term termA = DrillDown.term(defaultParams, new CategoryPath("a"));
    assertEquals(new Term(defaultField, "a"), termA);
    
    Term termB = DrillDown.term(defaultParams, new CategoryPath("b"));
    assertEquals(new Term(defaultField, "b"), termB);
  }
  
  @Test
  public void testQuery() throws IOException {
    IndexSearcher searcher = newSearcher(reader);

    // Making sure the query yields 25 documents with the facet "a"
    Query q = DrillDown.query(defaultParams, new CategoryPath("a"));
    TopDocs docs = searcher.search(q, 100);
    assertEquals(25, docs.totalHits);
    
    // Making sure the query yields 5 documents with the facet "b" and the
    // previous (facet "a") query as a base query
    Query q2 = DrillDown.query(defaultParams, q, new CategoryPath("b"));
    docs = searcher.search(q2, 100);
    assertEquals(5, docs.totalHits);

    // Making sure that a query of both facet "a" and facet "b" yields 5 results
    Query q3 = DrillDown.query(defaultParams, new CategoryPath("a"), new CategoryPath("b"));
    docs = searcher.search(q3, 100);
    assertEquals(5, docs.totalHits);
    
    // Check that content:foo (which yields 50% results) and facet/b (which yields 20%)
    // would gather together 10 results (10%..) 
    Query fooQuery = new TermQuery(new Term("content", "foo"));
    Query q4 = DrillDown.query(defaultParams, fooQuery, new CategoryPath("b"));
    docs = searcher.search(q4, 100);
    assertEquals(10, docs.totalHits);
  }
  
  @Test
  public void testQueryImplicitDefaultParams() throws IOException {
    IndexSearcher searcher = newSearcher(reader);

    // Create the base query to start with
    Query q = DrillDown.query(defaultParams, new CategoryPath("a"));
    
    // Making sure the query yields 5 documents with the facet "b" and the
    // previous (facet "a") query as a base query
    Query q2 = DrillDown.query(q, new CategoryPath("b"));
    TopDocs docs = searcher.search(q2, 100);
    assertEquals(5, docs.totalHits);

    // Check that content:foo (which yields 50% results) and facet/b (which yields 20%)
    // would gather together 10 results (10%..) 
    Query fooQuery = new TermQuery(new Term("content", "foo"));
    Query q4 = DrillDown.query(fooQuery, new CategoryPath("b"));
    docs = searcher.search(q4, 100);
    assertEquals(10, docs.totalHits);
  }
  
  @AfterClass
  public static void closeIndexes() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    
    if (taxo != null) {
      taxo.close();
      taxo = null;
    }
    
    dir.close();
    taxoDir.close();
  }
    
}
