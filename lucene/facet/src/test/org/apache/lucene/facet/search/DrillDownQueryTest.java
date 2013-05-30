package org.apache.lucene.facet.search;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.PerDimensionIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DrillDownQueryTest extends FacetTestCase {
  
  private static IndexReader reader;
  private static DirectoryTaxonomyReader taxo;
  private static Directory dir;
  private static Directory taxoDir;
  
  private FacetIndexingParams defaultParams;
  private PerDimensionIndexingParams nonDefaultParams;

  @AfterClass
  public static void afterClassDrillDownQueryTest() throws Exception {
    IOUtils.close(reader, taxo, dir, taxoDir);
  }

  @BeforeClass
  public static void beforeClassDrillDownQueryTest() throws Exception {
    dir = newDirectory();
    Random r = random();
    RandomIndexWriter writer = new RandomIndexWriter(r, dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(r, MockTokenizer.KEYWORD, false)));
    
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
        if (r.nextBoolean()) {
          paths.add(new CategoryPath("a/1", '/'));
        } else {
          paths.add(new CategoryPath("a/2", '/'));
        }
      }
      if (i % 5 == 0) { // 20
        paths.add(new CategoryPath("b"));
      }
      FacetFields facetFields = new FacetFields(taxoWriter);
      if (paths.size() > 0) {
        facetFields.addFields(doc, paths);
      }
      writer.addDocument(doc);
    }
    
    taxoWriter.close();
    reader = writer.getReader();
    writer.close();
    
    taxo = new DirectoryTaxonomyReader(taxoDir);
  }
  
  public DrillDownQueryTest() {
    Map<CategoryPath,CategoryListParams> paramsMap = new HashMap<CategoryPath,CategoryListParams>();
    paramsMap.put(new CategoryPath("a"), randomCategoryListParams("testing_facets_a"));
    paramsMap.put(new CategoryPath("b"), randomCategoryListParams("testing_facets_b"));
    nonDefaultParams = new PerDimensionIndexingParams(paramsMap);
    defaultParams = new FacetIndexingParams(randomCategoryListParams(CategoryListParams.DEFAULT_FIELD));
  }
  
  @Test
  public void testDefaultField() {
    String defaultField = CategoryListParams.DEFAULT_FIELD;
    
    Term termA = DrillDownQuery.term(defaultParams, new CategoryPath("a"));
    assertEquals(new Term(defaultField, "a"), termA);
    
    Term termB = DrillDownQuery.term(defaultParams, new CategoryPath("b"));
    assertEquals(new Term(defaultField, "b"), termB);
  }
  
  @Test
  public void testAndOrs() throws Exception {
    IndexSearcher searcher = newSearcher(reader);

    // test (a/1 OR a/2) AND b
    DrillDownQuery q = new DrillDownQuery(defaultParams);
    q.add(new CategoryPath("a/1", '/'), new CategoryPath("a/2", '/'));
    q.add(new CategoryPath("b"));
    TopDocs docs = searcher.search(q, 100);
    assertEquals(5, docs.totalHits);
  }
  
  @Test
  public void testQuery() throws IOException {
    IndexSearcher searcher = newSearcher(reader);

    // Making sure the query yields 25 documents with the facet "a"
    DrillDownQuery q = new DrillDownQuery(defaultParams);
    q.add(new CategoryPath("a"));
    QueryUtils.check(q);
    TopDocs docs = searcher.search(q, 100);
    assertEquals(25, docs.totalHits);
    
    // Making sure the query yields 5 documents with the facet "b" and the
    // previous (facet "a") query as a base query
    DrillDownQuery q2 = new DrillDownQuery(defaultParams, q);
    q2.add(new CategoryPath("b"));
    docs = searcher.search(q2, 100);
    assertEquals(5, docs.totalHits);

    // Making sure that a query of both facet "a" and facet "b" yields 5 results
    DrillDownQuery q3 = new DrillDownQuery(defaultParams);
    q3.add(new CategoryPath("a"));
    q3.add(new CategoryPath("b"));
    docs = searcher.search(q3, 100);
    
    assertEquals(5, docs.totalHits);
    // Check that content:foo (which yields 50% results) and facet/b (which yields 20%)
    // would gather together 10 results (10%..) 
    Query fooQuery = new TermQuery(new Term("content", "foo"));
    DrillDownQuery q4 = new DrillDownQuery(defaultParams, fooQuery);
    q4.add(new CategoryPath("b"));
    docs = searcher.search(q4, 100);
    assertEquals(10, docs.totalHits);
  }
  
  @Test
  public void testQueryImplicitDefaultParams() throws IOException {
    IndexSearcher searcher = newSearcher(reader);

    // Create the base query to start with
    DrillDownQuery q = new DrillDownQuery(defaultParams);
    q.add(new CategoryPath("a"));
    
    // Making sure the query yields 5 documents with the facet "b" and the
    // previous (facet "a") query as a base query
    DrillDownQuery q2 = new DrillDownQuery(defaultParams, q);
    q2.add(new CategoryPath("b"));
    TopDocs docs = searcher.search(q2, 100);
    assertEquals(5, docs.totalHits);

    // Check that content:foo (which yields 50% results) and facet/b (which yields 20%)
    // would gather together 10 results (10%..) 
    Query fooQuery = new TermQuery(new Term("content", "foo"));
    DrillDownQuery q4 = new DrillDownQuery(defaultParams, fooQuery);
    q4.add(new CategoryPath("b"));
    docs = searcher.search(q4, 100);
    assertEquals(10, docs.totalHits);
  }
  
  @Test
  public void testScoring() throws IOException {
    // verify that drill-down queries do not modify scores
    IndexSearcher searcher = newSearcher(reader);

    float[] scores = new float[reader.maxDoc()];
    
    Query q = new TermQuery(new Term("content", "foo"));
    TopDocs docs = searcher.search(q, reader.maxDoc()); // fetch all available docs to this query
    for (ScoreDoc sd : docs.scoreDocs) {
      scores[sd.doc] = sd.score;
    }
    
    // create a drill-down query with category "a", scores should not change
    DrillDownQuery q2 = new DrillDownQuery(defaultParams, q);
    q2.add(new CategoryPath("a"));
    docs = searcher.search(q2, reader.maxDoc()); // fetch all available docs to this query
    for (ScoreDoc sd : docs.scoreDocs) {
      assertEquals("score of doc=" + sd.doc + " modified", scores[sd.doc], sd.score, 0f);
    }
  }
  
  @Test
  public void testScoringNoBaseQuery() throws IOException {
    // verify that drill-down queries (with no base query) returns 0.0 score
    IndexSearcher searcher = newSearcher(reader);
    
    DrillDownQuery q = new DrillDownQuery(defaultParams);
    q.add(new CategoryPath("a"));
    TopDocs docs = searcher.search(q, reader.maxDoc()); // fetch all available docs to this query
    for (ScoreDoc sd : docs.scoreDocs) {
      assertEquals(0f, sd.score, 0f);
    }
  }
  
  @Test
  public void testTermNonDefault() {
    Term termA = DrillDownQuery.term(nonDefaultParams, new CategoryPath("a"));
    assertEquals(new Term("testing_facets_a", "a"), termA);
    
    Term termB = DrillDownQuery.term(nonDefaultParams, new CategoryPath("b"));
    assertEquals(new Term("testing_facets_b", "b"), termB);
  }

  @Test
  public void testClone() throws Exception {
    DrillDownQuery q = new DrillDownQuery(defaultParams, new MatchAllDocsQuery());
    q.add(new CategoryPath("a"));
    
    DrillDownQuery clone = q.clone();
    clone.add(new CategoryPath("b"));
    
    assertFalse("query wasn't cloned: source=" + q + " clone=" + clone, q.toString().equals(clone.toString()));
  }
  
  @Test(expected=IllegalStateException.class)
  public void testNoBaseNorDrillDown() throws Exception {
    DrillDownQuery q = new DrillDownQuery(defaultParams);
    q.rewrite(reader);
  }
  
  public void testNoDrillDown() throws Exception {
    Query base = new MatchAllDocsQuery();
    DrillDownQuery q = new DrillDownQuery(defaultParams, base);
    Query rewrite = q.rewrite(reader).rewrite(reader);
    assertSame(base, rewrite);
  }
  
}
