package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.collections.ObjectToIntMap;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.params.PerDimensionOrdinalPolicy;
import org.apache.lucene.facet.params.CategoryListParams.OrdinalPolicy;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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

public class CountingFacetsAggregatorTest extends FacetTestCase {
  
  private static final Term A = new Term("f", "a");
  private static final CategoryPath CP_A = new CategoryPath("A"), CP_B = new CategoryPath("B");
  private static final CategoryPath CP_C = new CategoryPath("C"), CP_D = new CategoryPath("D"); // indexed w/ NO_PARENTS
  private static final int NUM_CHILDREN_CP_A = 5, NUM_CHILDREN_CP_B = 3;
  private static final int NUM_CHILDREN_CP_C = 5, NUM_CHILDREN_CP_D = 5;
  private static final CategoryPath[] CATEGORIES_A, CATEGORIES_B;
  private static final CategoryPath[] CATEGORIES_C, CATEGORIES_D;
  static {
    CATEGORIES_A = new CategoryPath[NUM_CHILDREN_CP_A];
    for (int i = 0; i < NUM_CHILDREN_CP_A; i++) {
      CATEGORIES_A[i] = new CategoryPath(CP_A.components[0], Integer.toString(i));
    }
    CATEGORIES_B = new CategoryPath[NUM_CHILDREN_CP_B];
    for (int i = 0; i < NUM_CHILDREN_CP_B; i++) {
      CATEGORIES_B[i] = new CategoryPath(CP_B.components[0], Integer.toString(i));
    }
    
    // NO_PARENTS categories
    CATEGORIES_C = new CategoryPath[NUM_CHILDREN_CP_C];
    for (int i = 0; i < NUM_CHILDREN_CP_C; i++) {
      CATEGORIES_C[i] = new CategoryPath(CP_C.components[0], Integer.toString(i));
    }
    
    // Multi-level categories
    CATEGORIES_D = new CategoryPath[NUM_CHILDREN_CP_D];
    for (int i = 0; i < NUM_CHILDREN_CP_D; i++) {
      String val = Integer.toString(i);
      CATEGORIES_D[i] = new CategoryPath(CP_D.components[0], val, val + val); // e.g. D/1/11, D/2/22...
    }
  }
  
  private static Directory indexDir, taxoDir;
  private static ObjectToIntMap<CategoryPath> allExpectedCounts, termExpectedCounts;
  private static FacetIndexingParams fip;

  @AfterClass
  public static void afterClassCountingFacetsAggregatorTest() throws Exception {
    IOUtils.close(indexDir, taxoDir); 
  }
  
  private static List<CategoryPath> randomCategories(Random random) {
    // add random categories from the two dimensions, ensuring that the same
    // category is not added twice.
    int numFacetsA = random.nextInt(3) + 1; // 1-3
    int numFacetsB = random.nextInt(2) + 1; // 1-2
    ArrayList<CategoryPath> categories_a = new ArrayList<CategoryPath>();
    categories_a.addAll(Arrays.asList(CATEGORIES_A));
    ArrayList<CategoryPath> categories_b = new ArrayList<CategoryPath>();
    categories_b.addAll(Arrays.asList(CATEGORIES_B));
    Collections.shuffle(categories_a, random);
    Collections.shuffle(categories_b, random);

    ArrayList<CategoryPath> categories = new ArrayList<CategoryPath>();
    categories.addAll(categories_a.subList(0, numFacetsA));
    categories.addAll(categories_b.subList(0, numFacetsB));
    
    // add the NO_PARENT categories
    categories.add(CATEGORIES_C[random().nextInt(NUM_CHILDREN_CP_C)]);
    categories.add(CATEGORIES_D[random().nextInt(NUM_CHILDREN_CP_D)]);

    return categories;
  }

  private static void addField(Document doc) {
    doc.add(new StringField(A.field(), A.text(), Store.NO));
  }
  
  private static void addFacets(Document doc, FacetFields facetFields, boolean updateTermExpectedCounts) 
      throws IOException {
    List<CategoryPath> docCategories = randomCategories(random());
    for (CategoryPath cp : docCategories) {
      if (cp.components[0].equals(CP_D.components[0])) {
        cp = cp.subpath(2); // we'll get counts for the 2nd level only
      }
      allExpectedCounts.put(cp, allExpectedCounts.get(cp) + 1);
      if (updateTermExpectedCounts) {
        termExpectedCounts.put(cp, termExpectedCounts.get(cp) + 1);
      }
    }
    // add 1 to each NO_PARENTS dimension
    allExpectedCounts.put(CP_B, allExpectedCounts.get(CP_B) + 1);
    allExpectedCounts.put(CP_C, allExpectedCounts.get(CP_C) + 1);
    allExpectedCounts.put(CP_D, allExpectedCounts.get(CP_D) + 1);
    if (updateTermExpectedCounts) {
      termExpectedCounts.put(CP_B, termExpectedCounts.get(CP_B) + 1);
      termExpectedCounts.put(CP_C, termExpectedCounts.get(CP_C) + 1);
      termExpectedCounts.put(CP_D, termExpectedCounts.get(CP_D) + 1);
    }
    
    facetFields.addFields(doc, docCategories);
  }
  
  private static void indexDocsNoFacets(IndexWriter indexWriter) throws IOException {
    int numDocs = atLeast(2);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      addField(doc);
      indexWriter.addDocument(doc);
    }
    indexWriter.commit(); // flush a segment
  }
  
  private static void indexDocsWithFacetsNoTerms(IndexWriter indexWriter, TaxonomyWriter taxoWriter, 
      ObjectToIntMap<CategoryPath> expectedCounts) throws IOException {
    Random random = random();
    int numDocs = atLeast(random, 2);
    FacetFields facetFields = new FacetFields(taxoWriter, fip);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      addFacets(doc, facetFields, false);
      indexWriter.addDocument(doc);
    }
    indexWriter.commit(); // flush a segment
  }
  
  private static void indexDocsWithFacetsAndTerms(IndexWriter indexWriter, TaxonomyWriter taxoWriter, 
      ObjectToIntMap<CategoryPath> expectedCounts) throws IOException {
    Random random = random();
    int numDocs = atLeast(random, 2);
    FacetFields facetFields = new FacetFields(taxoWriter, fip);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      addFacets(doc, facetFields, true);
      addField(doc);
      indexWriter.addDocument(doc);
    }
    indexWriter.commit(); // flush a segment
  }
  
  private static void indexDocsWithFacetsAndSomeTerms(IndexWriter indexWriter, TaxonomyWriter taxoWriter, 
      ObjectToIntMap<CategoryPath> expectedCounts) throws IOException {
    Random random = random();
    int numDocs = atLeast(random, 2);
    FacetFields facetFields = new FacetFields(taxoWriter, fip);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      boolean hasContent = random.nextBoolean();
      if (hasContent) {
        addField(doc);
      }
      addFacets(doc, facetFields, hasContent);
      indexWriter.addDocument(doc);
    }
    indexWriter.commit(); // flush a segment
  }
  
  // initialize expectedCounts w/ 0 for all categories
  private static ObjectToIntMap<CategoryPath> newCounts() {
    ObjectToIntMap<CategoryPath> counts = new ObjectToIntMap<CategoryPath>();
    counts.put(CP_A, 0);
    counts.put(CP_B, 0);
    counts.put(CP_C, 0);
    counts.put(CP_D, 0);
    for (CategoryPath cp : CATEGORIES_A) {
      counts.put(cp, 0);
    }
    for (CategoryPath cp : CATEGORIES_B) {
      counts.put(cp, 0);
    }
    for (CategoryPath cp : CATEGORIES_C) {
      counts.put(cp, 0);
    }
    for (CategoryPath cp : CATEGORIES_D) {
      counts.put(cp.subpath(2), 0);
    }
    return counts;
  }
  
  @BeforeClass
  public static void beforeClassCountingFacetsAggregatorTest() throws Exception {
    indexDir = newDirectory();
    taxoDir = newDirectory();
    
    // create an index which has:
    // 1. Segment with no categories, but matching results
    // 2. Segment w/ categories, but no results
    // 3. Segment w/ categories and results
    // 4. Segment w/ categories, but only some results
    
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setMergePolicy(NoMergePolicy.COMPOUND_FILES); // prevent merges, so we can control the index segments
    IndexWriter indexWriter = new IndexWriter(indexDir, conf);
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

    Map<String,OrdinalPolicy> policies = new HashMap<String,CategoryListParams.OrdinalPolicy>();
    policies.put(CP_B.components[0], OrdinalPolicy.ALL_PARENTS);
    policies.put(CP_C.components[0], OrdinalPolicy.NO_PARENTS);
    policies.put(CP_D.components[0], OrdinalPolicy.NO_PARENTS);
    CategoryListParams clp = new PerDimensionOrdinalPolicy(policies);
    fip = new FacetIndexingParams(clp);
    
    allExpectedCounts = newCounts();
    termExpectedCounts = newCounts();
    
    // segment w/ no categories
    indexDocsNoFacets(indexWriter);

    // segment w/ categories, no content
    indexDocsWithFacetsNoTerms(indexWriter, taxoWriter, allExpectedCounts);

    // segment w/ categories and content
    indexDocsWithFacetsAndTerms(indexWriter, taxoWriter, allExpectedCounts);
    
    // segment w/ categories and some content
    indexDocsWithFacetsAndSomeTerms(indexWriter, taxoWriter, allExpectedCounts);
    
    IOUtils.close(indexWriter, taxoWriter);
  }
  
  private FacetsAccumulator randomAccumulator(FacetSearchParams fsp, IndexReader indexReader, TaxonomyReader taxoReader) {
    final FacetsAggregator aggregator;
    double val = random().nextDouble();
    if (val < 0.6) {
      aggregator = new FastCountingFacetsAggregator(); // it's the default, so give it the highest chance
    } else if (val < 0.8) {
      aggregator = new CountingFacetsAggregator();
    } else {
      aggregator = new CachedOrdsCountingFacetsAggregator();
    }
    return new FacetsAccumulator(fsp, indexReader, taxoReader) {
      @Override
      public FacetsAggregator getAggregator() {
        return aggregator;
      }
    };
  }
  
  @Test
  public void testDifferentNumResults() throws Exception {
    // test the collector w/ FacetRequests and different numResults
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(indexReader);
    
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(CP_A, NUM_CHILDREN_CP_A), 
        new CountFacetRequest(CP_B, NUM_CHILDREN_CP_B));
    FacetsCollector fc = FacetsCollector.create(randomAccumulator(fsp, indexReader, taxoReader));
    TermQuery q = new TermQuery(A);
    searcher.search(q, fc);
    
    List<FacetResult> facetResults = fc.getFacetResults();
    assertEquals("invalid number of facet results", 2, facetResults.size());
    for (FacetResult res : facetResults) {
      FacetResultNode root = res.getFacetResultNode();
      assertEquals("wrong count for " + root.label, termExpectedCounts.get(root.label), (int) root.value);
      for (FacetResultNode child : root.subResults) {
        assertEquals("wrong count for " + child.label, termExpectedCounts.get(child.label), (int) child.value);
      }
    }
    
    IOUtils.close(indexReader, taxoReader);
  }
  
  @Test
  public void testAllCounts() throws Exception {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(indexReader);
    
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(CP_A, NUM_CHILDREN_CP_A), 
        new CountFacetRequest(CP_B, NUM_CHILDREN_CP_B));
    FacetsCollector fc = FacetsCollector.create(randomAccumulator(fsp, indexReader, taxoReader));
    searcher.search(new MatchAllDocsQuery(), fc);
    
    List<FacetResult> facetResults = fc.getFacetResults();
    assertEquals("invalid number of facet results", 2, facetResults.size());
    for (FacetResult res : facetResults) {
      FacetResultNode root = res.getFacetResultNode();
      assertEquals("wrong count for " + root.label, allExpectedCounts.get(root.label), (int) root.value);
      int prevValue = Integer.MAX_VALUE;
      int prevOrdinal = Integer.MAX_VALUE;
      for (FacetResultNode child : root.subResults) {
        assertEquals("wrong count for " + child.label, allExpectedCounts.get(child.label), (int) child.value);
        assertTrue("wrong sort order of sub results: child.value=" + child.value + " prevValue=" + prevValue, child.value <= prevValue);
        if (child.value == prevValue) {
          assertTrue("wrong sort order of sub results", child.ordinal < prevOrdinal);
        }
        prevValue = (int) child.value;
        prevOrdinal = child.ordinal;
      }
    }
    
    IOUtils.close(indexReader, taxoReader);
  }
  
  @Test
  public void testBigNumResults() throws Exception {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(indexReader);
    
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(CP_A, Integer.MAX_VALUE), 
        new CountFacetRequest(CP_B, Integer.MAX_VALUE));
    FacetsCollector fc = FacetsCollector.create(randomAccumulator(fsp, indexReader, taxoReader));
    searcher.search(new MatchAllDocsQuery(), fc);
    
    List<FacetResult> facetResults = fc.getFacetResults();
    assertEquals("invalid number of facet results", 2, facetResults.size());
    for (FacetResult res : facetResults) {
      FacetResultNode root = res.getFacetResultNode();
      assertEquals("wrong count for " + root.label, allExpectedCounts.get(root.label), (int) root.value);
      for (FacetResultNode child : root.subResults) {
        assertEquals("wrong count for " + child.label, allExpectedCounts.get(child.label), (int) child.value);
      }
    }
    
    IOUtils.close(indexReader, taxoReader);
  }
  
  @Test
  public void testNoParents() throws Exception {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(indexReader);
    FacetSearchParams fsp = new FacetSearchParams(fip, new CountFacetRequest(CP_C, NUM_CHILDREN_CP_C), 
        new CountFacetRequest(CP_D, NUM_CHILDREN_CP_D));
    FacetsCollector fc = FacetsCollector.create(randomAccumulator(fsp, indexReader, taxoReader));
    searcher.search(new MatchAllDocsQuery(), fc);
    
    List<FacetResult> facetResults = fc.getFacetResults();
    assertEquals("invalid number of facet results", fsp.facetRequests.size(), facetResults.size());
    for (FacetResult res : facetResults) {
      FacetResultNode root = res.getFacetResultNode();
      assertEquals("wrong count for " + root.label, allExpectedCounts.get(root.label), (int) root.value);
      for (FacetResultNode child : root.subResults) {
        assertEquals("wrong count for " + child.label, allExpectedCounts.get(child.label), (int) child.value);
      }
    }
    
    IOUtils.close(indexReader, taxoReader);
  }
  
}
