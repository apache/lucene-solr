package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.index.categorypolicy.OrdinalPolicy;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.params.FacetRequest.SortBy;
import org.apache.lucene.facet.search.params.FacetRequest.SortOrder;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.params.ScoreFacetRequest;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.collections.ObjectToIntMap;
import org.apache.lucene.util.encoding.IntEncoder;
import org.apache.lucene.util.encoding.VInt8IntEncoder;
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

public class CountingFacetsCollectorTest extends LuceneTestCase {
  
  private static final Term A = new Term("f", "a");
  private static final CategoryPath CP_A = new CategoryPath("A"), CP_B = new CategoryPath("B");
  private static final int NUM_CHILDREN_CP_A = 5, NUM_CHILDREN_CP_B = 3;
  private static final CategoryPath[] CATEGORIES_A, CATEGORIES_B;
  static {
    CATEGORIES_A = new CategoryPath[NUM_CHILDREN_CP_A];
    for (int i = 0; i < NUM_CHILDREN_CP_A; i++) {
      CATEGORIES_A[i] = new CategoryPath(CP_A.components[0], Integer.toString(i));
    }
    CATEGORIES_B = new CategoryPath[NUM_CHILDREN_CP_B];
    for (int i = 0; i < NUM_CHILDREN_CP_B; i++) {
      CATEGORIES_B[i] = new CategoryPath(CP_B.components[0], Integer.toString(i));
    }
  }
  
  protected static Directory indexDir, taxoDir;
  protected static ObjectToIntMap<CategoryPath> allExpectedCounts, termExpectedCounts;
  protected static int numChildrenIndexedA, numChildrenIndexedB;

  @AfterClass
  public static void afterClassCountingFacetsCollectorTest() throws Exception {
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
    return categories;
  }

  private static void addField(Document doc) {
    doc.add(new StringField(A.field(), A.text(), Store.NO));
  }
  
  private static void addFacets(Document doc, FacetFields facetFields, boolean updateTermExpectedCounts) 
      throws IOException {
    List<CategoryPath> docCategories = randomCategories(random());
    for (CategoryPath cp : docCategories) {
      allExpectedCounts.put(cp, allExpectedCounts.get(cp) + 1);
      if (updateTermExpectedCounts) {
        termExpectedCounts.put(cp, termExpectedCounts.get(cp) + 1);
      }
    }
    // add 1 to each dimension
    allExpectedCounts.put(CP_A, allExpectedCounts.get(CP_A) + 1);
    allExpectedCounts.put(CP_B, allExpectedCounts.get(CP_B) + 1);
    if (updateTermExpectedCounts) {
      termExpectedCounts.put(CP_A, termExpectedCounts.get(CP_A) + 1);
      termExpectedCounts.put(CP_B, termExpectedCounts.get(CP_B) + 1);
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
    FacetFields facetFields = new FacetFields(taxoWriter);
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
    FacetFields facetFields = new FacetFields(taxoWriter);
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
    FacetFields facetFields = new FacetFields(taxoWriter);
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
    for (CategoryPath cp : CATEGORIES_A) {
      counts.put(cp, 0);
    }
    for (CategoryPath cp : CATEGORIES_B) {
      counts.put(cp, 0);
    }
    return counts;
  }
  
  @BeforeClass
  public static void beforeClassCountingFacetsCollectorTest() throws Exception {
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
    
    // set num children indexed from each dimension
    for (CategoryPath cp : CATEGORIES_A) {
      if (termExpectedCounts.get(cp) > 0) {
        ++numChildrenIndexedA;
      }
    }
    for (CategoryPath cp : CATEGORIES_B) {
      if (termExpectedCounts.get(cp) > 0) {
        ++numChildrenIndexedB;
      }
    }
    
    IOUtils.close(indexWriter, taxoWriter);
  }
  
  @Test
  public void testInvalidValidParams() throws Exception {
    final CategoryPath dummyCP = new CategoryPath("a");
    final FacetRequest dummyFR = new CountFacetRequest(dummyCP, 10);

    // only CountFacetRequests are allowed
    assertNotNull("only CountFacetRequests should be allowed", 
        CountingFacetsCollector.assertParams(new FacetSearchParams(new ScoreFacetRequest(dummyCP, 10))));

    // only depth=1
    FacetRequest cfr = new CountFacetRequest(dummyCP, 10);
    cfr.setDepth(2);
    assertNotNull("only depth 1 should be allowed", CountingFacetsCollector.assertParams(new FacetSearchParams(cfr)));

    // only SortOrder.DESCENDING
    cfr = new CountFacetRequest(dummyCP, 10);
    cfr.setSortOrder(SortOrder.ASCENDING);
    assertNotNull("only SortOrder.DESCENDING should be allowed", CountingFacetsCollector.assertParams(new FacetSearchParams(cfr)));
    
    // only SortBy.VALUE
    cfr = new CountFacetRequest(dummyCP, 10);
    cfr.setSortBy(SortBy.ORDINAL);
    assertNotNull("only SortBy.VALUE should be allowed", CountingFacetsCollector.assertParams(new FacetSearchParams(cfr)));

    // no numToLabel
    cfr = new CountFacetRequest(dummyCP, 10);
    cfr.setNumLabel(2);
    assertNotNull("numToLabel should not be allowed", CountingFacetsCollector.assertParams(new FacetSearchParams(cfr)));
    
    FacetIndexingParams fip = new FacetIndexingParams(new CategoryListParams("moo")) {
      @Override
      public List<CategoryListParams> getAllCategoryListParams() {
        return Arrays.asList(new CategoryListParams[] { clParams, clParams });
      }
    };
    assertNotNull("only one CLP should be allowed", CountingFacetsCollector.assertParams(new FacetSearchParams(fip, dummyFR)));
    
    fip = new FacetIndexingParams(new CategoryListParams("moo")) {
      final CategoryListParams clp = new CategoryListParams() {
        @Override
        public IntEncoder createEncoder() {
          return new VInt8IntEncoder();
        }
      };
      @Override
      public List<CategoryListParams> getAllCategoryListParams() {
        return Collections.singletonList(clp);
      }
      
      @Override
      public CategoryListParams getCategoryListParams(CategoryPath category) {
        return clp;
      }
    };
    assertNotNull("only DGapVIntEncoder should be allowed", CountingFacetsCollector.assertParams(new FacetSearchParams(fip, dummyFR)));

    fip = new FacetIndexingParams(new CategoryListParams("moo")) {
      @Override
      public int getPartitionSize() {
        return 2;
      }
    };
    assertNotNull("partitions should be allowed", CountingFacetsCollector.assertParams(new FacetSearchParams(fip, dummyFR)));
  }

  @Test
  public void testDifferentNumResults() throws Exception {
    // test the collector w/ FacetRequests and different numResults
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(CP_A, NUM_CHILDREN_CP_A), 
        new CountFacetRequest(CP_B, NUM_CHILDREN_CP_B));
    FacetsCollector fc = new CountingFacetsCollector(fsp , taxoReader);
    TermQuery q = new TermQuery(A);
    searcher.search(q, fc);
    
    List<FacetResult> facetResults = fc.getFacetResults();
    assertEquals("invalid number of facet results", 2, facetResults.size());
    for (FacetResult res : facetResults) {
      FacetResultNode root = res.getFacetResultNode();
      assertEquals("wrong count for " + root.label, termExpectedCounts.get(root.label), (int) root.value);
      assertEquals("invalid residue", 0, (int) root.residue);
      for (FacetResultNode child : root.subResults) {
        assertEquals("wrong count for " + child.label, termExpectedCounts.get(child.label), (int) child.value);
      }
    }
    
    IOUtils.close(indexReader, taxoReader);
  }
  
  @Test
  public void testResidue() throws Exception {
    // test the collector's handling of residue
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    
    // asking for top 1 is the only way to guarantee there will be a residue
    // provided that enough children were indexed (see below)
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(CP_A, 1), new CountFacetRequest(CP_B, 1));
    FacetsCollector fc = new CountingFacetsCollector(fsp , taxoReader);
    TermQuery q = new TermQuery(A);
    searcher.search(q, fc);
    
    List<FacetResult> facetResults = fc.getFacetResults();
    assertEquals("invalid number of facet results", 2, facetResults.size());
    for (FacetResult res : facetResults) {
      FacetResultNode root = res.getFacetResultNode();
      assertEquals("wrong count for " + root.label, termExpectedCounts.get(root.label), (int) root.value);
      // make sure randomness didn't pick only one child of root (otherwise there's no residue)
      int numChildrenIndexed = res.getFacetRequest().categoryPath == CP_A ? numChildrenIndexedA : numChildrenIndexedB;
      if (numChildrenIndexed > 1) {
        assertTrue("expected residue", root.residue > 0);
      }
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
    IndexSearcher searcher = new IndexSearcher(indexReader);
    
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(CP_A, NUM_CHILDREN_CP_A), 
        new CountFacetRequest(CP_B, NUM_CHILDREN_CP_B));
    FacetsCollector fc = new CountingFacetsCollector(fsp , taxoReader);
    searcher.search(new MatchAllDocsQuery(), fc);
    
    List<FacetResult> facetResults = fc.getFacetResults();
    assertEquals("invalid number of facet results", 2, facetResults.size());
    for (FacetResult res : facetResults) {
      FacetResultNode root = res.getFacetResultNode();
      assertEquals("wrong count for " + root.label, allExpectedCounts.get(root.label), (int) root.value);
      assertEquals("invalid residue", 0, (int) root.residue);
      for (FacetResultNode child : root.subResults) {
        assertEquals("wrong count for " + child.label, allExpectedCounts.get(child.label), (int) child.value);
      }
    }
    
    IOUtils.close(indexReader, taxoReader);
  }
  
  @Test
  public void testBigNumResults() throws Exception {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(CP_A, Integer.MAX_VALUE), 
        new CountFacetRequest(CP_B, Integer.MAX_VALUE));
    FacetsCollector fc = new CountingFacetsCollector(fsp , taxoReader);
    searcher.search(new MatchAllDocsQuery(), fc);
    
    List<FacetResult> facetResults = fc.getFacetResults();
    assertEquals("invalid number of facet results", 2, facetResults.size());
    for (FacetResult res : facetResults) {
      FacetResultNode root = res.getFacetResultNode();
      assertEquals("wrong count for " + root.label, allExpectedCounts.get(root.label), (int) root.value);
      assertEquals("invalid residue", 0, (int) root.residue);
      for (FacetResultNode child : root.subResults) {
        assertEquals("wrong count for " + child.label, allExpectedCounts.get(child.label), (int) child.value);
      }
    }
    
    IOUtils.close(indexReader, taxoReader);
  }
  
  @Test
  public void testDirectSource() throws Exception {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(CP_A, NUM_CHILDREN_CP_A), 
        new CountFacetRequest(CP_B, NUM_CHILDREN_CP_B));
    FacetsCollector fc = new CountingFacetsCollector(fsp , taxoReader, new FacetArrays(taxoReader.getSize()), true);
    searcher.search(new MatchAllDocsQuery(), fc);
    
    List<FacetResult> facetResults = fc.getFacetResults();
    assertEquals("invalid number of facet results", 2, facetResults.size());
    for (FacetResult res : facetResults) {
      FacetResultNode root = res.getFacetResultNode();
      assertEquals("wrong count for " + root.label, allExpectedCounts.get(root.label), (int) root.value);
      assertEquals("invalid residue", 0, (int) root.residue);
      for (FacetResultNode child : root.subResults) {
        assertEquals("wrong count for " + child.label, allExpectedCounts.get(child.label), (int) child.value);
      }
    }
    
    IOUtils.close(indexReader, taxoReader);
  }

  @Test
  public void testNoParents() throws Exception {
    // TODO: when OrdinalPolicy is on CLP, index the NO_PARENTS categories into
    // their own dimension, and avoid this index creation
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(2);
    conf.setMergePolicy(NoMergePolicy.COMPOUND_FILES);
    IndexWriter indexWriter = new IndexWriter(indexDir, conf);
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetIndexingParams fip = new FacetIndexingParams() {
      @Override
      public OrdinalPolicy getOrdinalPolicy() {
        return OrdinalPolicy.NO_PARENTS;
      }
    };
    FacetFields facetFields = new FacetFields(taxoWriter, fip);
    ObjectToIntMap<CategoryPath> expCounts = newCounts();

    // index few docs with categories, not sharing parents.
    int numDocs = atLeast(10);
    final CategoryPath cpc = new CategoryPath("L1", "L2", "L3");
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      ArrayList<CategoryPath> categories = new ArrayList<CategoryPath>();
      CategoryPath cpa = CATEGORIES_A[random().nextInt(NUM_CHILDREN_CP_A)];
      CategoryPath cpb = CATEGORIES_B[random().nextInt(NUM_CHILDREN_CP_B)];
      categories.add(cpa);
      categories.add(cpb);
      categories.add(cpc);
      expCounts.put(cpa, expCounts.get(cpa) + 1);
      expCounts.put(cpb, expCounts.get(cpb) + 1);
      facetFields.addFields(doc, categories);
      indexWriter.addDocument(doc);
    }
    expCounts.put(CP_A, numDocs);
    expCounts.put(CP_B, numDocs);
    for (int i = 0; i < cpc.length; i++) {
      expCounts.put(cpc.subpath(i+1), numDocs);
    }
    
    IOUtils.close(indexWriter, taxoWriter);

    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    FacetSearchParams fsp = new FacetSearchParams(fip, new CountFacetRequest(CP_A, NUM_CHILDREN_CP_A), 
        new CountFacetRequest(CP_B, NUM_CHILDREN_CP_B), new CountFacetRequest(cpc.subpath(1), 10));
    FacetsCollector fc = new CountingFacetsCollector(fsp , taxoReader);
    searcher.search(new MatchAllDocsQuery(), fc);
    
    List<FacetResult> facetResults = fc.getFacetResults();
    assertEquals("invalid number of facet results", 3, facetResults.size());
    for (FacetResult res : facetResults) {
      FacetResultNode root = res.getFacetResultNode();
      assertEquals("wrong count for " + root.label, expCounts.get(root.label), (int) root.value);
      assertEquals("invalid residue", 0, (int) root.residue);
      for (FacetResultNode child : root.subResults) {
        assertEquals("wrong count for " + child.label, expCounts.get(child.label), (int) child.value);
      }
    }
    
    IOUtils.close(indexReader, taxoReader);
    
    IOUtils.close(indexDir, taxoDir);
  }
  
}
