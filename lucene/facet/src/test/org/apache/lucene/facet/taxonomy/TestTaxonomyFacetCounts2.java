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
package org.apache.lucene.facet.taxonomy;

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
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTaxonomyFacetCounts2 extends FacetTestCase {
  
  private static final Term A = new Term("f", "a");
  private static final String CP_A = "A", CP_B = "B";
  private static final String CP_C = "C", CP_D = "D"; // indexed w/ NO_PARENTS
  private static final int NUM_CHILDREN_CP_A = 5, NUM_CHILDREN_CP_B = 3;
  private static final int NUM_CHILDREN_CP_C = 5, NUM_CHILDREN_CP_D = 5;
  private static final FacetField[] CATEGORIES_A, CATEGORIES_B;
  private static final FacetField[] CATEGORIES_C, CATEGORIES_D;
  static {
    CATEGORIES_A = new FacetField[NUM_CHILDREN_CP_A];
    for (int i = 0; i < NUM_CHILDREN_CP_A; i++) {
      CATEGORIES_A[i] = new FacetField(CP_A, Integer.toString(i));
    }
    CATEGORIES_B = new FacetField[NUM_CHILDREN_CP_B];
    for (int i = 0; i < NUM_CHILDREN_CP_B; i++) {
      CATEGORIES_B[i] = new FacetField(CP_B, Integer.toString(i));
    }
    
    // NO_PARENTS categories
    CATEGORIES_C = new FacetField[NUM_CHILDREN_CP_C];
    for (int i = 0; i < NUM_CHILDREN_CP_C; i++) {
      CATEGORIES_C[i] = new FacetField(CP_C, Integer.toString(i));
    }
    
    // Multi-level categories
    CATEGORIES_D = new FacetField[NUM_CHILDREN_CP_D];
    for (int i = 0; i < NUM_CHILDREN_CP_D; i++) {
      String val = Integer.toString(i);
      CATEGORIES_D[i] = new FacetField(CP_D, val, val + val); // e.g. D/1/11, D/2/22...
    }
  }
  
  private static Directory indexDir, taxoDir;
  private static Map<String,Integer> allExpectedCounts, termExpectedCounts;

  @AfterClass
  public static void afterClassCountingFacetsAggregatorTest() throws Exception {
    IOUtils.close(indexDir, taxoDir); 
    indexDir = taxoDir = null;
  }
  
  private static List<FacetField> randomCategories(Random random) {
    // add random categories from the two dimensions, ensuring that the same
    // category is not added twice.
    int numFacetsA = random.nextInt(3) + 1; // 1-3
    int numFacetsB = random.nextInt(2) + 1; // 1-2
    ArrayList<FacetField> categories_a = new ArrayList<>();
    categories_a.addAll(Arrays.asList(CATEGORIES_A));
    ArrayList<FacetField> categories_b = new ArrayList<>();
    categories_b.addAll(Arrays.asList(CATEGORIES_B));
    Collections.shuffle(categories_a, random);
    Collections.shuffle(categories_b, random);

    ArrayList<FacetField> categories = new ArrayList<>();
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

  private static void addFacets(Document doc, FacetsConfig config, boolean updateTermExpectedCounts) 
      throws IOException {
    List<FacetField> docCategories = randomCategories(random());
    for (FacetField ff : docCategories) {
      doc.add(ff);
      String cp = ff.dim + "/" + ff.path[0];
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
  }

  private static FacetsConfig getConfig() {
    FacetsConfig config = new FacetsConfig();
    config.setMultiValued("A", true);
    config.setMultiValued("B", true);
    config.setRequireDimCount("B", true);
    config.setHierarchical("D", true);
    return config;
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
                                                 Map<String,Integer> expectedCounts) throws IOException {
    Random random = random();
    int numDocs = atLeast(random, 2);
    FacetsConfig config = getConfig();
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      addFacets(doc, config, false);
      indexWriter.addDocument(config.build(taxoWriter, doc));
    }
    indexWriter.commit(); // flush a segment
  }
  
  private static void indexDocsWithFacetsAndTerms(IndexWriter indexWriter, TaxonomyWriter taxoWriter, 
                                                  Map<String,Integer> expectedCounts) throws IOException {
    Random random = random();
    int numDocs = atLeast(random, 2);
    FacetsConfig config = getConfig();
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      addFacets(doc, config, true);
      addField(doc);
      indexWriter.addDocument(config.build(taxoWriter, doc));
    }
    indexWriter.commit(); // flush a segment
  }
  
  private static void indexDocsWithFacetsAndSomeTerms(IndexWriter indexWriter, TaxonomyWriter taxoWriter, 
                                                      Map<String,Integer> expectedCounts) throws IOException {
    Random random = random();
    int numDocs = atLeast(random, 2);
    FacetsConfig config = getConfig();
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      boolean hasContent = random.nextBoolean();
      if (hasContent) {
        addField(doc);
      }
      addFacets(doc, config, hasContent);
      indexWriter.addDocument(config.build(taxoWriter, doc));
    }
    indexWriter.commit(); // flush a segment
  }
  
  // initialize expectedCounts w/ 0 for all categories
  private static Map<String,Integer> newCounts() {
    Map<String,Integer> counts = new HashMap<>();
    counts.put(CP_A, 0);
    counts.put(CP_B, 0);
    counts.put(CP_C, 0);
    counts.put(CP_D, 0);
    for (FacetField ff : CATEGORIES_A) {
      counts.put(ff.dim + "/" + ff.path[0], 0);
    }
    for (FacetField ff : CATEGORIES_B) {
      counts.put(ff.dim + "/" + ff.path[0], 0);
    }
    for (FacetField ff : CATEGORIES_C) {
      counts.put(ff.dim + "/" + ff.path[0], 0);
    }
    for (FacetField ff : CATEGORIES_D) {
      counts.put(ff.dim + "/" + ff.path[0], 0);
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
    
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMergePolicy(NoMergePolicy.INSTANCE); // prevent merges, so we can control the index segments
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

    indexWriter.close();
    IOUtils.close(taxoWriter);
  }
  
  @Test
  public void testDifferentNumResults() throws Exception {
    // test the collector w/ FacetRequests and different numResults
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(indexReader);
    
    FacetsCollector sfc = new FacetsCollector();
    TermQuery q = new TermQuery(A);
    searcher.search(q, sfc);
    Facets facets = getTaxonomyFacetCounts(taxoReader, getConfig(), sfc);
    FacetResult result = facets.getTopChildren(NUM_CHILDREN_CP_A, CP_A);
    assertEquals(-1, result.value.intValue());
    for(LabelAndValue labelValue : result.labelValues) {
      assertEquals(termExpectedCounts.get(CP_A + "/" + labelValue.label), labelValue.value);
    }
    result = facets.getTopChildren(NUM_CHILDREN_CP_B, CP_B);
    assertEquals(termExpectedCounts.get(CP_B), result.value);
    for(LabelAndValue labelValue : result.labelValues) {
      assertEquals(termExpectedCounts.get(CP_B + "/" + labelValue.label), labelValue.value);
    }
    
    IOUtils.close(indexReader, taxoReader);
  }
  
  @Test
  public void testAllCounts() throws Exception {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(indexReader);
    
    FacetsCollector sfc = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), sfc);

    Facets facets = getTaxonomyFacetCounts(taxoReader, getConfig(), sfc);
    
    FacetResult result = facets.getTopChildren(NUM_CHILDREN_CP_A, CP_A);
    assertEquals(-1, result.value.intValue());
    int prevValue = Integer.MAX_VALUE;
    for(LabelAndValue labelValue : result.labelValues) {
      assertEquals(allExpectedCounts.get(CP_A + "/" + labelValue.label), labelValue.value);
      assertTrue("wrong sort order of sub results: labelValue.value=" + labelValue.value + " prevValue=" + prevValue, labelValue.value.intValue() <= prevValue);
      prevValue = labelValue.value.intValue();
    }

    result = facets.getTopChildren(NUM_CHILDREN_CP_B, CP_B);
    assertEquals(allExpectedCounts.get(CP_B), result.value);
    prevValue = Integer.MAX_VALUE;
    for(LabelAndValue labelValue : result.labelValues) {
      assertEquals(allExpectedCounts.get(CP_B + "/" + labelValue.label), labelValue.value);
      assertTrue("wrong sort order of sub results: labelValue.value=" + labelValue.value + " prevValue=" + prevValue, labelValue.value.intValue() <= prevValue);
      prevValue = labelValue.value.intValue();
    }

    IOUtils.close(indexReader, taxoReader);
  }
  
  @Test
  public void testBigNumResults() throws Exception {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(indexReader);
    
    FacetsCollector sfc = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), sfc);

    Facets facets = getTaxonomyFacetCounts(taxoReader, getConfig(), sfc);

    FacetResult result = facets.getTopChildren(Integer.MAX_VALUE, CP_A);
    assertEquals(-1, result.value.intValue());
    for(LabelAndValue labelValue : result.labelValues) {
      assertEquals(allExpectedCounts.get(CP_A + "/" + labelValue.label), labelValue.value);
    }
    result = facets.getTopChildren(Integer.MAX_VALUE, CP_B);
    assertEquals(allExpectedCounts.get(CP_B), result.value);
    for(LabelAndValue labelValue : result.labelValues) {
      assertEquals(allExpectedCounts.get(CP_B + "/" + labelValue.label), labelValue.value);
    }
    
    IOUtils.close(indexReader, taxoReader);
  }
  
  @Test
  public void testNoParents() throws Exception {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = newSearcher(indexReader);
    
    FacetsCollector sfc = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), sfc);

    Facets facets = getTaxonomyFacetCounts(taxoReader, getConfig(), sfc);

    FacetResult result = facets.getTopChildren(NUM_CHILDREN_CP_C, CP_C);
    assertEquals(allExpectedCounts.get(CP_C), result.value);
    for(LabelAndValue labelValue : result.labelValues) {
      assertEquals(allExpectedCounts.get(CP_C + "/" + labelValue.label), labelValue.value);
    }
    result = facets.getTopChildren(NUM_CHILDREN_CP_D, CP_D);
    assertEquals(allExpectedCounts.get(CP_C), result.value);
    for(LabelAndValue labelValue : result.labelValues) {
      assertEquals(allExpectedCounts.get(CP_D + "/" + labelValue.label), labelValue.value);
    }
    
    IOUtils.close(indexReader, taxoReader);
  }
}
