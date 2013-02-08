package org.apache.lucene.facet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.collections.IntToObjectMap;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.params.CategoryListParams.OrdinalPolicy;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

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

@SuppressCodecs({"SimpleText","Lucene3x"})
public abstract class FacetTestBase extends FacetTestCase {
  
  /** Holds a search and taxonomy Directories pair. */
  private static final class SearchTaxoDirPair {
    Directory searchDir, taxoDir;
    SearchTaxoDirPair() {}
  }
  
  private static IntToObjectMap<SearchTaxoDirPair> dirsPerPartitionSize;
  private static IntToObjectMap<FacetIndexingParams> fipPerPartitionSize;
  private static File TEST_DIR;
  
  /** Documents text field. */
  protected static final String CONTENT_FIELD = "content";
  
  /** taxonomy Reader for the test. */
  protected TaxonomyReader taxoReader;
  
  /** Index Reader for the test. */
  protected IndexReader indexReader;
  
  /** Searcher for the test. */
  protected IndexSearcher searcher;
  
  @BeforeClass
  public static void beforeClassFacetTestBase() {
    TEST_DIR = _TestUtil.getTempDir("facets");
    dirsPerPartitionSize = new IntToObjectMap<FacetTestBase.SearchTaxoDirPair>();
    fipPerPartitionSize = new IntToObjectMap<FacetIndexingParams>();
  }
  
  @AfterClass
  public static void afterClassFacetTestBase() throws Exception {
    Iterator<SearchTaxoDirPair> iter = dirsPerPartitionSize.iterator();
    while (iter.hasNext()) {
      SearchTaxoDirPair pair = iter.next();
      IOUtils.close(pair.searchDir, pair.taxoDir);
    }
  }
  
  /** documents text (for the text field). */
  private static final String[] DEFAULT_CONTENT = {
      "the white car is the one I want.",
      "the white dog does not belong to anyone.",
  };
  
  /** Facets: facets[D][F] == category-path no. F for document no. D. */
  private static final CategoryPath[][] DEFAULT_CATEGORIES = {
      { new CategoryPath("root","a","f1"), new CategoryPath("root","a","f2") },
      { new CategoryPath("root","a","f1"), new CategoryPath("root","a","f3") },
  };
  
  /** categories to be added to specified doc */
  protected List<CategoryPath> getCategories(int doc) {
    return Arrays.asList(DEFAULT_CATEGORIES[doc]);
  }
  
  /** Number of documents to index */
  protected int numDocsToIndex() {
    return DEFAULT_CONTENT.length;
  }
  
  /** content to be added to specified doc */
  protected String getContent(int doc) {
    return DEFAULT_CONTENT[doc];
  }
  
  /** Prepare index (in RAM) with some documents and some facets. */
  protected final void initIndex(FacetIndexingParams fip) throws Exception {
    initIndex(false, fip);
  }

  /** Prepare index (in RAM/Disk) with some documents and some facets. */
  protected final void initIndex(boolean forceDisk, FacetIndexingParams fip) throws Exception {
    int partitionSize = fip.getPartitionSize();
    if (VERBOSE) {
      System.out.println("Partition Size: " + partitionSize + "  forceDisk: "+forceDisk);
    }

    SearchTaxoDirPair pair = dirsPerPartitionSize.get(Integer.valueOf(partitionSize));
    if (pair == null) {
      pair = new SearchTaxoDirPair();
      if (forceDisk) {
        pair.searchDir = newFSDirectory(new File(TEST_DIR, "index"));
        pair.taxoDir = newFSDirectory(new File(TEST_DIR, "taxo"));
      } else {
        pair.searchDir = newDirectory();
        pair.taxoDir = newDirectory();
      }
      
      RandomIndexWriter iw = new RandomIndexWriter(random(), pair.searchDir, getIndexWriterConfig(getAnalyzer()));
      TaxonomyWriter taxo = new DirectoryTaxonomyWriter(pair.taxoDir, OpenMode.CREATE);
      
      populateIndex(iw, taxo, fip);
      
      // commit changes (taxonomy prior to search index for consistency)
      taxo.commit();
      iw.commit();
      taxo.close();
      iw.close();
      
      dirsPerPartitionSize.put(Integer.valueOf(partitionSize), pair);
    }
    
    // prepare for searching
    taxoReader = new DirectoryTaxonomyReader(pair.taxoDir);
    indexReader = DirectoryReader.open(pair.searchDir);
    searcher = newSearcher(indexReader);
  }
  
  /** Returns indexing params for the main index */
  protected IndexWriterConfig getIndexWriterConfig(Analyzer analyzer) {
    return newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
  }

  /** Returns a {@link FacetIndexingParams} per the given partition size. */
  protected FacetIndexingParams getFacetIndexingParams(final int partSize) {
    return getFacetIndexingParams(partSize, false);
  }
  
  /**
   * Returns a {@link FacetIndexingParams} per the given partition size. If
   * requested, then {@link OrdinalPolicy} will be set to
   * {@link OrdinalPolicy#ALL_PARENTS}, otherwise it will randomize.
   */
  protected FacetIndexingParams getFacetIndexingParams(final int partSize, final boolean forceAllParents) {
    FacetIndexingParams fip = fipPerPartitionSize.get(partSize);
    if (fip == null) {
      // randomize OrdinalPolicy. Since not all Collectors / Accumulators
      // support NO_PARENTS, don't include it.
      // TODO: once all code paths support NO_PARENTS, randomize it too.
      CategoryListParams randomOP = new CategoryListParams() {
        final OrdinalPolicy op = random().nextBoolean() ? OrdinalPolicy.ALL_BUT_DIMENSION : OrdinalPolicy.ALL_PARENTS;
        @Override
        public OrdinalPolicy getOrdinalPolicy(String dimension) {
          return forceAllParents ? OrdinalPolicy.ALL_PARENTS : op;
        }
      };
      
      // several of our encoders don't support the value 0, 
      // which is one of the values encoded when dealing w/ partitions,
      // therefore don't randomize the encoder.
      fip = new FacetIndexingParams(randomOP) {
        @Override
        public int getPartitionSize() {
          return partSize;
        }
      };
      fipPerPartitionSize.put(partSize, fip);
    }
    return fip;
  }
  
  /**
   * Faceted Search Params for the test. Sub classes should override in order to
   * test with different faceted search params.
   */
  protected FacetSearchParams getFacetSearchParams(FacetIndexingParams iParams, FacetRequest... facetRequests) {
    return new FacetSearchParams(iParams, facetRequests);
  }

  /**
   * Faceted Search Params for the test. Sub classes should override in order to
   * test with different faceted search params.
   */
  protected FacetSearchParams getFacetSearchParams(List<FacetRequest> facetRequests, FacetIndexingParams iParams) {
    return new FacetSearchParams(iParams, facetRequests);
  }

  /**
   * Populate the test index+taxonomy for this test.
   * <p>Subclasses can override this to test different scenarios
   */
  protected void populateIndex(RandomIndexWriter iw, TaxonomyWriter taxo, FacetIndexingParams iParams)
      throws IOException {
    // add test documents 
    int numDocsToIndex = numDocsToIndex();
    for (int doc=0; doc<numDocsToIndex; doc++) {
      indexDoc(iParams, iw, taxo, getContent(doc), getCategories(doc));
    }
    
    // also add a document that would be deleted, so that all tests are also working against deletions in the index
    String content4del = "ContentOfDocToDelete";
    indexDoc(iParams, iw, taxo, content4del, getCategories(0));
    iw.commit(); // commit it
    iw.deleteDocuments(new Term(CONTENT_FIELD,content4del)); // now delete the committed doc 
  }
  
  /** Close all indexes */
  protected void closeAll() throws Exception {
    // close and nullify everything
    IOUtils.close(taxoReader, indexReader);
    taxoReader = null;
    indexReader = null;
    searcher = null;
  }
  
  /**
   * Analyzer to use for the test.
   * Sub classes should override in order to test with different analyzer.
   */
  protected Analyzer getAnalyzer() {
    return new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
  }
  
  /** convenience method: convert sub results to an array */  
  protected static FacetResultNode[] resultNodesAsArray(FacetResultNode parentRes) {
    ArrayList<FacetResultNode> a = new ArrayList<FacetResultNode>();
    for (FacetResultNode frn : parentRes.subResults) {
      a.add(frn);
    }
    return a.toArray(new FacetResultNode[0]);
  }
  
  /** utility Create a dummy document with specified categories and content */
  protected final void indexDoc(FacetIndexingParams iParams, RandomIndexWriter iw,
      TaxonomyWriter tw, String content, List<CategoryPath> categories) throws IOException {
    Document d = new Document();
    FacetFields facetFields = new FacetFields(tw, iParams);
    facetFields.addFields(d, categories);
    d.add(new TextField("content", content, Field.Store.YES));
    iw.addDocument(d);
  }
  
  /** Build the "truth" with ALL the facets enumerating indexes content. */
  protected Map<CategoryPath, Integer> facetCountsTruth() throws IOException {
    FacetIndexingParams iParams = getFacetIndexingParams(Integer.MAX_VALUE);
    String delim = String.valueOf(iParams.getFacetDelimChar());
    Map<CategoryPath, Integer> res = new HashMap<CategoryPath, Integer>();
    HashSet<String> handledTerms = new HashSet<String>();
    for (CategoryListParams clp : iParams.getAllCategoryListParams()) {
      if (!handledTerms.add(clp.field)) {
        continue; // already handled this term (for another list) 
      }
      Terms terms = MultiFields.getTerms(indexReader, clp.field);
      if (terms == null) {
        continue;
      }
      Bits liveDocs = MultiFields.getLiveDocs(indexReader);
      TermsEnum te = terms.iterator(null);
      DocsEnum de = null;
      while (te.next() != null) {
        de = _TestUtil.docs(random(), te, liveDocs, de, DocsEnum.FLAG_NONE);
        int cnt = 0;
        while (de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          cnt++;
        }
        res.put(new CategoryPath(te.term().utf8ToString().split(delim)), cnt);
      }
    }
    return res;
  }
  
  /** Validate counts for returned facets, and that there are not too many results */
  protected static void assertCountsAndCardinality(Map<CategoryPath, Integer> facetCountsTruth, List<FacetResult> facetResults) throws Exception {
    for (FacetResult fr : facetResults) {
      FacetResultNode topResNode = fr.getFacetResultNode();
      FacetRequest freq = fr.getFacetRequest();
      if (VERBOSE) {
        System.out.println(freq.categoryPath.toString()+ "\t\t" + topResNode);
      }
      assertCountsAndCardinality(facetCountsTruth, topResNode, freq.numResults);
    }
  }
    
  /** Validate counts for returned facets, and that there are not too many results */
  private static void assertCountsAndCardinality(Map<CategoryPath,Integer> facetCountsTruth,  FacetResultNode resNode, int reqNumResults) throws Exception {
    int actualNumResults = resNode.subResults.size();
    if (VERBOSE) {
      System.out.println("NumResults: " + actualNumResults);
    }
    assertTrue("Too many results!", actualNumResults <= reqNumResults);
    for (FacetResultNode subRes : resNode.subResults) {
      assertEquals("wrong count for: "+subRes, facetCountsTruth.get(subRes.label).intValue(), (int)subRes.value);
      assertCountsAndCardinality(facetCountsTruth, subRes, reqNumResults); // recurse into child results
    }
  }

  /** Validate results equality */
  protected static void assertSameResults(List<FacetResult> expected, List<FacetResult> actual) {
    assertEquals("wrong number of facet results", expected.size(), actual.size());
    int size = expected.size();
    for (int i = 0; i < size; i++) {
      FacetResult expectedResult = expected.get(i);
      FacetResult actualResult = actual.get(i);
      String expectedStr = FacetTestUtils.toSimpleString(expectedResult);
      String actualStr = FacetTestUtils.toSimpleString(actualResult);
      assertEquals("Results not the same!\nExpected:" + expectedStr + "\nActual:\n" + actualStr, expectedStr, actualStr);
    }
  }
  
}
