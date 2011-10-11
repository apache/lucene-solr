package org.apache.lucene.facet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.lucene.DocumentBuilder.DocumentBuilderException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
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
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.facet.index.CategoryDocumentBuilder;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.DefaultFacetIndexingParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.lucene.LuceneTaxonomyReader;
import org.apache.lucene.facet.taxonomy.lucene.LuceneTaxonomyWriter;

/**
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

/** Base faceted search test. */
public abstract class FacetTestBase extends LuceneTestCase {
  
  /** Documents text field. */
  protected static final String CONTENT_FIELD = "content";
  
  /** Directory for the index */
  protected Directory indexDir;
  
  /** Directory for the taxonomy */
  protected Directory taxoDir;
  
  /** taxonomy Reader for the test. */
  protected TaxonomyReader taxoReader;
  
  /** Index Reader for the test. */
  protected IndexReader indexReader;
  
  /** Searcher for the test. */
  protected IndexSearcher searcher;
  
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
  
  /** Prepare index (in RAM) with single partition */
  protected final void initIndex() throws Exception {
    initIndex(Integer.MAX_VALUE);
  }
  
  /** Prepare index (in RAM) with some documents and some facets */
  protected final void initIndex(int partitionSize) throws Exception {
    initIndex(partitionSize, false);
  }

  /** Prepare index (in RAM/Disk) with some documents and some facets */
  protected final void initIndex(int partitionSize, boolean onDisk) throws Exception {
    if (VERBOSE) {
      System.out.println("Partition Size: " + partitionSize+"  onDisk: "+onDisk);
    }

    if (onDisk) {
      File indexFile = _TestUtil.getTempDir("index");
      indexDir = newFSDirectory(indexFile);
      taxoDir = newFSDirectory(new File(indexFile,"facets"));
    } else { 
      indexDir = newDirectory();
      taxoDir = newDirectory();
    }
    
    RandomIndexWriter iw = new RandomIndexWriter(random, indexDir, getIndexWriterConfig(getAnalyzer()));
    TaxonomyWriter taxo = new LuceneTaxonomyWriter(taxoDir, OpenMode.CREATE);
    
    populateIndex(iw, taxo, getFacetIndexingParams(partitionSize));
    
    // commit changes (taxonomy prior to search index for consistency)
    taxo.commit();
    iw.commit();
    taxo.close();
    iw.close();
    
    // prepare for searching
    taxoReader = new LuceneTaxonomyReader(taxoDir);
    indexReader = IndexReader.open(indexDir);
    searcher = newSearcher(indexReader);
  }
  
  /** Returns indexing params for the main index */
  protected IndexWriterConfig getIndexWriterConfig(Analyzer analyzer) {
    return newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
  }

  /** Returns a default facet indexing params */
  protected FacetIndexingParams getFacetIndexingParams(final int partSize) {
    return new DefaultFacetIndexingParams() {
      @Override
      protected int fixedPartitionSize() {
        return partSize;
      }
    };
  }
  
  /**
   * Faceted Search Params for the test.
   * Sub classes should override in order to test with different faceted search params.
   */
  protected FacetSearchParams getFacetedSearchParams() {
    return getFacetedSearchParams(Integer.MAX_VALUE);
  }

  /**
   * Faceted Search Params with specified partition size.
   * @see #getFacetedSearchParams()
   */
  protected FacetSearchParams getFacetedSearchParams(int partitionSize) {
    FacetSearchParams res = new FacetSearchParams(getFacetIndexingParams(partitionSize));
    return res; 
  }

  /**
   * Populate the test index+taxonomy for this test.
   * <p>Subclasses can override this to test different scenarios
   */
  protected void populateIndex(RandomIndexWriter iw, TaxonomyWriter taxo, FacetIndexingParams iParams)
      throws IOException, DocumentBuilderException, CorruptIndexException {
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
    taxoReader.close();
    taxoReader = null;
    indexReader.close();
    indexReader = null;
    searcher.close();
    searcher = null;
    indexDir.close();
    indexDir = null;
    taxoDir.close();
    taxoDir = null;
  }
  
  /**
   * Analyzer to use for the test.
   * Sub classes should override in order to test with different analyzer.
   */
  protected Analyzer getAnalyzer() {
    return new MockAnalyzer(random, MockTokenizer.WHITESPACE, false);
  }
  
  /** convenience method: convert sub results to an array */  
  protected static FacetResultNode[] resultNodesAsArray(FacetResultNode parentRes) {
    ArrayList<FacetResultNode> a = new ArrayList<FacetResultNode>();
    for (FacetResultNode frn : parentRes.getSubResults()) {
      a.add(frn);
    }
    return a.toArray(new FacetResultNode[0]);
  }
  
  /** utility Create a dummy document with specified categories and content */
  protected final void indexDoc(FacetIndexingParams iParams, RandomIndexWriter iw,
      TaxonomyWriter tw, String content, List<CategoryPath> categories) throws IOException,
      CorruptIndexException {
    Document d = new Document();
    CategoryDocumentBuilder builder = new CategoryDocumentBuilder(tw, iParams);
    builder.setCategoryPaths(categories);
    builder.build(d);
    d.add(new Field("content", content, TextField.TYPE_STORED));
    iw.addDocument(d);
  }
  
  /** Build the "truth" with ALL the facets enumerating indexes content. */
  protected Map<CategoryPath, Integer> facetCountsTruth() throws IOException {
    FacetIndexingParams iParams = getFacetIndexingParams(Integer.MAX_VALUE);
    String delim = String.valueOf(iParams.getFacetDelimChar());
    Map<CategoryPath, Integer> res = new HashMap<CategoryPath, Integer>();
    HashSet<Term> handledTerms = new HashSet<Term>();
    for (CategoryListParams clp : iParams.getAllCategoryListParams()) {
      Term baseTerm = new Term(clp.getTerm().field());
      if (!handledTerms.add(baseTerm)) {
        continue; // already handled this term (for another list) 
      }
      Terms terms = MultiFields.getTerms(indexReader, baseTerm.field());
      if (terms == null) {
        continue;
      }
      Bits liveDocs = MultiFields.getLiveDocs(indexReader);
      TermsEnum te = terms.iterator();
      DocsEnum de = null;
      while (te.next() != null) {
        de = te.docs(liveDocs, de);
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
        System.out.println(freq.getCategoryPath().toString()+ "\t\t" + topResNode);
      }
      assertCountsAndCardinality(facetCountsTruth, topResNode, freq.getNumResults());
    }
  }
    
  /** Validate counts for returned facets, and that there are not too many results */
  private static void assertCountsAndCardinality(Map<CategoryPath,Integer> facetCountsTruth,  FacetResultNode resNode, int reqNumResults) throws Exception {
    int actualNumResults = resNode.getNumSubResults();
    if (VERBOSE) {
      System.out.println("NumResults: " + actualNumResults);
    }
    assertTrue("Too many results!", actualNumResults <= reqNumResults);
    for (FacetResultNode subRes : resNode.getSubResults()) {
      assertEquals("wrong count for: "+subRes, facetCountsTruth.get(subRes.getLabel()).intValue(), (int)subRes.getValue());
      assertCountsAndCardinality(facetCountsTruth, subRes, reqNumResults); // recurse into child results
    }
  }
  
  /** Validate results equality */
  protected static void assertSameResults(List<FacetResult> expected,
                                          List<FacetResult> actual) {
    String expectedResults = resStringValueOnly(expected);
    String actualResults = resStringValueOnly(actual);
    if (!expectedResults.equals(actualResults)) {
      System.err.println("Results are not the same!");
      System.err.println("Expected:\n" + expectedResults);
      System.err.println("Actual" + actualResults);
      throw new NotSameResultError();
    }
  }
  
  /** exclude the residue and numDecendants because it is incorrect in sampling */
  private static final String resStringValueOnly(List<FacetResult> results) {
    StringBuilder sb = new StringBuilder();
    for (FacetResult facetRes : results) {
      sb.append(facetRes.toString()).append('\n');
    }
    return sb.toString().replaceAll("Residue:.*.0", "").replaceAll("Num valid Descendants.*", "");
  }
  
  /** Special Error class for ability to ignore only this error and retry... */ 
  public static class NotSameResultError extends Error {
    public NotSameResultError() {
      super("Results are not the same!");
    }
  }
  
}
