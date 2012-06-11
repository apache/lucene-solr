package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.facet.FacetTestUtils;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.index.params.PerDimensionIndexingParams;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
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

public class TestMultipleCategoryLists extends LuceneTestCase {

  @Test
  public void testDefault() throws Exception {
    Directory[][] dirs = getDirs();
    // create and open an index writer
    RandomIndexWriter iw = new RandomIndexWriter(random(), dirs[0][0], newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    // create and open a taxonomy writer
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(dirs[0][1], OpenMode.CREATE);

    /**
     * Configure with no custom counting lists
     */
    PerDimensionIndexingParams iParams = new PerDimensionIndexingParams();

    seedIndex(iw, tw, iParams);

    IndexReader ir = iw.getReader();
    tw.commit();

    // prepare index reader and taxonomy.
    TaxonomyReader tr = new DirectoryTaxonomyReader(dirs[0][1]);

    // prepare searcher to search against
    IndexSearcher searcher = newSearcher(ir);

    FacetsCollector facetsCollector = performSearch(iParams, tr, ir,
        searcher);

    // Obtain facets results and hand-test them
    assertCorrectResults(facetsCollector);

    DocsEnum td = _TestUtil.docs(random(), ir, "$facets", new BytesRef("$fulltree$"), MultiFields.getLiveDocs(ir), null, false);
    assertTrue(td.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);

    tr.close();
    ir.close();
    iw.close();
    tw.close();
    IOUtils.close(dirs[0]);
  }

  @Test
  public void testCustom() throws Exception {
    Directory[][] dirs = getDirs();
    // create and open an index writer
    RandomIndexWriter iw = new RandomIndexWriter(random(), dirs[0][0], newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    // create and open a taxonomy writer
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(dirs[0][1],
        OpenMode.CREATE);

    PerDimensionIndexingParams iParams = new PerDimensionIndexingParams();
    iParams.addCategoryListParams(new CategoryPath("Author"),
        new CategoryListParams(new Term("$author", "Authors")));
    seedIndex(iw, tw, iParams);

    IndexReader ir = iw.getReader();
    tw.commit();

    // prepare index reader and taxonomy.
    TaxonomyReader tr = new DirectoryTaxonomyReader(dirs[0][1]);

    // prepare searcher to search against
    IndexSearcher searcher = newSearcher(ir);

    FacetsCollector facetsCollector = performSearch(iParams, tr, ir,
        searcher);

    // Obtain facets results and hand-test them
    assertCorrectResults(facetsCollector);

    assertPostingListExists("$facets", "$fulltree$", ir);
    assertPostingListExists("$author", "Authors", ir);

    tr.close();
    ir.close();
    iw.close();
    tw.close();
    IOUtils.close(dirs[0]);
  }

  @Test
  public void testTwoCustomsSameField() throws Exception {
    Directory[][] dirs = getDirs();
    // create and open an index writer
    RandomIndexWriter iw = new RandomIndexWriter(random(), dirs[0][0], newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    // create and open a taxonomy writer
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(dirs[0][1],
        OpenMode.CREATE);

    PerDimensionIndexingParams iParams = new PerDimensionIndexingParams();
    iParams.addCategoryListParams(new CategoryPath("Band"),
        new CategoryListParams(new Term("$music", "Bands")));
    iParams.addCategoryListParams(new CategoryPath("Composer"),
        new CategoryListParams(new Term("$music", "Composers")));
    seedIndex(iw, tw, iParams);

    IndexReader ir = iw.getReader();
    tw.commit();

    // prepare index reader and taxonomy.
    TaxonomyReader tr = new DirectoryTaxonomyReader(dirs[0][1]);

    // prepare searcher to search against
    IndexSearcher searcher = newSearcher(ir);

    FacetsCollector facetsCollector = performSearch(iParams, tr, ir,
        searcher);

    // Obtain facets results and hand-test them
    assertCorrectResults(facetsCollector);

    assertPostingListExists("$facets", "$fulltree$", ir);
    assertPostingListExists("$music", "Bands", ir);
    assertPostingListExists("$music", "Composers", ir);

    tr.close();
    ir.close();
    iw.close();
    tw.close();
    IOUtils.close(dirs[0]);
  }

  private void assertPostingListExists(String field, String text, IndexReader ir) throws IOException {
    DocsEnum de = _TestUtil.docs(random(), ir, field, new BytesRef(text), null, null, false);
    assertTrue(de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
  }

  @Test
  public void testDifferentFieldsAndText() throws Exception {
    Directory[][] dirs = getDirs();
    // create and open an index writer
    RandomIndexWriter iw = new RandomIndexWriter(random(), dirs[0][0], newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    // create and open a taxonomy writer
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(dirs[0][1], OpenMode.CREATE);

    PerDimensionIndexingParams iParams = new PerDimensionIndexingParams();
    iParams.addCategoryListParams(new CategoryPath("Band"),
        new CategoryListParams(new Term("$bands", "Bands")));
    iParams.addCategoryListParams(new CategoryPath("Composer"),
        new CategoryListParams(new Term("$composers", "Composers")));
    seedIndex(iw, tw, iParams);

    IndexReader ir = iw.getReader();
    tw.commit();

    // prepare index reader and taxonomy.
    TaxonomyReader tr = new DirectoryTaxonomyReader(dirs[0][1]);

    // prepare searcher to search against
    IndexSearcher searcher = newSearcher(ir);

    FacetsCollector facetsCollector = performSearch(iParams, tr, ir,
        searcher);

    // Obtain facets results and hand-test them
    assertCorrectResults(facetsCollector);
    assertPostingListExists("$facets", "$fulltree$", ir);
    assertPostingListExists("$bands", "Bands", ir);
    assertPostingListExists("$composers", "Composers", ir);
    tr.close();
    ir.close();
    iw.close();
    tw.close();
    IOUtils.close(dirs[0]);
  }

  @Test
  public void testSomeSameSomeDifferent() throws Exception {
    Directory[][] dirs = getDirs();
    // create and open an index writer
    RandomIndexWriter iw = new RandomIndexWriter(random(), dirs[0][0], newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    // create and open a taxonomy writer
    TaxonomyWriter tw = new DirectoryTaxonomyWriter(dirs[0][1],
        OpenMode.CREATE);

    PerDimensionIndexingParams iParams = new PerDimensionIndexingParams();
    iParams.addCategoryListParams(new CategoryPath("Band"),
        new CategoryListParams(new Term("$music", "music")));
    iParams.addCategoryListParams(new CategoryPath("Composer"),
        new CategoryListParams(new Term("$music", "music")));
    iParams.addCategoryListParams(new CategoryPath("Author"),
        new CategoryListParams(new Term("$literature", "Authors")));

    seedIndex(iw, tw, iParams);

    IndexReader ir = iw.getReader();
    tw.commit();

    // prepare index reader and taxonomy.
    TaxonomyReader tr = new DirectoryTaxonomyReader(dirs[0][1]);

    // prepare searcher to search against
    IndexSearcher searcher = newSearcher(ir);

    FacetsCollector facetsCollector = performSearch(iParams, tr, ir,
        searcher);

    // Obtain facets results and hand-test them
    assertCorrectResults(facetsCollector);
    assertPostingListExists("$music", "music", ir);
    assertPostingListExists("$literature", "Authors", ir);

    tr.close();
    ir.close();
    iw.close();
    tw.close();
    IOUtils.close(dirs[0]);
  }

  private Directory[][] getDirs() throws IOException {
    return FacetTestUtils.createIndexTaxonomyDirs(1);
  }

  private void assertCorrectResults(FacetsCollector facetsCollector)
  throws IOException, IllegalAccessException, InstantiationException {
    List<FacetResult> res = facetsCollector.getFacetResults();

    FacetResult results = res.get(0);
    FacetResultNode resNode = results.getFacetResultNode();
    Iterable<? extends FacetResultNode> subResults = resNode
    .getSubResults();
    Iterator<? extends FacetResultNode> subIter = subResults.iterator();

    checkResult(resNode, "Band", 5.0);
    checkResult(subIter.next(), "Band/Rock & Pop", 4.0);
    checkResult(subIter.next(), "Band/Punk", 1.0);

    results = res.get(1);
    resNode = results.getFacetResultNode();
    subResults = resNode.getSubResults();
    subIter = subResults.iterator();

    checkResult(resNode, "Band", 5.0);
    checkResult(subIter.next(), "Band/Rock & Pop", 4.0);
    checkResult(subIter.next(), "Band/Rock & Pop/Dave Matthews Band", 1.0);
    checkResult(subIter.next(), "Band/Rock & Pop/REM", 1.0);
    checkResult(subIter.next(), "Band/Rock & Pop/U2", 1.0);
    checkResult(subIter.next(), "Band/Punk/The Ramones", 1.0);
    checkResult(subIter.next(), "Band/Punk", 1.0);
    checkResult(subIter.next(), "Band/Rock & Pop/The Beatles", 1.0);

    results = res.get(2);
    resNode = results.getFacetResultNode();
    subResults = resNode.getSubResults();
    subIter = subResults.iterator();

    checkResult(resNode, "Author", 3.0);
    checkResult(subIter.next(), "Author/Kurt Vonnegut", 1.0);
    checkResult(subIter.next(), "Author/Stephen King", 1.0);
    checkResult(subIter.next(), "Author/Mark Twain", 1.0);

    results = res.get(3);
    resNode = results.getFacetResultNode();
    subResults = resNode.getSubResults();
    subIter = subResults.iterator();

    checkResult(resNode, "Band/Rock & Pop", 4.0);
    checkResult(subIter.next(), "Band/Rock & Pop/Dave Matthews Band", 1.0);
    checkResult(subIter.next(), "Band/Rock & Pop/REM", 1.0);
    checkResult(subIter.next(), "Band/Rock & Pop/U2", 1.0);
    checkResult(subIter.next(), "Band/Rock & Pop/The Beatles", 1.0);
  }

  private FacetsCollector performSearch(FacetIndexingParams iParams,
                                        TaxonomyReader tr, IndexReader ir,
                                        IndexSearcher searcher) throws IOException {
    // step 1: collect matching documents into a collector
    Query q = new MatchAllDocsQuery();
    TopScoreDocCollector topDocsCollector = TopScoreDocCollector.create(10,
        true);

    // Faceted search parameters indicate which facets are we interested in
    FacetSearchParams facetSearchParams = new FacetSearchParams(iParams);

    facetSearchParams.addFacetRequest(new CountFacetRequest(
        new CategoryPath("Band"), 10));
    CountFacetRequest bandDepth = new CountFacetRequest(new CategoryPath(
    "Band"), 10);
    bandDepth.setDepth(2);
    facetSearchParams.addFacetRequest(bandDepth);
    facetSearchParams.addFacetRequest(new CountFacetRequest(
        new CategoryPath("Author"), 10));
    facetSearchParams.addFacetRequest(new CountFacetRequest(
        new CategoryPath("Band", "Rock & Pop"), 10));

    // perform documents search and facets accumulation
    FacetsCollector facetsCollector = new FacetsCollector(facetSearchParams, ir, tr);
    searcher.search(q, MultiCollector.wrap(topDocsCollector, facetsCollector));
    return facetsCollector;
  }

  private void seedIndex(RandomIndexWriter iw, TaxonomyWriter tw,
                          FacetIndexingParams iParams) throws IOException, CorruptIndexException {
    FacetTestUtils.add(iParams, iw, tw, "Author", "Mark Twain");
    FacetTestUtils.add(iParams, iw, tw, "Author", "Stephen King");
    FacetTestUtils.add(iParams, iw, tw, "Author", "Kurt Vonnegut");
    FacetTestUtils.add(iParams, iw, tw, "Band", "Rock & Pop",
    "The Beatles");
    FacetTestUtils.add(iParams, iw, tw, "Band", "Punk", "The Ramones");
    FacetTestUtils.add(iParams, iw, tw, "Band", "Rock & Pop", "U2");
    FacetTestUtils.add(iParams, iw, tw, "Band", "Rock & Pop", "REM");
    FacetTestUtils.add(iParams, iw, tw, "Band", "Rock & Pop",
    "Dave Matthews Band");
    FacetTestUtils.add(iParams, iw, tw, "Composer", "Bach");
  }

  private static void checkResult(FacetResultNode sub, String label, double value) {
    assertEquals("Label of subresult " + sub.getLabel() + " was incorrect",
        label, sub.getLabel().toString());
    assertEquals(
        "Value for " + sub.getLabel() + " subresult was incorrect",
        value, sub.getValue(), 0.0);
  }

}