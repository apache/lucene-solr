package org.apache.lucene.facet.search.params;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.index.CategoryDocumentBuilder;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.DefaultFacetIndexingParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.CategoryListIterator;
import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.FacetResultsHandler;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.ScoredDocIDs;
import org.apache.lucene.facet.search.StandardFacetsAccumulator;
import org.apache.lucene.facet.search.TopKFacetResultsHandler;
import org.apache.lucene.facet.search.cache.CategoryListCache;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.search.results.IntermediateFacetResult;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.util.ScoredDocIdsUtils;

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

/**
 * Test faceted search with creation of multiple category list iterators by the
 * same CLP, depending on the provided facet request
 */
public class MultiIteratorsPerCLParamsTest extends LuceneTestCase {

  CategoryPath[][] perDocCategories = new CategoryPath[][] {
      { new CategoryPath("author", "Mark Twain"),
          new CategoryPath("date", "2010") },
      { new CategoryPath("author", "Robert Frost"),
          new CategoryPath("date", "2009") },
      { new CategoryPath("author", "Artur Miller"),
          new CategoryPath("date", "2010") },
      { new CategoryPath("author", "Edgar Allan Poe"),
          new CategoryPath("date", "2009") },
      { new CategoryPath("author", "Henry James"),
          new CategoryPath("date", "2010") } };
  
  String countForbiddenDimension;

  @Test
  public void testCLParamMultiIteratorsByRequest() throws Exception {
    doTestCLParamMultiIteratorsByRequest(false);
  }

  @Test
  public void testCLParamMultiIteratorsByRequestCacheCLI() throws Exception {
    doTestCLParamMultiIteratorsByRequest(true);
  }

  private void doTestCLParamMultiIteratorsByRequest(boolean cacheCLI) throws Exception {
    // Create a CLP which generates different CLIs according to the
    // FacetRequest's dimension
    CategoryListParams clp = new CategoryListParams();
    FacetIndexingParams iParams = new DefaultFacetIndexingParams(clp);
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    populateIndex(iParams, indexDir, taxoDir);

    TaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);
    IndexReader reader = DirectoryReader.open(indexDir);

    CategoryListCache clCache = null;
    if (cacheCLI) {
      // caching the iteratorr, so:
      // 1: create the cached iterator, using original params
      clCache = new CategoryListCache();
      clCache.loadAndRegister(clp, reader, taxo, iParams);
    }
    
    ScoredDocIDs allDocs = ScoredDocIdsUtils
        .createAllDocsScoredDocIDs(reader);

    // Search index with 'author' should filter ONLY ordinals whose parent
    // is 'author'
    countForbiddenDimension = "date";
    validateFacetedSearch(iParams, taxo, reader, clCache, allDocs, "author", 5, 5);

    // Search index with 'date' should filter ONLY ordinals whose parent is
    // 'date'
    countForbiddenDimension = "author";
    validateFacetedSearch(iParams, taxo, reader, clCache, allDocs, "date", 5, 2);

    // Search index with both 'date' and 'author'
    countForbiddenDimension = null;
    validateFacetedSearch(iParams, taxo, reader, clCache, allDocs, new String[] {
            "author", "date" }, new int[] { 5, 5 }, new int[] { 5, 2 });
    taxo.close();
    reader.close();
    indexDir.close();
    taxoDir.close();
  }

  private void validateFacetedSearch(FacetIndexingParams iParams,
      TaxonomyReader taxo, IndexReader reader, CategoryListCache clCache,
      ScoredDocIDs allDocs, String dimension, int expectedValue, int expectedNumDescendants) throws IOException {
    validateFacetedSearch(iParams, taxo, reader, clCache, allDocs,
        new String[] { dimension }, new int[] { expectedValue },
        new int[] { expectedNumDescendants });
  }

  private void validateFacetedSearch(FacetIndexingParams iParams,
      TaxonomyReader taxo, IndexReader reader,  CategoryListCache clCache, ScoredDocIDs allDocs,
      String[] dimension, int[] expectedValue,
      int[] expectedNumDescendants)
      throws IOException {
    FacetSearchParams sParams = new FacetSearchParams(iParams);
    sParams.setClCache(clCache);
    for (String dim : dimension) {
      sParams.addFacetRequest(new PerDimCountFacetRequest(
          new CategoryPath(dim), 10));
    }
    FacetsAccumulator acc = new StandardFacetsAccumulator(sParams, reader, taxo);
    
    // no use to test this with complement since at that mode all facets are taken
    acc.setComplementThreshold(FacetsAccumulator.DISABLE_COMPLEMENT);

    List<FacetResult> results = acc.accumulate(allDocs);
    assertEquals("Wrong #results", dimension.length, results.size());

    for (int i = 0; i < results.size(); i++) {
      FacetResult res = results.get(i);
      assertEquals("wrong num-descendants for dimension " + dimension[i],
          expectedNumDescendants[i], res.getNumValidDescendants());
      FacetResultNode resNode = res.getFacetResultNode();
      assertEquals("wrong value for dimension " + dimension[i],
          expectedValue[i], (int) resNode.getValue());
    }
  }

  private void populateIndex(FacetIndexingParams iParams, Directory indexDir,
      Directory taxoDir) throws Exception {
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexDir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.KEYWORD, false)));
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

    for (CategoryPath[] categories : perDocCategories) {
      writer.addDocument(new CategoryDocumentBuilder(taxoWriter, iParams)
          .setCategoryPaths(Arrays.asList(categories)).build(
              new Document()));

    }
    taxoWriter.commit();
    writer.commit();
    taxoWriter.close();
    writer.close();
  }

  private class PerDimCountFacetRequest extends CountFacetRequest {
    
    public PerDimCountFacetRequest(CategoryPath path, int num) {
      super(path, num);
    }

    @Override
    public CategoryListIterator createCategoryListIterator(IndexReader reader, 
        TaxonomyReader taxo, FacetSearchParams sParams, int partition) throws IOException {
      // categories of certain dimension only
      return new PerDimensionCLI(taxo, super.createCategoryListIterator(
          reader, taxo, sParams, partition), getCategoryPath());
    }
    
    @Override
    /** Override this method just for verifying that only specified facets are iterated.. */
    public FacetResultsHandler createFacetResultsHandler(
        TaxonomyReader taxonomyReader) {
      return new TopKFacetResultsHandler(taxonomyReader, this) {
        @Override
        public IntermediateFacetResult fetchPartitionResult(
            FacetArrays facetArrays, int offset) throws IOException {
          final IntermediateFacetResult res = super.fetchPartitionResult(facetArrays, offset);
          if (countForbiddenDimension!=null) {
            int ord = taxonomyReader.getOrdinal(new CategoryPath(countForbiddenDimension));
            assertEquals("Should not have accumulated for dimension '"+countForbiddenDimension+"'!",0,facetArrays.getIntArray()[ord]);
          }
          return res;
        }
      };
    }
  }

  /**
   * a CLI which filters another CLI for the dimension of the provided
   * category-path
   */
  private static class PerDimensionCLI implements CategoryListIterator {
    private final CategoryListIterator superCLI;
    private final int[] parentArray;
    private final int parentOrdinal;

    PerDimensionCLI(TaxonomyReader taxo, CategoryListIterator superCLI,
        CategoryPath requestedPath) throws IOException {
      this.superCLI = superCLI;
      if (requestedPath == null) {
        parentOrdinal = 0;
      } else {
        CategoryPath cp = new CategoryPath(requestedPath.getComponent(0));
        parentOrdinal = taxo.getOrdinal(cp);
      }
      parentArray = taxo.getParentArray();
    }

    public boolean init() throws IOException {
      return superCLI.init();
    }

    public long nextCategory() throws IOException {
      long next;
      while ((next = superCLI.nextCategory()) <= Integer.MAX_VALUE
          && !isInDimension((int) next)) {
      }

      return next;
    }

    /** look for original parent ordinal, meaning same dimension */
    private boolean isInDimension(int ordinal) {
      while (ordinal > 0) {
        if (ordinal == parentOrdinal) {
          return true;
        }
        ordinal = parentArray[ordinal];
      }
      return false;
    }

    public boolean skipTo(int docId) throws IOException {
      return superCLI.skipTo(docId);
    }
  }
}