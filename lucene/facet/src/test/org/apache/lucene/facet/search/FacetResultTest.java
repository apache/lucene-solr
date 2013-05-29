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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.FacetTestUtils;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class FacetResultTest extends FacetTestCase {
  
  private Document newDocument(FacetFields facetFields, String... categories) throws IOException {
    Document doc = new Document();
    List<CategoryPath> cats = new ArrayList<CategoryPath>();
    for (String cat : categories) {
      cats.add(new CategoryPath(cat, '/'));
    }
    facetFields.addFields(doc, cats);
    return doc;
  }
  
  private void initIndex(Directory indexDir, Directory taxoDir) throws IOException {
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter indexWriter = new IndexWriter(indexDir, conf);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetFields facetFields = new FacetFields(taxoWriter);
    indexWriter.addDocument(newDocument(facetFields, "Date/2010/March/12", "A/1"));
    indexWriter.addDocument(newDocument(facetFields, "Date/2010/March/23", "A/2"));
    indexWriter.addDocument(newDocument(facetFields, "Date/2010/April/17", "A/3"));
    indexWriter.addDocument(newDocument(facetFields, "Date/2010/May/18", "A/1"));
    indexWriter.addDocument(newDocument(facetFields, "Date/2011/January/1", "A/3"));
    indexWriter.addDocument(newDocument(facetFields, "Date/2011/February/12", "A/1"));
    indexWriter.addDocument(newDocument(facetFields, "Date/2011/February/18", "A/4"));
    indexWriter.addDocument(newDocument(facetFields, "Date/2012/August/15", "A/1"));
    indexWriter.addDocument(newDocument(facetFields, "Date/2012/July/5", "A/2"));
    indexWriter.addDocument(newDocument(facetFields, "Date/2013/September/13", "A/1"));
    indexWriter.addDocument(newDocument(facetFields, "Date/2013/September/25", "A/4"));
    IOUtils.close(indexWriter, taxoWriter);
  }
  
  private void searchIndex(TaxonomyReader taxoReader, IndexSearcher searcher, boolean fillMissingCounts, String[] exp,
      String[][] drillDowns, int[] numResults) throws IOException {
    CategoryPath[][] cps = new CategoryPath[drillDowns.length][];
    for (int i = 0; i < cps.length; i++) {
      cps[i] = new CategoryPath[drillDowns[i].length];
      for (int j = 0; j < cps[i].length; j++) {
        cps[i][j] = new CategoryPath(drillDowns[i][j], '/');
      }
    }
    DrillDownQuery ddq = new DrillDownQuery(FacetIndexingParams.DEFAULT, new MatchAllDocsQuery());
    for (CategoryPath[] cats : cps) {
      ddq.add(cats);
    }
    
    List<FacetRequest> facetRequests = new ArrayList<FacetRequest>();
    for (CategoryPath[] cats : cps) {
      for (int i = 0; i < cats.length; i++) {
        CategoryPath cp = cats[i];
        int numres = numResults == null ? 2 : numResults[i];
        // for each drill-down, add itself as well as its parent as requests, so
        // we get the drill-sideways
        facetRequests.add(new CountFacetRequest(cp, numres));
        CountFacetRequest parent = new CountFacetRequest(cp.subpath(cp.length - 1), numres);
        if (!facetRequests.contains(parent) && parent.categoryPath.length > 0) {
          facetRequests.add(parent);
        }
      }
    }
    
    FacetSearchParams fsp = new FacetSearchParams(facetRequests);
    final DrillSideways ds;
    final Map<String,FacetArrays> dimArrays;
    if (fillMissingCounts) {
      dimArrays = new HashMap<String,FacetArrays>();
      ds = new DrillSideways(searcher, taxoReader) {
        @Override
        protected FacetsAccumulator getDrillSidewaysAccumulator(String dim, FacetSearchParams fsp) throws IOException {
          FacetsAccumulator fa = super.getDrillSidewaysAccumulator(dim, fsp);
          dimArrays.put(dim, fa.facetArrays);
          return fa;
        }
      };
    } else {
      ds = new DrillSideways(searcher, taxoReader);
      dimArrays = null;
    }
    
    final DrillSidewaysResult sidewaysRes = ds.search(null, ddq, 5, fsp);
    List<FacetResult> facetResults = FacetResult.mergeHierarchies(sidewaysRes.facetResults, taxoReader, dimArrays);
    CollectionUtil.introSort(facetResults, new Comparator<FacetResult>() {
      @Override
      public int compare(FacetResult o1, FacetResult o2) {
        return o1.getFacetRequest().categoryPath.compareTo(o2.getFacetRequest().categoryPath);
      }
    });
    assertEquals(exp.length, facetResults.size()); // A + single one for date
    for (int i = 0; i < facetResults.size(); i++) {
      assertEquals(exp[i], FacetTestUtils.toSimpleString(facetResults.get(i)));
    }
  }
  
  @Test
  public void testMergeHierarchies() throws Exception {
    Directory indexDir = new RAMDirectory(), taxoDir = new RAMDirectory();
    initIndex(indexDir, taxoDir);
    
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    
    String[] exp = new String[] { "Date (0)\n  2010 (4)\n  2011 (3)\n" };
    searchIndex(taxoReader, searcher, false, exp, new String[][] { new String[] { "Date" } }, null);
    
    // two dimensions
    exp = new String[] { "A (0)\n  1 (5)\n  4 (2)\n", "Date (0)\n  2010 (4)\n  2011 (3)\n" };
    searchIndex(taxoReader, searcher, false, exp, new String[][] { new String[] { "Date" }, new String[] { "A" } }, null);
    
    // both parent and child are OR'd
    exp = new String[] { "Date (-1)\n  2010 (4)\n    March (2)\n      23 (1)\n      12 (1)\n    May (1)\n" };
    searchIndex(taxoReader, searcher, false, exp, new String[][] { new String[] { "Date/2010/March", "Date/2010/March/23" }}, null);
    
    // both parent and child are OR'd (fill counts)
    exp = new String[] { "Date (0)\n  2010 (4)\n    March (2)\n      23 (1)\n      12 (1)\n    May (1)\n" };
    searchIndex(taxoReader, searcher, true, exp, new String[][] { new String[] { "Date/2010/March", "Date/2010/March/23" }}, null);
    
    // same DD twice
    exp = new String[] { "Date (0)\n  2010 (4)\n    March (2)\n    May (1)\n  2011 (3)\n" };
    searchIndex(taxoReader, searcher, false, exp, new String[][] { new String[] { "Date/2010", "Date/2010" }}, null);
    
    exp = new String[] { "Date (0)\n  2010 (4)\n    March (2)\n    May (1)\n  2011 (3)\n" };
    searchIndex(taxoReader, searcher, false, exp, new String[][] { new String[] { "Date/2010" }}, null);
    
    exp = new String[] { "Date (0)\n  2010 (4)\n    March (2)\n    May (1)\n  2011 (3)\n    February (2)\n    January (1)\n" };
    searchIndex(taxoReader, searcher, false, exp, new String[][] { new String[] { "Date/2010", "Date/2011" }}, null);
    
    exp = new String[] { "Date (0)\n  2010 (4)\n    March (2)\n      23 (1)\n      12 (1)\n    May (1)\n  2011 (3)\n    February (2)\n    January (1)\n" };
    searchIndex(taxoReader, searcher, false, exp, new String[][] { new String[] { "Date/2010/March", "Date/2011" }}, null);
    
    // Date/2010/April not in top-2 of Date/2010
    exp = new String[] { "Date (0)\n  2010 (4)\n    March (2)\n      23 (1)\n      12 (1)\n    May (1)\n    April (1)\n      17 (1)\n  2011 (3)\n    February (2)\n    January (1)\n" };
    searchIndex(taxoReader, searcher, false, exp, new String[][] { new String[] { "Date/2010/March", "Date/2010/April", "Date/2011" }}, null);
    
    // missing ancestors
    exp = new String[] { "Date (-1)\n  2010 (4)\n    March (2)\n    May (1)\n    April (1)\n      17 (1)\n  2011 (-1)\n    January (1)\n      1 (1)\n" };
    searchIndex(taxoReader, searcher, false, exp, new String[][] { new String[] { "Date/2011/January/1", "Date/2010/April" }}, null);
    
    // missing ancestors (fill counts)
    exp = new String[] { "Date (0)\n  2010 (4)\n    March (2)\n    May (1)\n    April (1)\n      17 (1)\n  2011 (3)\n    January (1)\n      1 (1)\n" };
    searchIndex(taxoReader, searcher, true, exp, new String[][] { new String[] { "Date/2011/January/1", "Date/2010/April" }}, null);
    
    // non-hierarchical dimension with both parent and child
    exp = new String[] { "A (0)\n  1 (5)\n  4 (2)\n  3 (2)\n" };
    searchIndex(taxoReader, searcher, INFOSTREAM, exp, new String[][] { new String[] { "A", "A/3" }}, null);
    
    // non-hierarchical dimension with same request but different numResults
    exp = new String[] { "A (0)\n  1 (5)\n  4 (2)\n  3 (2)\n  2 (2)\n" };
    searchIndex(taxoReader, searcher, INFOSTREAM, exp, new String[][] { new String[] { "A", "A" }}, new int[] { 2, 4 });
    
    IOUtils.close(indexReader, taxoReader);
    
    IOUtils.close(indexDir, taxoDir);
  }
  
}
