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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.FacetTestUtils;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.util.PrintTaxonomyStats;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

public class TestDemoFacets extends FacetTestCase {

  private DirectoryTaxonomyWriter taxoWriter;
  private RandomIndexWriter writer;
  private FacetFields facetFields;

  private void add(String ... categoryPaths) throws IOException {
    Document doc = new Document();
    
    List<CategoryPath> paths = new ArrayList<CategoryPath>();
    for(String categoryPath : categoryPaths) {
      paths.add(new CategoryPath(categoryPath, '/'));
    }
    facetFields.addFields(doc, paths);
    writer.addDocument(doc);
  }

  public void test() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    writer = new RandomIndexWriter(random(), dir);

    // Writes facet ords to a separate directory from the
    // main index:
    taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    // Reused across documents, to add the necessary facet
    // fields:
    facetFields = new FacetFields(taxoWriter);

    add("Author/Bob", "Publish Date/2010/10/15");
    add("Author/Lisa", "Publish Date/2010/10/20");
    add("Author/Lisa", "Publish Date/2012/1/1");
    add("Author/Susan", "Publish Date/2012/1/7");
    add("Author/Frank", "Publish Date/1999/5/5");

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    taxoWriter.close();

    // Count both "Publish Date" and "Author" dimensions:
    FacetSearchParams fsp = new FacetSearchParams(
        new CountFacetRequest(new CategoryPath("Publish Date"), 10), 
        new CountFacetRequest(new CategoryPath("Author"), 10));

    // Aggregatses the facet counts:
    FacetsCollector c = FacetsCollector.create(fsp, searcher.getIndexReader(), taxoReader);

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), c);

    // Retrieve & verify results:
    List<FacetResult> results = c.getFacetResults();
    assertEquals(2, results.size());
    assertEquals("Publish Date (0)\n  2012 (2)\n  2010 (2)\n  1999 (1)\n",
        FacetTestUtils.toSimpleString(results.get(0)));
    assertEquals("Author (0)\n  Lisa (2)\n  Frank (1)\n  Susan (1)\n  Bob (1)\n",
        FacetTestUtils.toSimpleString(results.get(1)));

    
    // Now user drills down on Publish Date/2010:
    fsp = new FacetSearchParams(new CountFacetRequest(new CategoryPath("Author"), 10));
    DrillDownQuery q2 = new DrillDownQuery(fsp.indexingParams, new MatchAllDocsQuery());
    q2.add(new CategoryPath("Publish Date/2010", '/'));
    c = FacetsCollector.create(fsp, searcher.getIndexReader(), taxoReader);
    searcher.search(q2, c);
    results = c.getFacetResults();
    assertEquals(1, results.size());
    assertEquals("Author (0)\n  Lisa (1)\n  Bob (1)\n",
        FacetTestUtils.toSimpleString(results.get(0)));

    // Smoke test PrintTaxonomyStats:
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintTaxonomyStats.printStats(taxoReader, new PrintStream(bos, false, "UTF-8"), true);
    String result = bos.toString("UTF-8");
    assertTrue(result.indexOf("/Author: 4 immediate children; 5 total categories") != -1);
    assertTrue(result.indexOf("/Publish Date: 3 immediate children; 12 total categories") != -1);
    // Make sure at least a few nodes of the tree came out:
    assertTrue(result.indexOf("  /1999") != -1);
    assertTrue(result.indexOf("  /2012") != -1);
    assertTrue(result.indexOf("      /20") != -1);

    taxoReader.close();
    searcher.getIndexReader().close();
    dir.close();
    taxoDir.close();
  }

  public void testReallyNoNormsForDrillDown() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setSimilarity(new PerFieldSimilarityWrapper() {
        final Similarity sim = new DefaultSimilarity();

        @Override
        public Similarity get(String name) {
          assertEquals("field", name);
          return sim;
        }
      });
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    FacetFields facetFields = new FacetFields(taxoWriter);      

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    facetFields.addFields(doc, Collections.singletonList(new CategoryPath("a/path", '/')));
    writer.addDocument(doc);
    writer.close();
    taxoWriter.close();
    dir.close();
    taxoDir.close();
  }

  public void testAllParents() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    CategoryListParams clp = new CategoryListParams("$facets") {
        @Override
        public OrdinalPolicy getOrdinalPolicy(String fieldName) {
          return OrdinalPolicy.ALL_PARENTS;
        }
      };
    FacetIndexingParams fip = new FacetIndexingParams(clp);

    FacetFields facetFields = new FacetFields(taxoWriter, fip);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    facetFields.addFields(doc, Collections.singletonList(new CategoryPath("a/path", '/')));
    writer.addDocument(doc);

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    taxoWriter.close();
    
    FacetSearchParams fsp = new FacetSearchParams(fip,
                                                  new CountFacetRequest(new CategoryPath("a", '/'), 10));

    // Aggregate the facet counts:
    FacetsCollector c = FacetsCollector.create(fsp, searcher.getIndexReader(), taxoReader);

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), c);
    List<FacetResult> results = c.getFacetResults();
    assertEquals(1, results.size());
    assertEquals(1, (int) results.get(0).getFacetResultNode().value);

    // LUCENE-4913:
    for(FacetResultNode childNode : results.get(0).getFacetResultNode().subResults) {
      assertTrue(childNode.ordinal != 0);
    }

    searcher.getIndexReader().close();
    taxoReader.close();
    dir.close();
    taxoDir.close();
  }

  public void testLabelWithDelimiter() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetFields facetFields = new FacetFields(taxoWriter);

    Document doc = new Document();
    doc.add(newTextField("field", "text", Field.Store.NO));
    BytesRef br = new BytesRef(new byte[] {(byte) 0xee, (byte) 0x92, (byte) 0xaa, (byte) 0xef, (byte) 0x9d, (byte) 0x89});
    facetFields.addFields(doc, Collections.singletonList(new CategoryPath("dim/" + br.utf8ToString(), '/')));
    try {
      writer.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
    }
    writer.close();
    taxoWriter.close();
    dir.close();
    taxoDir.close();
  }
}
