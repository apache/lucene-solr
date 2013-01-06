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
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestDemoFacets extends LuceneTestCase {

  private DirectoryTaxonomyWriter taxoWriter;
  private RandomIndexWriter writer;
  private FacetFields docBuilder;

  private void add(String ... categoryPaths) throws IOException {
    Document doc = new Document();
    
    List<CategoryPath> paths = new ArrayList<CategoryPath>();
    for(String categoryPath : categoryPaths) {
      paths.add(new CategoryPath(categoryPath, '/'));
    }
    docBuilder.addFields(doc, paths);
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
    docBuilder = new FacetFields(taxoWriter);

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
    FacetsCollector c = new FacetsCollector(fsp, searcher.getIndexReader(), taxoReader);

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), c);

    // Retrieve & verify results:
    List<FacetResult> results = c.getFacetResults();
    assertEquals(2, results.size());
    assertEquals("Publish Date (5)\n  2012 (2)\n  2010 (2)\n  1999 (1)\n",
                 toSimpleString(results.get(0)));
    assertEquals("Author (5)\n  Lisa (2)\n  Frank (1)\n  Susan (1)\n  Bob (1)\n",
                 toSimpleString(results.get(1)));

    
    // Now user drills down on Publish Date/2010:
    fsp = new FacetSearchParams(new CountFacetRequest(new CategoryPath("Author"), 10));
    Query q2 = DrillDown.query(fsp, new MatchAllDocsQuery(), new CategoryPath("Publish Date/2010", '/'));
    c = new FacetsCollector(fsp, searcher.getIndexReader(), taxoReader);
    searcher.search(q2, c);
    results = c.getFacetResults();
    assertEquals(1, results.size());
    assertEquals("Author (2)\n  Lisa (1)\n  Bob (1)\n",
                 toSimpleString(results.get(0)));

    taxoReader.close();
    searcher.getIndexReader().close();
    dir.close();
    taxoDir.close();
  }

  private String toSimpleString(FacetResult fr) {
    StringBuilder sb = new StringBuilder();
    toSimpleString(0, sb, fr.getFacetResultNode(), "");
    return sb.toString();
  }

  private void toSimpleString(int depth, StringBuilder sb, FacetResultNode node, String indent) {
    sb.append(indent + node.getLabel().components[depth] + " (" + (int) node.getValue() + ")\n");
    for(FacetResultNode childNode : node.getSubResults()) {
      toSimpleString(depth+1, sb, childNode, indent + "  ");
    }
  }
}
