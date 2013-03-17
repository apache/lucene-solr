package org.apache.lucene.demo.facet;

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

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.DrillDownQuery;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesAccumulator;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetFields;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/** Shows simple usage of faceted indexing and search,
 *  using {@link SortedSetDocValuesFacetFields} and {@link
 *  SortedSetDocValuesAccumulator}.  */

public class SimpleSortedSetFacetsExample {

  private final Directory indexDir = new RAMDirectory();

  /** Empty constructor */
  public SimpleSortedSetFacetsExample() {}
  
  private void add(IndexWriter indexWriter, SortedSetDocValuesFacetFields facetFields, String ... categoryPaths) throws IOException {
    Document doc = new Document();
    
    List<CategoryPath> paths = new ArrayList<CategoryPath>();
    for (String categoryPath : categoryPaths) {
      paths.add(new CategoryPath(categoryPath, '/'));
    }
    facetFields.addFields(doc, paths);
    indexWriter.addDocument(doc);
  }

  /** Build the example index. */
  private void index() throws IOException {
    IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, 
        new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));

    // Reused across documents, to add the necessary facet fields
    SortedSetDocValuesFacetFields facetFields = new SortedSetDocValuesFacetFields();

    add(indexWriter, facetFields, "Author/Bob", "Publish Year/2010");
    add(indexWriter, facetFields, "Author/Lisa", "Publish Year/2010");
    add(indexWriter, facetFields, "Author/Lisa", "Publish Year/2012");
    add(indexWriter, facetFields, "Author/Susan", "Publish Year/2012");
    add(indexWriter, facetFields, "Author/Frank", "Publish Year/1999");
    
    indexWriter.close();
  }

  /** User runs a query and counts facets. */
  private List<FacetResult> search() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(indexReader);

    // Count both "Publish Year" and "Author" dimensions
    FacetSearchParams fsp = new FacetSearchParams(
        new CountFacetRequest(new CategoryPath("Publish Year"), 10), 
        new CountFacetRequest(new CategoryPath("Author"), 10));

    // Aggregatses the facet counts
    FacetsCollector fc = FacetsCollector.create(new SortedSetDocValuesAccumulator(fsp, state));

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), fc);

    // Retrieve results
    List<FacetResult> facetResults = fc.getFacetResults();
    
    indexReader.close();
    
    return facetResults;
  }
  
  /** User drills down on 'Publish Year/2010'. */
  private List<FacetResult> drillDown() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(indexReader);

    // Now user drills down on Publish Year/2010:
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(new CategoryPath("Author"), 10));
    DrillDownQuery q = new DrillDownQuery(fsp.indexingParams, new MatchAllDocsQuery());
    q.add(new CategoryPath("Publish Year/2010", '/'));
    FacetsCollector fc = FacetsCollector.create(new SortedSetDocValuesAccumulator(fsp, state));
    searcher.search(q, fc);

    // Retrieve results
    List<FacetResult> facetResults = fc.getFacetResults();
    
    indexReader.close();
    
    return facetResults;
  }

  /** Runs the search example. */
  public List<FacetResult> runSearch() throws IOException {
    index();
    return search();
  }
  
  /** Runs the drill-down example. */
  public List<FacetResult> runDrillDown() throws IOException {
    index();
    return drillDown();
  }

  /** Runs the search and drill-down examples and prints the results. */
  public static void main(String[] args) throws Exception {
    System.out.println("Facet counting example:");
    System.out.println("-----------------------");
    List<FacetResult> results = new SimpleSortedSetFacetsExample().runSearch();
    for (FacetResult res : results) {
      System.out.println(res);
    }

    System.out.println("\n");
    System.out.println("Facet drill-down example (Publish Year/2010):");
    System.out.println("---------------------------------------------");
    results = new SimpleSortedSetFacetsExample().runDrillDown();
    for (FacetResult res : results) {
      System.out.println(res);
    }
  }
  
}
