package org.apache.lucene.demo.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.DrillDownQuery;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsCollector;
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

/** Shows simple usage of faceted indexing and search. */
public class SimpleFacetsExample {

  private final Directory indexDir = new RAMDirectory();
  private final Directory taxoDir = new RAMDirectory();

  /** Empty constructor */
  public SimpleFacetsExample() {}
  
  private void add(IndexWriter indexWriter, FacetFields facetFields, String ... categoryPaths) throws IOException {
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

    // Writes facet ords to a separate directory from the main index
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

    // Reused across documents, to add the necessary facet fields
    FacetFields facetFields = new FacetFields(taxoWriter);

    add(indexWriter, facetFields, "Author/Bob", "Publish Date/2010/10/15");
    add(indexWriter, facetFields, "Author/Lisa", "Publish Date/2010/10/20");
    add(indexWriter, facetFields, "Author/Lisa", "Publish Date/2012/1/1");
    add(indexWriter, facetFields, "Author/Susan", "Publish Date/2012/1/7");
    add(indexWriter, facetFields, "Author/Frank", "Publish Date/1999/5/5");
    
    indexWriter.close();
    taxoWriter.close();
  }

  /** User runs a query and counts facets. */
  private List<FacetResult> search() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    // Count both "Publish Date" and "Author" dimensions
    FacetSearchParams fsp = new FacetSearchParams(
        new CountFacetRequest(new CategoryPath("Publish Date"), 10), 
        new CountFacetRequest(new CategoryPath("Author"), 10));

    // Aggregatses the facet counts
    FacetsCollector fc = FacetsCollector.create(fsp, searcher.getIndexReader(), taxoReader);

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), fc);

    // Retrieve results
    List<FacetResult> facetResults = fc.getFacetResults();
    
    indexReader.close();
    taxoReader.close();
    
    return facetResults;
  }
  
  /** User drills down on 'Publish Date/2010'. */
  private List<FacetResult> drillDown() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    // Now user drills down on Publish Date/2010:
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(new CategoryPath("Author"), 10));
    DrillDownQuery q = new DrillDownQuery(fsp.indexingParams, new MatchAllDocsQuery());
    q.add(new CategoryPath("Publish Date/2010", '/'));
    FacetsCollector fc = FacetsCollector.create(fsp, searcher.getIndexReader(), taxoReader);
    searcher.search(q, fc);

    // Retrieve results
    List<FacetResult> facetResults = fc.getFacetResults();
    
    indexReader.close();
    taxoReader.close();
    
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
    List<FacetResult> results = new SimpleFacetsExample().runSearch();
    for (FacetResult res : results) {
      System.out.println(res);
    }

    System.out.println("\n");
    System.out.println("Facet drill-down example (Publish Date/2010):");
    System.out.println("---------------------------------------------");
    results = new SimpleFacetsExample().runDrillDown();
    for (FacetResult res : results) {
      System.out.println(res);
    }
  }
  
}
