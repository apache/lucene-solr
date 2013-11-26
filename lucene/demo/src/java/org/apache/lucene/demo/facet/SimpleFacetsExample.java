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
import org.apache.lucene.facet.simple.FacetField;
import org.apache.lucene.facet.simple.Facets;
import org.apache.lucene.facet.simple.FacetsConfig;
import org.apache.lucene.facet.simple.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.simple.SimpleDrillDownQuery;
import org.apache.lucene.facet.simple.SimpleFacetResult;
import org.apache.lucene.facet.simple.SimpleFacetsCollector;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/** Shows simple usage of faceted indexing and search. */
public class SimpleFacetsExample {

  private final Directory indexDir = new RAMDirectory();
  private final Directory taxoDir = new RAMDirectory();

  /** Empty constructor */
  public SimpleFacetsExample() {}
  
  /** It's fine if taxoWriter is null (i.e., at search time) */
  private FacetsConfig getConfig(TaxonomyWriter taxoWriter) {
    FacetsConfig config = new FacetsConfig(taxoWriter);
    config.setHierarchical("Publish Date", true);
    return config;
  }

  /** Build the example index. */
  private void index() throws IOException {
    IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, 
        new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));

    // Writes facet ords to a separate directory from the main index
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

    FacetsConfig config = getConfig(taxoWriter);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    indexWriter.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    indexWriter.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    indexWriter.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    indexWriter.addDocument(config.build(doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    indexWriter.addDocument(config.build(doc));
    
    indexWriter.close();
    taxoWriter.close();
  }

  /** User runs a query and counts facets. */
  private List<SimpleFacetResult> search() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    FacetsConfig config = getConfig(null);

    SimpleFacetsCollector sfc = new SimpleFacetsCollector();

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), sfc);

    // Retrieve results
    List<SimpleFacetResult> results = new ArrayList<SimpleFacetResult>();

    // Count both "Publish Date" and "Author" dimensions
    Facets facets = new FastTaxonomyFacetCounts(taxoReader, config, sfc);
    results.add(facets.getTopChildren(10, "Author"));
    results.add(facets.getTopChildren(10, "Publish Date"));
    
    indexReader.close();
    taxoReader.close();
    
    return results;
  }
  
  /** User drills down on 'Publish Date/2010'. */
  private SimpleFacetResult drillDown() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    FacetsConfig config = getConfig(null);

    // Passing no baseQuery means we drill down on all
    // documents ("browse only"):
    SimpleDrillDownQuery q = new SimpleDrillDownQuery(config);

    // Now user drills down on Publish Date/2010:
    q.add("Publish Date", "2010");
    SimpleFacetsCollector sfc = new SimpleFacetsCollector();
    searcher.search(q, sfc);

    // Retrieve results
    Facets facets = new FastTaxonomyFacetCounts(taxoReader, config, sfc);
    SimpleFacetResult result = facets.getTopChildren(10, "Author");

    indexReader.close();
    taxoReader.close();
    
    return result;
  }

  /** Runs the search example. */
  public List<SimpleFacetResult> runSearch() throws IOException {
    index();
    return search();
  }
  
  /** Runs the drill-down example. */
  public SimpleFacetResult runDrillDown() throws IOException {
    index();
    return drillDown();
  }

  /** Runs the search and drill-down examples and prints the results. */
  public static void main(String[] args) throws Exception {
    System.out.println("Facet counting example:");
    System.out.println("-----------------------");
    SimpleFacetsExample example = new SimpleFacetsExample();
    List<SimpleFacetResult> results = example.runSearch();
    System.out.println("Author: " + results.get(0));
    System.out.println("Publish Date: " + results.get(1));

    System.out.println("\n");
    System.out.println("Facet drill-down example (Publish Date/2010):");
    System.out.println("---------------------------------------------");
    System.out.println("Author: " + example.runDrillDown());
  }
  
}
