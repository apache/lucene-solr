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
package org.apache.lucene.demo.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

/** Demonstrates indexing categories into different indexed fields. */
public class MultiCategoryListsFacetsExample {

  private final Directory indexDir = new ByteBuffersDirectory();
  private final Directory taxoDir = new ByteBuffersDirectory();
  private final FacetsConfig config = new FacetsConfig();

  /** Creates a new instance and populates the category list params mapping. */
  public MultiCategoryListsFacetsExample() {
    config.setIndexFieldName("Author", "author");
    config.setIndexFieldName("Publish Date", "pubdate");
    config.setHierarchical("Publish Date", true);
  }

  /** Build the example index. */
  private void index() throws IOException {
    IndexWriter indexWriter =
        new IndexWriter(
            indexDir, new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE));

    // Writes facet ords to a separate directory from the main index
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    indexWriter.close();
    taxoWriter.close();
  }

  /** User runs a query and counts facets. */
  private List<FacetResult> search() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    FacetsCollector fc = new FacetsCollector();

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query:
    FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, fc);

    // Retrieve results
    List<FacetResult> results = new ArrayList<>();

    // Count both "Publish Date" and "Author" dimensions
    Facets author = new FastTaxonomyFacetCounts("author", taxoReader, config, fc);
    results.add(author.getTopChildren(10, "Author"));

    Facets pubDate = new FastTaxonomyFacetCounts("pubdate", taxoReader, config, fc);
    results.add(pubDate.getTopChildren(10, "Publish Date"));

    indexReader.close();
    taxoReader.close();

    return results;
  }

  /** Runs the search example. */
  public List<FacetResult> runSearch() throws IOException {
    index();
    return search();
  }

  /** Runs the search example and prints the results. */
  public static void main(String[] args) throws Exception {
    System.out.println("Facet counting over multiple category lists example:");
    System.out.println("-----------------------");
    List<FacetResult> results = new MultiCategoryListsFacetsExample().runSearch();
    System.out.println("Author: " + results.get(0));
    System.out.println("Publish Date: " + results.get(1));
  }
}
