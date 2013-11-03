package org.apache.lucene.demo.facet;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.SumValueSourceFacetRequest;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SortField;
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

/** Shows facets aggregation by an expression. */
public class ExpressionAggregationFacetsExample {

  private final Directory indexDir = new RAMDirectory();
  private final Directory taxoDir = new RAMDirectory();

  /** Empty constructor */
  public ExpressionAggregationFacetsExample() {}
  
  private void add(IndexWriter indexWriter, FacetFields facetFields, String text, String category, long popularity) throws IOException {
    Document doc = new Document();
    doc.add(new TextField("c", text, Store.NO));
    doc.add(new NumericDocValuesField("popularity", popularity));
    facetFields.addFields(doc, Collections.singletonList(new CategoryPath(category, '/')));
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

    add(indexWriter, facetFields, "foo bar", "A/B", 5L);
    add(indexWriter, facetFields, "foo foo bar", "A/C", 3L);
    
    indexWriter.close();
    taxoWriter.close();
  }

  /** User runs a query and aggregates facets. */
  private List<FacetResult> search() throws IOException, ParseException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    // Aggregate categories by an expression that combines the document's score
    // and its popularity field
    Expression expr = JavascriptCompiler.compile("_score * sqrt(popularity)");
    SimpleBindings bindings = new SimpleBindings();
    bindings.add(new SortField("_score", SortField.Type.SCORE)); // the score of the document
    bindings.add(new SortField("popularity", SortField.Type.LONG)); // the value of the 'popularity' field

    FacetSearchParams fsp = new FacetSearchParams(
        new SumValueSourceFacetRequest(new CategoryPath("A"), 10, expr.getValueSource(bindings), true));

    // Aggregates the facet values
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
  
  /** Runs the search example. */
  public List<FacetResult> runSearch() throws IOException, ParseException {
    index();
    return search();
  }
  
  /** Runs the search and drill-down examples and prints the results. */
  public static void main(String[] args) throws Exception {
    System.out.println("Facet counting example:");
    System.out.println("-----------------------");
    List<FacetResult> results = new ExpressionAggregationFacetsExample().runSearch();
    for (FacetResult res : results) {
      System.out.println(res);
    }
  }
  
}
