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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.RangeAccumulator;
import org.apache.lucene.facet.range.RangeFacetRequest;
import org.apache.lucene.facet.search.DrillDownQuery;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;


/** Shows simple usage of dynamic range faceting. */
public class RangeFacetsExample implements Closeable {

  private final Directory indexDir = new RAMDirectory();
  private IndexSearcher searcher;
  private final long nowSec = System.currentTimeMillis();

  /** Empty constructor */
  public RangeFacetsExample() {}
  
  /** Build the example index. */
  public void index() throws IOException {
    IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, 
        new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));

    // Add documents with a fake timestamp, 1000 sec before
    // "now", 2000 sec before "now", ...:
    for(int i=0;i<100;i++) {
      Document doc = new Document();
      long then = nowSec - i * 1000;
      // Add as doc values field, so we can compute range facets:
      doc.add(new NumericDocValuesField("timestamp", then));
      // Add as numeric field so we can drill-down:
      doc.add(new LongField("timestamp", then, Field.Store.NO));
      indexWriter.addDocument(doc);
    }

    // Open near-real-time searcher
    searcher = new IndexSearcher(DirectoryReader.open(indexWriter, true));
    indexWriter.close();
  }

  /** User runs a query and counts facets. */
  public List<FacetResult> search() throws IOException {

    FacetSearchParams fsp = new FacetSearchParams(
                                new RangeFacetRequest<LongRange>("timestamp",
                                                                 new LongRange("Past hour", nowSec-3600, true, nowSec, true),
                                                                 new LongRange("Past six hours", nowSec-6*3600, true, nowSec, true),
                                                                 new LongRange("Past day", nowSec-24*3600, true, nowSec, true)));
    // Aggregatses the facet counts
    FacetsCollector fc = FacetsCollector.create(new RangeAccumulator(fsp, searcher.getIndexReader()));

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query, and use MultiCollector to
    // wrap collecting the "normal" hits and also facets:
    searcher.search(new MatchAllDocsQuery(), fc);

    // Retrieve results
    return fc.getFacetResults();
  }
  
  /** User drills down on the specified range. */
  public TopDocs drillDown(LongRange range) throws IOException {

    // Passing no baseQuery means we drill down on all
    // documents ("browse only"):
    DrillDownQuery q = new DrillDownQuery(FacetIndexingParams.DEFAULT);

    // Use FieldCacheRangeFilter; this will use
    // NumericDocValues:
    q.add("timestamp", NumericRangeQuery.newLongRange("timestamp", range.min, range.max, range.minInclusive, range.maxInclusive));

    return searcher.search(q, 10);
  }

  public void close() throws IOException {
    searcher.getIndexReader().close();
    indexDir.close();
  }

  /** Runs the search and drill-down examples and prints the results. */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    RangeFacetsExample example = new RangeFacetsExample();
    example.index();

    System.out.println("Facet counting example:");
    System.out.println("-----------------------");
    List<FacetResult> results = example.search();
    for (FacetResult res : results) {
      System.out.println(res);
    }

    System.out.println("\n");
    System.out.println("Facet drill-down example (timestamp/Past six hours):");
    System.out.println("---------------------------------------------");
    TopDocs hits = example.drillDown((LongRange) ((RangeFacetRequest<LongRange>) results.get(0).getFacetRequest()).ranges[1]);
    System.out.println(hits.totalHits + " totalHits");

    example.close();
  }
}
