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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.LongRangeFacetCounts;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/** Shows simple usage of dynamic range faceting. */
public class RangeFacetsExample implements Closeable {

  private final Directory indexDir = new RAMDirectory();
  private IndexSearcher searcher;
  private final long nowSec = System.currentTimeMillis()/1000L;

  final LongRange PAST_HOUR = new LongRange("Past hour", nowSec-3600, true, nowSec, true);
  final LongRange PAST_SIX_HOURS = new LongRange("Past six hours", nowSec-6*3600, true, nowSec, true);
  final LongRange PAST_DAY = new LongRange("Past day", nowSec-24*3600, true, nowSec, true);

  /** Empty constructor */
  public RangeFacetsExample() {}
  
  /** Build the example index. */
  public void index() throws IOException {
    IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(
        new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE));

    // Add documents with a fake timestamp, 1000 sec before
    // "now", 2000 sec before "now", ...:
    for(int i=0;i<100;i++) {
      Document doc = new Document();
      long then = nowSec - i * 1000;
      // Add as doc values field, so we can compute range facets:
      doc.add(new NumericDocValuesField("timestamp", then));
      // Add as numeric field so we can drill-down:
      doc.add(new LongPoint("timestamp", then));
      indexWriter.addDocument(doc);
    }

    // Open near-real-time searcher
    searcher = new IndexSearcher(DirectoryReader.open(indexWriter));
    indexWriter.close();
  }

  private FacetsConfig getConfig() {
    return new FacetsConfig();
  }

  /** User runs a query and counts facets. */
  public FacetResult search() throws IOException {

    // Aggregates the facet counts
    FacetsCollector fc = new FacetsCollector();

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query:
    FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, fc);

    Facets facets = new LongRangeFacetCounts("timestamp", fc,
                                             PAST_HOUR,
                                             PAST_SIX_HOURS,
                                             PAST_DAY);
    return facets.getTopChildren(10, "timestamp");
  }
  
  /** User drills down on the specified range. */
  public TopDocs drillDown(LongRange range) throws IOException {

    // Passing no baseQuery means we drill down on all
    // documents ("browse only"):
    DrillDownQuery q = new DrillDownQuery(getConfig());

    q.add("timestamp", LongPoint.newRangeQuery("timestamp", range.min, range.max));
    return searcher.search(q, 10);
  }

  /** User drills down on the specified range, and also computes drill sideways counts. */
  public DrillSideways.DrillSidewaysResult drillSideways(LongRange range) throws IOException {
    // Passing no baseQuery means we drill down on all
    // documents ("browse only"):
    DrillDownQuery q = new DrillDownQuery(getConfig());
    q.add("timestamp", LongPoint.newRangeQuery("timestamp", range.min, range.max));

    // DrillSideways only handles taxonomy and sorted set drill facets by default; to do range facets we must subclass and override the
    // buildFacetsResult method.
    DrillSideways.DrillSidewaysResult result = new DrillSideways(searcher, getConfig(), null, null) {
      @Override
      protected Facets buildFacetsResult(FacetsCollector drillDowns, FacetsCollector[] drillSideways, String[] drillSidewaysDims) throws IOException {
        // If we had other dims we would also compute their drill-down or drill-sideways facets here:
        assert drillSidewaysDims[0].equals("timestamp");
        return new LongRangeFacetCounts("timestamp", drillSideways[0],
                                        PAST_HOUR,
                                        PAST_SIX_HOURS,
                                        PAST_DAY);
      }
    }.search(q, 10);

    return result;
  }

  @Override
  public void close() throws IOException {
    searcher.getIndexReader().close();
    indexDir.close();
  }

  /** Runs the search and drill-down examples and prints the results. */
  public static void main(String[] args) throws Exception {
    RangeFacetsExample example = new RangeFacetsExample();
    example.index();

    System.out.println("Facet counting example:");
    System.out.println("-----------------------");
    System.out.println(example.search());

    System.out.println("\n");
    System.out.println("Facet drill-down example (timestamp/Past six hours):");
    System.out.println("---------------------------------------------");
    TopDocs hits = example.drillDown(example.PAST_SIX_HOURS);
    System.out.println(hits.totalHits + " totalHits");

    System.out.println("\n");
    System.out.println("Facet drill-sideways example (timestamp/Past six hours):");
    System.out.println("---------------------------------------------");
    DrillSideways.DrillSidewaysResult sideways = example.drillSideways(example.PAST_SIX_HOURS);
    System.out.println(sideways.hits.totalHits + " totalHits");
    System.out.println(sideways.facets.getTopChildren(10, "timestamp"));

    example.close();
  }
}
