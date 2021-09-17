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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.TestUtil;

public class TestDrillSideways extends FacetTestCase {

  protected DrillSideways getNewDrillSideways(IndexSearcher searcher, FacetsConfig config,
          SortedSetDocValuesReaderState state) {
    return new DrillSideways(searcher, config, state);
  }

  protected DrillSideways getNewDrillSideways(IndexSearcher searcher, FacetsConfig config, TaxonomyReader taxoReader) {
    return new DrillSideways(searcher, config, taxoReader);
  }

  protected DrillSideways getNewDrillSidewaysScoreSubdocsAtOnce(IndexSearcher searcher, FacetsConfig config,
          TaxonomyReader taxoReader) {
    return new DrillSideways(searcher, config, taxoReader) {
      @Override
      protected boolean scoreSubDocsAtOnce() {
        return true;
      }
    };
  }

  protected DrillSideways getNewDrillSidewaysBuildFacetsResult(IndexSearcher searcher, FacetsConfig config,
          TaxonomyReader taxoReader) {
    return new DrillSideways(searcher, config, taxoReader) {
      @Override
      protected Facets buildFacetsResult(FacetsCollector drillDowns, FacetsCollector[] drillSideways,
              String[] drillSidewaysDims) throws IOException {
        Map<String, Facets> drillSidewaysFacets = new HashMap<>();
        Facets drillDownFacets = getTaxonomyFacetCounts(taxoReader, config, drillDowns);
        if (drillSideways != null) {
          for (int i = 0; i < drillSideways.length; i++) {
            drillSidewaysFacets.put(drillSidewaysDims[i], getTaxonomyFacetCounts(taxoReader, config, drillSideways[i]));
          }
        }

        if (drillSidewaysFacets.isEmpty()) {
          return drillDownFacets;
        } else {
          return new MultiFacets(drillSidewaysFacets, drillDownFacets);
        }

      }
    };
  }

  private IndexSearcher getNewSearcher(IndexReader reader) {
    // Do not wrap with an asserting searcher, since DrillSidewaysQuery doesn't
    // implement all the required components like Weight#scorer.
    return newSearcher(reader, true, false, random().nextBoolean());
  }

  // See LUCENE-10060:
  public void testNoCaching() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();

    // Setup some basic test documents:
    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new SortedDocValuesField("id", new BytesRef("1")));
    doc.add(new FacetField("Color", "Red"));
    doc.add(new FacetField("Size", "Small"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new StringField("id", "2", Field.Store.NO));
    doc.add(new SortedDocValuesField("id", new BytesRef("2")));
    doc.add(new FacetField("Color", "Green"));
    doc.add(new FacetField("Size", "Small"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new StringField("id", "3", Field.Store.NO));
    doc.add(new SortedDocValuesField("id", new BytesRef("3")));
    doc.add(new FacetField("Color", "Blue"));
    doc.add(new FacetField("Size", "Small"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new StringField("id", "4", Field.Store.NO));
    doc.add(new SortedDocValuesField("id", new BytesRef("4")));
    doc.add(new FacetField("Color", "Red"));
    doc.add(new FacetField("Size", "Medium"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new StringField("id", "5", Field.Store.NO));
    doc.add(new SortedDocValuesField("id", new BytesRef("5")));
    doc.add(new FacetField("Color", "Blue"));
    doc.add(new FacetField("Size", "Medium"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new StringField("id", "6", Field.Store.NO));
    doc.add(new SortedDocValuesField("id", new BytesRef("6")));
    doc.add(new FacetField("Color", "Blue"));
    doc.add(new FacetField("Size", "Large"));
    writer.addDocument(config.build(taxoWriter, doc));

    // Delete a couple documents. We'll want to make sure they don't get counted in binning:
    writer.deleteDocuments(new Term("id", "4"));
    writer.deleteDocuments(new Term("id", "6"));

    // Simple DDQ that just filters all results by Color == Blue:
    DrillDownQuery ddq = new DrillDownQuery(config);
    ddq.add("Color", "Blue");

    // Setup an IndexSearcher that will try to cache queries aggressively:
    IndexSearcher searcher = getNewSearcher(writer.getReader());
    searcher.setQueryCachingPolicy(
        new QueryCachingPolicy() {
          @Override
          public void onUse(Query query) {}

          @Override
          public boolean shouldCache(Query query) {
            return true;
          }
        });

    // Setup a DS instance for searching:
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    DrillSideways ds = getNewDrillSideways(searcher, config, taxoReader);

    // We'll use a CollectorManager to trigger the trickiest caching behavior:
    SimpleCollectorManager collectorManager =
        new SimpleCollectorManager(
            10, Comparator.comparing(cr -> cr.id), ScoreMode.COMPLETE_NO_SCORES);
    // Make sure our CM produces Collectors that _do not_ need scores to ensure IndexSearcher tries
    // to cache:
    assertFalse(collectorManager.newCollector().scoreMode().needsScores());
    // If we incorrectly cache here, the "sideways" FacetsCollectors will get populated with counts
    // for the deleted
    // docs. Make sure they don't:
    DrillSideways.ConcurrentDrillSidewaysResult<List<DocAndScore>> concurrentResult =
        ds.search(ddq, collectorManager);
    assertEquals(2, concurrentResult.collectorResult.size());
    assertEquals(
        "dim=Color path=[] value=4 childCount=3\n  Blue (2)\n  Red (1)\n  Green (1)\n",
        concurrentResult.facets.getTopChildren(10, "Color").toString());
    assertEquals(
        "dim=Size path=[] value=2 childCount=2\n  Small (1)\n  Medium (1)\n",
        concurrentResult.facets.getTopChildren(10, "Size").toString());

    // Now do the same thing but use a Collector directly:
    SimpleCollector collector = new SimpleCollector(ScoreMode.COMPLETE_NO_SCORES);
    // Make sure our Collector _does not_ need scores to ensure IndexSearcher tries to cache:
    assertFalse(collector.scoreMode().needsScores());
    // If we incorrectly cache here, the "sideways" FacetsCollectors will get populated with counts
    // for the deleted
    // docs. Make sure they don't:
    DrillSidewaysResult result = ds.search(ddq, collector);
    assertEquals(2, collector.hits.size());
    assertEquals(
        "dim=Color path=[] value=4 childCount=3\n  Blue (2)\n  Red (1)\n  Green (1)\n",
        result.facets.getTopChildren(10, "Color").toString());
    assertEquals(
        "dim=Size path=[] value=2 childCount=2\n  Small (1)\n  Medium (1)\n",
        result.facets.getTopChildren(10, "Size").toString());

    writer.close();
    IOUtils.close(searcher.getIndexReader(), taxoReader, taxoWriter, dir, taxoDir);
  }

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = getNewSearcher(writer.getReader());

    //System.out.println("searcher=" + searcher);

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    DrillSideways ds = getNewDrillSideways(searcher, config, taxoReader);

    //  case: drill-down on a single field; in this
    // case the drill-sideways + drill-down counts ==
    // drill-down of just the query:
    DrillDownQuery ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    DrillSidewaysResult r = ds.search(null, ddq, 10);
    assertEquals(2, r.hits.totalHits.value);
    // Publish Date is only drill-down, and Lisa published
    // one in 2012 and one in 2010:
    assertEquals("dim=Publish Date path=[] value=2 childCount=2\n  2010 (1)\n  2012 (1)\n",
            r.facets.getTopChildren(10, "Publish Date").toString());

    // Author is drill-sideways + drill-down: Lisa
    // (drill-down) published twice, and Frank/Susan/Bob
    // published once:
    assertEquals("dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
            r.facets.getTopChildren(10, "Author").toString());

    // Same simple case, but no baseQuery (pure browse):
    // drill-down on a single field; in this case the
    // drill-sideways + drill-down counts == drill-down of
    // just the query:
    ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    r = ds.search(null, ddq, 10);

    assertEquals(2, r.hits.totalHits.value);
    // Publish Date is only drill-down, and Lisa published
    // one in 2012 and one in 2010:
    assertEquals("dim=Publish Date path=[] value=2 childCount=2\n  2010 (1)\n  2012 (1)\n",
            r.facets.getTopChildren(10, "Publish Date").toString());

    // Author is drill-sideways + drill-down: Lisa
    // (drill-down) published twice, and Frank/Susan/Bob
    // published once:
    assertEquals("dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
            r.facets.getTopChildren(10, "Author").toString());

    // Another simple case: drill-down on single fields
    // but OR of two values
    ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    ddq.add("Author", "Bob");
    r = ds.search(null, ddq, 10);
    assertEquals(3, r.hits.totalHits.value);
    // Publish Date is only drill-down: Lisa and Bob
    // (drill-down) published twice in 2010 and once in 2012:
    assertEquals("dim=Publish Date path=[] value=3 childCount=2\n  2010 (2)\n  2012 (1)\n",
            r.facets.getTopChildren(10, "Publish Date").toString());
    // Author is drill-sideways + drill-down: Lisa
    // (drill-down) published twice, and Frank/Susan/Bob
    // published once:
    assertEquals("dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
            r.facets.getTopChildren(10, "Author").toString());

    assertTrue(r.facets instanceof MultiFacets);
    List<FacetResult> allResults = r.facets.getAllDims(10);
    assertEquals(2, allResults.size());
    assertEquals("dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
            allResults.get(0).toString());
    assertEquals("dim=Publish Date path=[] value=3 childCount=2\n  2010 (2)\n  2012 (1)\n",
            allResults.get(1).toString());

    // More interesting case: drill-down on two fields
    ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    ddq.add("Publish Date", "2010");
    r = ds.search(null, ddq, 10);
    assertEquals(1, r.hits.totalHits.value);
    // Publish Date is drill-sideways + drill-down: Lisa
    // (drill-down) published once in 2010 and once in 2012:
    assertEquals("dim=Publish Date path=[] value=2 childCount=2\n  2010 (1)\n  2012 (1)\n",
            r.facets.getTopChildren(10, "Publish Date").toString());
    // Author is drill-sideways + drill-down:
    // only Lisa & Bob published (once each) in 2010:
    assertEquals("dim=Author path=[] value=2 childCount=2\n  Bob (1)\n  Lisa (1)\n",
            r.facets.getTopChildren(10, "Author").toString());

    // Even more interesting case: drill down on two fields,
    // but one of them is OR
    ddq = new DrillDownQuery(config);

    // Drill down on Lisa or Bob:
    ddq.add("Author", "Lisa");
    ddq.add("Publish Date", "2010");
    ddq.add("Author", "Bob");
    r = ds.search(null, ddq, 10);
    assertEquals(2, r.hits.totalHits.value);
    // Publish Date is both drill-sideways + drill-down:
    // Lisa or Bob published twice in 2010 and once in 2012:
    assertEquals("dim=Publish Date path=[] value=3 childCount=2\n  2010 (2)\n  2012 (1)\n",
            r.facets.getTopChildren(10, "Publish Date").toString());
    // Author is drill-sideways + drill-down:
    // only Lisa & Bob published (once each) in 2010:
    assertEquals("dim=Author path=[] value=2 childCount=2\n  Bob (1)\n  Lisa (1)\n",
            r.facets.getTopChildren(10, "Author").toString());

    // Test drilling down on invalid field:
    ddq = new DrillDownQuery(config);
    ddq.add("Foobar", "Baz");
    r = ds.search(null, ddq, 10);
    assertEquals(0, r.hits.totalHits.value);
    assertNull(r.facets.getTopChildren(10, "Publish Date"));
    assertNull(r.facets.getTopChildren(10, "Foobar"));

    // Test drilling down on valid term or'd with invalid term:
    ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    ddq.add("Author", "Tom");
    r = ds.search(null, ddq, 10);
    assertEquals(2, r.hits.totalHits.value);
    // Publish Date is only drill-down, and Lisa published
    // one in 2012 and one in 2010:
    assertEquals("dim=Publish Date path=[] value=2 childCount=2\n  2010 (1)\n  2012 (1)\n",
            r.facets.getTopChildren(10, "Publish Date").toString());
    // Author is drill-sideways + drill-down: Lisa
    // (drill-down) published twice, and Frank/Susan/Bob
    // published once:
    assertEquals("dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
            r.facets.getTopChildren(10, "Author").toString());

    // LUCENE-4915: test drilling down on a dimension but
    // NOT facet counting it:
    ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    ddq.add("Author", "Tom");
    r = ds.search(null, ddq, 10);
    assertEquals(2, r.hits.totalHits.value);
    // Publish Date is only drill-down, and Lisa published
    // one in 2012 and one in 2010:
    assertEquals("dim=Publish Date path=[] value=2 childCount=2\n  2010 (1)\n  2012 (1)\n",
            r.facets.getTopChildren(10, "Publish Date").toString());

    // Test main query gets null scorer:
    ddq = new DrillDownQuery(config, new TermQuery(new Term("foobar", "baz")));
    ddq.add("Author", "Lisa");
    r = ds.search(null, ddq, 10);

    assertEquals(0, r.hits.totalHits.value);
    assertNull(r.facets.getTopChildren(10, "Publish Date"));
    assertNull(r.facets.getTopChildren(10, "Author"));
    writer.close();
    IOUtils.close(searcher.getIndexReader(), taxoReader, taxoWriter, dir, taxoDir);
  }

  public void testBasicWithCollectorManager() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(newStringField("content", "foo", Field.Store.NO));
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(newStringField("content", "foo", Field.Store.NO));
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(newStringField("content", "foo", Field.Store.NO));
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(newStringField("content", "bar", Field.Store.NO));
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(newStringField("content", "bar", Field.Store.NO));
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = getNewSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    DrillSideways ds = getNewDrillSideways(searcher, config, taxoReader);

    Comparator<CollectedResult> comparator =
        (a, b) -> {
          int cmp = Float.compare(b.docAndScore.score, a.docAndScore.score); // highest-to-lowest
          if (cmp == 0) {
            cmp = Integer.compare(a.docAndScore.doc, b.docAndScore.doc); // tie-break
          }
          return cmp;
        };

    SimpleCollectorManager manager = new SimpleCollectorManager(10, comparator);

    // case: drill-down on a single field; in this
    // case the drill-sideways + drill-down counts ==
    // drill-down of just the query:
    DrillDownQuery ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    DrillSideways.ConcurrentDrillSidewaysResult<List<DocAndScore>> r = ds.search(ddq, manager);
    assertEquals(2, r.collectorResult.size());
    // Publish Date is only drill-down, and Lisa published
    // one in 2012 and one in 2010:
    assertEquals(
        "dim=Publish Date path=[] value=2 childCount=2\n  2010 (1)\n  2012 (1)\n",
        r.facets.getTopChildren(10, "Publish Date").toString());

    // Author is drill-sideways + drill-down: Lisa
    // (drill-down) published twice, and Frank/Susan/Bob
    // published once:
    assertEquals(
        "dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
        r.facets.getTopChildren(10, "Author").toString());

    // Same simple case, but no baseQuery (pure browse):
    // drill-down on a single field; in this case the
    // drill-sideways + drill-down counts == drill-down of
    // just the query:
    ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    r = ds.search(ddq, manager);

    assertEquals(2, r.collectorResult.size());
    // Publish Date is only drill-down, and Lisa published
    // one in 2012 and one in 2010:
    assertEquals(
        "dim=Publish Date path=[] value=2 childCount=2\n  2010 (1)\n  2012 (1)\n",
        r.facets.getTopChildren(10, "Publish Date").toString());

    // Author is drill-sideways + drill-down: Lisa
    // (drill-down) published twice, and Frank/Susan/Bob
    // published once:
    assertEquals(
        "dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
        r.facets.getTopChildren(10, "Author").toString());

    // Another simple case: drill-down on single fields
    // but OR of two values
    ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    ddq.add("Author", "Bob");
    r = ds.search(ddq, manager);
    assertEquals(3, r.collectorResult.size());
    // Publish Date is only drill-down: Lisa and Bob
    // (drill-down) published twice in 2010 and once in 2012:
    assertEquals(
        "dim=Publish Date path=[] value=3 childCount=2\n  2010 (2)\n  2012 (1)\n",
        r.facets.getTopChildren(10, "Publish Date").toString());
    // Author is drill-sideways + drill-down: Lisa
    // (drill-down) published twice, and Frank/Susan/Bob
    // published once:
    assertEquals(
        "dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
        r.facets.getTopChildren(10, "Author").toString());

    assertTrue(r.facets instanceof MultiFacets);
    List<FacetResult> allResults = r.facets.getAllDims(10);
    assertEquals(2, allResults.size());
    assertEquals(
        "dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
        allResults.get(0).toString());
    assertEquals(
        "dim=Publish Date path=[] value=3 childCount=2\n  2010 (2)\n  2012 (1)\n",
        allResults.get(1).toString());

    // More interesting case: drill-down on two fields
    ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    ddq.add("Publish Date", "2010");
    r = ds.search(ddq, manager);
    assertEquals(1, r.collectorResult.size());
    // Publish Date is drill-sideways + drill-down: Lisa
    // (drill-down) published once in 2010 and once in 2012:
    assertEquals(
        "dim=Publish Date path=[] value=2 childCount=2\n  2010 (1)\n  2012 (1)\n",
        r.facets.getTopChildren(10, "Publish Date").toString());
    // Author is drill-sideways + drill-down:
    // only Lisa & Bob published (once each) in 2010:
    assertEquals(
        "dim=Author path=[] value=2 childCount=2\n  Bob (1)\n  Lisa (1)\n",
        r.facets.getTopChildren(10, "Author").toString());

    // Even more interesting case: drill down on two fields,
    // but one of them is OR
    ddq = new DrillDownQuery(config);

    // Drill down on Lisa or Bob:
    ddq.add("Author", "Lisa");
    ddq.add("Publish Date", "2010");
    ddq.add("Author", "Bob");
    r = ds.search(ddq, manager);
    assertEquals(2, r.collectorResult.size());
    // Publish Date is both drill-sideways + drill-down:
    // Lisa or Bob published twice in 2010 and once in 2012:
    assertEquals(
        "dim=Publish Date path=[] value=3 childCount=2\n  2010 (2)\n  2012 (1)\n",
        r.facets.getTopChildren(10, "Publish Date").toString());
    // Author is drill-sideways + drill-down:
    // only Lisa & Bob published (once each) in 2010:
    assertEquals(
        "dim=Author path=[] value=2 childCount=2\n  Bob (1)\n  Lisa (1)\n",
        r.facets.getTopChildren(10, "Author").toString());

    // Test drilling down on invalid field:
    ddq = new DrillDownQuery(config);
    ddq.add("Foobar", "Baz");
    r = ds.search(ddq, manager);
    assertEquals(0, r.collectorResult.size());
    assertNull(r.facets.getTopChildren(10, "Publish Date"));
    assertNull(r.facets.getTopChildren(10, "Foobar"));

    // Test drilling down on valid term or'd with invalid term:
    ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    ddq.add("Author", "Tom");
    r = ds.search(ddq, manager);
    assertEquals(2, r.collectorResult.size());
    // Publish Date is only drill-down, and Lisa published
    // one in 2012 and one in 2010:
    assertEquals(
        "dim=Publish Date path=[] value=2 childCount=2\n  2010 (1)\n  2012 (1)\n",
        r.facets.getTopChildren(10, "Publish Date").toString());
    // Author is drill-sideways + drill-down: Lisa
    // (drill-down) published twice, and Frank/Susan/Bob
    // published once:
    assertEquals(
        "dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
        r.facets.getTopChildren(10, "Author").toString());

    // LUCENE-4915: test drilling down on a dimension but
    // NOT facet counting it:
    ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    ddq.add("Author", "Tom");
    r = ds.search(ddq, manager);
    assertEquals(2, r.collectorResult.size());
    // Publish Date is only drill-down, and Lisa published
    // one in 2012 and one in 2010:
    assertEquals(
        "dim=Publish Date path=[] value=2 childCount=2\n  2010 (1)\n  2012 (1)\n",
        r.facets.getTopChildren(10, "Publish Date").toString());

    // Test main query gets null scorer:
    ddq = new DrillDownQuery(config, new TermQuery(new Term("foobar", "baz")));
    ddq.add("Author", "Lisa");
    r = ds.search(ddq, manager);
    assertEquals(0, r.collectorResult.size());
    assertNull(r.facets.getTopChildren(10, "Publish Date"));
    assertNull(r.facets.getTopChildren(10, "Author"));

    // Test no drill down dims:
    ddq = new DrillDownQuery(config, new MatchAllDocsQuery());
    r = ds.search(ddq, manager);
    assertEquals(5, r.collectorResult.size());
    assertEquals(
        "dim=Publish Date path=[] value=5 childCount=3\n  2010 (2)\n  2012 (2)\n  1999 (1)\n",
        r.facets.getTopChildren(10, "Publish Date").toString());
    assertEquals(
        "dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
        r.facets.getTopChildren(10, "Author").toString());

    // Test no drill down dims with null FacetsCollectorManager for the main query:
    ddq = new DrillDownQuery(config, new MatchAllDocsQuery());
    r =
        new DrillSideways(searcher, config, taxoReader) {
          @Override
          protected FacetsCollectorManager createDrillDownFacetsCollectorManager() {
            return null;
          }
        }.search(ddq, manager);
    assertEquals(5, r.collectorResult.size());
    // Expect null facets since we provided a null FacetsCollectorManager
    assertNull(r.facets);

    // Test the case where the base query rewrites itself. See LUCENE-9988:
    Query baseQuery =
        new TermQuery(new Term("content", "foo")) {
          @Override
          public Query rewrite(IndexReader reader) {
            // return a new instance, forcing the DrillDownQuery to also rewrite itself, exposing
            // the bug in LUCENE-9988:
            return new TermQuery(getTerm());
          }
        };
    ddq = new DrillDownQuery(config, baseQuery);
    ddq.add("Author", "Lisa");
    r = ds.search(ddq, manager);
    assertEquals(2, r.collectorResult.size());
    assertEquals(
        "dim=Publish Date path=[] value=2 childCount=2\n  2010 (1)\n  2012 (1)\n",
        r.facets.getTopChildren(10, "Publish Date").toString());
    assertEquals(
        "dim=Author path=[] value=3 childCount=2\n  Lisa (2)\n  Bob (1)\n",
        r.facets.getTopChildren(10, "Author").toString());

    writer.close();
    IOUtils.close(searcher.getIndexReader(), taxoReader, taxoWriter, dir, taxoDir);
  }

  public void testSometimesInvalidDrillDown() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    writer.addDocument(config.build(taxoWriter, doc));

    writer.commit();

    // 2nd segment has no Author:
    doc = new Document();
    doc.add(new FacetField("Foobar", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = getNewSearcher(writer.getReader());

    //System.out.println("searcher=" + searcher);

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    DrillDownQuery ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    DrillSidewaysResult r = getNewDrillSideways(searcher, config, taxoReader).search(null, ddq, 10);

    assertEquals(1, r.hits.totalHits.value);
    // Publish Date is only drill-down, and Lisa published
    // one in 2012 and one in 2010:
    assertEquals("dim=Publish Date path=[] value=1 childCount=1\n  2010 (1)\n",
            r.facets.getTopChildren(10, "Publish Date").toString());
    // Author is drill-sideways + drill-down: Lisa
    // (drill-down) published once, and Bob
    // published once:
    assertEquals("dim=Author path=[] value=2 childCount=2\n  Bob (1)\n  Lisa (1)\n",
            r.facets.getTopChildren(10, "Author").toString());

    writer.close();
    IOUtils.close(searcher.getIndexReader(), taxoReader, taxoWriter, dir, taxoDir);
  }

  public void testMultipleRequestsPerDim() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("dim", true);

    Document doc = new Document();
    doc.add(new FacetField("dim", "a", "x"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("dim", "a", "y"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("dim", "a", "z"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("dim", "b"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("dim", "c"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("dim", "d"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = getNewSearcher(writer.getReader());

    //System.out.println("searcher=" + searcher);

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    DrillDownQuery ddq = new DrillDownQuery(config);
    ddq.add("dim", "a");
    DrillSidewaysResult r = getNewDrillSideways(searcher, config, taxoReader).search(null, ddq, 10);

    assertEquals(3, r.hits.totalHits.value);
    assertEquals("dim=dim path=[] value=6 childCount=4\n  a (3)\n  b (1)\n  c (1)\n  d (1)\n",
            r.facets.getTopChildren(10, "dim").toString());
    assertEquals("dim=dim path=[a] value=3 childCount=3\n  x (1)\n  y (1)\n  z (1)\n",
            r.facets.getTopChildren(10, "dim", "a").toString());

    writer.close();
    IOUtils.close(searcher.getIndexReader(), taxoReader, taxoWriter, dir, taxoDir);
  }

  private static class Doc implements Comparable<Doc> {
    String id;
    String contentToken;

    public Doc() {
    }

    // -1 if the doc is missing this dim, else the index
    // -into the values for this dim:
    int[] dims;

    // 2nd value per dim for the doc (so we test
    // multi-valued fields):
    int[] dims2;
    boolean deleted;

    @Override
    public int compareTo(Doc other) {
      return id.compareTo(other.id);
    }
  }

  private double aChance, bChance, cChance;

  private String randomContentToken(boolean isQuery) {
    double d = random().nextDouble();
    if (isQuery) {
      if (d < 0.33) {
        return "a";
      } else if (d < 0.66) {
        return "b";
      } else {
        return "c";
      }
    } else {
      if (d <= aChance) {
        return "a";
      } else if (d < aChance + bChance) {
        return "b";
      } else {
        return "c";
      }
    }
  }

  public void testRandom() throws Exception {

    while (aChance == 0.0) {
      aChance = random().nextDouble();
    }
    while (bChance == 0.0) {
      bChance = random().nextDouble();
    }
    while (cChance == 0.0) {
      cChance = random().nextDouble();
    }
    //aChance = .01;
    //bChance = 0.5;
    //cChance = 1.0;
    double sum = aChance + bChance + cChance;
    aChance /= sum;
    bChance /= sum;
    cChance /= sum;

    int numDims = TestUtil.nextInt(random(), 2, 5);
    //int numDims = 3;
    int numDocs = atLeast(300);
    //int numDocs = 20;
    if (VERBOSE) {
      System.out.println(
              "numDims=" + numDims + " numDocs=" + numDocs + " aChance=" + aChance + " bChance=" + bChance + " cChance="
                      + cChance);
    }
    String[][] dimValues = new String[numDims][];
    int valueCount = 2;

    for (int dim = 0; dim < numDims; dim++) {
      Set<String> values = new HashSet<>();
      while (values.size() < valueCount) {
        String s = TestUtil.randomRealisticUnicodeString(random());
        //String s = _TestUtil.randomString(random());
        if (s.length() > 0) {
          values.add(s);
        }
      }
      dimValues[dim] = values.toArray(new String[values.size()]);
      valueCount *= 2;
    }

    List<Doc> docs = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      Doc doc = new Doc();
      doc.id = "" + i;
      doc.contentToken = randomContentToken(false);
      doc.dims = new int[numDims];
      doc.dims2 = new int[numDims];
      for (int dim = 0; dim < numDims; dim++) {
        if (random().nextInt(5) == 3) {
          // This doc is missing this dim:
          doc.dims[dim] = -1;
        } else if (dimValues[dim].length <= 4) {
          int dimUpto = 0;
          doc.dims[dim] = dimValues[dim].length - 1;
          while (dimUpto < dimValues[dim].length) {
            if (random().nextBoolean()) {
              doc.dims[dim] = dimUpto;
              break;
            }
            dimUpto++;
          }
        } else {
          doc.dims[dim] = random().nextInt(dimValues[dim].length);
        }

        if (random().nextInt(5) == 3) {
          // 2nd value:
          doc.dims2[dim] = random().nextInt(dimValues[dim].length);
        } else {
          doc.dims2[dim] = -1;
        }
      }
      docs.add(doc);
    }

    Directory d = newDirectory();
    Directory td = newDirectory();

    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setInfoStream(InfoStream.NO_OUTPUT);
    RandomIndexWriter w = new RandomIndexWriter(random(), d, iwc);
    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(td, IndexWriterConfig.OpenMode.CREATE);
    FacetsConfig config = new FacetsConfig();
    for (int i = 0; i < numDims; i++) {
      config.setMultiValued("dim" + i, true);
    }

    boolean doUseDV = random().nextBoolean();

    for (Doc rawDoc : docs) {
      Document doc = new Document();
      doc.add(newStringField("id", rawDoc.id, Field.Store.YES));
      doc.add(new SortedDocValuesField("id", new BytesRef(rawDoc.id)));
      doc.add(newStringField("content", rawDoc.contentToken, Field.Store.NO));

      if (VERBOSE) {
        System.out.println("  doc id=" + rawDoc.id + " token=" + rawDoc.contentToken);
      }
      for (int dim = 0; dim < numDims; dim++) {
        int dimValue = rawDoc.dims[dim];
        if (dimValue != -1) {
          if (doUseDV) {
            doc.add(new SortedSetDocValuesFacetField("dim" + dim, dimValues[dim][dimValue]));
          } else {
            doc.add(new FacetField("dim" + dim, dimValues[dim][dimValue]));
          }
          doc.add(new StringField("dim" + dim, dimValues[dim][dimValue], Field.Store.YES));
          if (VERBOSE) {
            System.out.println("    dim" + dim + "=" + new BytesRef(dimValues[dim][dimValue]));
          }
        }
        int dimValue2 = rawDoc.dims2[dim];
        if (dimValue2 != -1) {
          if (doUseDV) {
            doc.add(new SortedSetDocValuesFacetField("dim" + dim, dimValues[dim][dimValue2]));
          } else {
            doc.add(new FacetField("dim" + dim, dimValues[dim][dimValue2]));
          }
          doc.add(new StringField("dim" + dim, dimValues[dim][dimValue2], Field.Store.YES));
          if (VERBOSE) {
            System.out.println("      dim" + dim + "=" + new BytesRef(dimValues[dim][dimValue2]));
          }
        }
      }

      w.addDocument(config.build(tw, doc));
    }

    if (random().nextBoolean()) {
      // Randomly delete a few docs:
      int numDel = TestUtil.nextInt(random(), 1, (int) (numDocs * 0.05));
      if (VERBOSE) {
        System.out.println("delete " + numDel);
      }
      int delCount = 0;
      while (delCount < numDel) {
        Doc doc = docs.get(random().nextInt(docs.size()));
        if (!doc.deleted) {
          if (VERBOSE) {
            System.out.println("  delete id=" + doc.id);
          }
          doc.deleted = true;
          w.deleteDocuments(new Term("id", doc.id));
          delCount++;
        }
      }
    }

    if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("TEST: forceMerge(1)...");
      }
      w.forceMerge(1);
    }
    IndexReader r = w.getReader();

    final SortedSetDocValuesReaderState sortedSetDVState;
    IndexSearcher s = getNewSearcher(r);

    if (doUseDV) {
      sortedSetDVState = new DefaultSortedSetDocValuesReaderState(s.getIndexReader());
    } else {
      sortedSetDVState = null;
    }

    if (VERBOSE) {
      System.out.println("r.numDocs() = " + r.numDocs());
    }

    // NRT open
    TaxonomyReader tr = new DirectoryTaxonomyReader(tw);

    int numIters = atLeast(10);

    for (int iter = 0; iter < numIters; iter++) {

      String contentToken = random().nextInt(30) == 17 ? null : randomContentToken(true);
      int numDrillDown = TestUtil.nextInt(random(), 1, Math.min(4, numDims));
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " baseQuery=" + contentToken + " numDrillDown=" + numDrillDown
                + " useSortedSetDV=" + doUseDV);
      }

      String[][] drillDowns = new String[numDims][];

      int count = 0;
      boolean anyMultiValuedDrillDowns = false;
      while (count < numDrillDown) {
        int dim = random().nextInt(numDims);
        if (drillDowns[dim] == null) {
          if (random().nextBoolean()) {
            // Drill down on one value:
            drillDowns[dim] = new String[] { dimValues[dim][random().nextInt(dimValues[dim].length)] };
          } else {
            int orCount = TestUtil.nextInt(random(), 1, Math.min(5, dimValues[dim].length));
            drillDowns[dim] = new String[orCount];
            anyMultiValuedDrillDowns |= orCount > 1;
            for (int i = 0; i < orCount; i++) {
              while (true) {
                String value = dimValues[dim][random().nextInt(dimValues[dim].length)];
                for (int j = 0; j < i; j++) {
                  if (value.equals(drillDowns[dim][j])) {
                    value = null;
                    break;
                  }
                }
                if (value != null) {
                  drillDowns[dim][i] = value;
                  break;
                }
              }
            }
          }
          if (VERBOSE) {
            BytesRef[] values = new BytesRef[drillDowns[dim].length];
            for (int i = 0; i < values.length; i++) {
              values[i] = new BytesRef(drillDowns[dim][i]);
            }
            System.out.println("  dim" + dim + "=" + Arrays.toString(values));
          }
          count++;
        }
      }

      Query baseQuery;
      if (contentToken == null) {
        baseQuery = new MatchAllDocsQuery();
      } else {
        baseQuery = new TermQuery(new Term("content", contentToken));
      }

      DrillDownQuery ddq = new DrillDownQuery(config, baseQuery);

      for (int dim = 0; dim < numDims; dim++) {
        if (drillDowns[dim] != null) {
          for (String value : drillDowns[dim]) {
            ddq.add("dim" + dim, value);
          }
        }
      }

      Query filter;
      if (random().nextInt(7) == 6) {
        if (VERBOSE) {
          System.out.println("  only-even filter");
        }
        filter = new Query() {

          @Override
          public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, boost) {

              @Override
              public Scorer scorer(LeafReaderContext context) throws IOException {
                DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
                return new ConstantScoreScorer(this, score(), scoreMode, new TwoPhaseIterator(approximation) {

                  @Override
                  public boolean matches() throws IOException {
                    int docID = approximation.docID();
                    return (Integer.parseInt(context.reader().document(docID).get("id")) & 1) == 0;
                  }

                  @Override
                  public float matchCost() {
                    return 1000f;
                  }
                });
              }

              @Override
              public boolean isCacheable(LeafReaderContext ctx) {
                return false;
              }

            };
          }

          @Override
          public void visit(QueryVisitor visitor) {

          }

          @Override
          public String toString(String field) {
            return "drillSidewaysTestFilter";
          }

          @Override
          public boolean equals(Object o) {
            return o == this;
          }

          @Override
          public int hashCode() {
            return System.identityHashCode(this);
          }
        };
      } else {
        filter = null;
      }

      // Verify docs are always collected in order.  If we
      // had an AssertingScorer it could catch it when
      // Weight.scoresDocsOutOfOrder lies!:
      getNewDrillSideways(s, config, tr)
          .search(
              ddq,
              new org.apache.lucene.search.SimpleCollector() {
        int lastDocID;

        @Override
        public void collect(int doc) {
          assert doc > lastDocID;
          lastDocID = doc;
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
          lastDocID = -1;
        }

        @Override
        public ScoreMode scoreMode() {
          return ScoreMode.COMPLETE_NO_SCORES;
        }
      });

      // Also separately verify that DS respects the
      // scoreSubDocsAtOnce method, to ensure that all
      // subScorers are on the same docID:
      if (!anyMultiValuedDrillDowns) {
        // Can only do this test when there are no OR'd
        // drill-down values, because in that case it's
        // easily possible for one of the DD terms to be on
        // a future docID:
        getNewDrillSidewaysScoreSubdocsAtOnce(s, config, tr).search(ddq, new AssertingSubDocsAtOnceCollector());
      }

      Sort sort = new Sort(new SortField("id", SortField.Type.STRING));
      DrillSideways ds;
      if (doUseDV) {
        ds = getNewDrillSideways(s, config, sortedSetDVState);
      } else {
        ds = getNewDrillSidewaysBuildFacetsResult(s, config, tr);
      }

      DSResults actual = new DSResults();
      if (random().nextBoolean()) {
        DrillSidewaysResult dsr = ds.search(ddq, filter, null, numDocs, sort, true);
        actual.resultCount = dsr.hits.totalHits;
        for (ScoreDoc scoreDoc : dsr.hits.scoreDocs) {
          actual.results.add(new DocAndScore(scoreDoc.doc, scoreDoc.score));
        }
        actual.facets = dsr.facets;
      } else {

        // can't support filter queries with this impl
        filter = null;

        CollectorManager<SimpleCollector, List<DocAndScore>> manager =
            new SimpleCollectorManager(numDocs, Comparator.comparing(cr -> cr.id));
        // Because we validate the scores computed through DrillSideways against those found through
        // a direct search
        // against IndexSearcher, make sure our Collectors announce themselves as requiring scores.
        // See conversation
        // in LUCENE-10060 where this bug was introduced and then discovered:
        assertTrue(manager.newCollector().scoreMode.needsScores());
        DrillSideways.ConcurrentDrillSidewaysResult<List<DocAndScore>> cr = ds.search(ddq, manager);
        actual.results = cr.collectorResult;
        actual.resultCount = new TotalHits(actual.results.size(), TotalHits.Relation.EQUAL_TO);
        actual.facets = cr.facets;
      }

      TestFacetResult expected =
          slowDrillSidewaysSearch(s, docs, contentToken, drillDowns, dimValues, filter);

      TopDocs hits = s.search(baseQuery, numDocs);
      Map<String, Float> scores = new HashMap<>();
      for (ScoreDoc sd : hits.scoreDocs) {
        scores.put(s.doc(sd.doc).get("id"), sd.score);
      }
      if (VERBOSE) {
        System.out.println("  verify all facets");
      }
      verifyEquals(dimValues, s, expected, actual, scores, doUseDV);

      // Make sure drill down doesn't change score:
      Query q = ddq;
      if (filter != null) {
        q = new BooleanQuery.Builder().add(q, Occur.MUST).add(filter, Occur.FILTER).build();
      }
      TopDocs ddqHits = s.search(q, numDocs);
      assertEquals(expected.hits.size(), ddqHits.totalHits.value);
      for (int i = 0; i < expected.hits.size(); i++) {
        // Score should be IDENTICAL:
        assertEquals(scores.get(expected.hits.get(i).id), ddqHits.scoreDocs[i].score, 0.0f);
      }
    }

    w.close();
    IOUtils.close(r, tr, tw, d, td);
  }

  private static class Counters {
    int[][] counts;

    public Counters(String[][] dimValues) {
      counts = new int[dimValues.length][];
      for (int dim = 0; dim < dimValues.length; dim++) {
        counts[dim] = new int[dimValues[dim].length];
      }
    }

    public void inc(int[] dims, int[] dims2) {
      inc(dims, dims2, -1);
    }

    public void inc(int[] dims, int[] dims2, int onlyDim) {
      assert dims.length == counts.length;
      assert dims2.length == counts.length;
      for (int dim = 0; dim < dims.length; dim++) {
        if (onlyDim == -1 || dim == onlyDim) {
          if (dims[dim] != -1) {
            counts[dim][dims[dim]]++;
          }
          if (dims2[dim] != -1 && dims2[dim] != dims[dim]) {
            counts[dim][dims2[dim]]++;
          }
        }
      }
    }
  }

  private static class TestFacetResult {
    List<Doc> hits;
    int[][] counts;
    int[] uniqueCounts;

    public TestFacetResult() {}
  }

  private static class DocAndScore {
    final int doc;
    final float score;

    DocAndScore(int doc, float score) {
      this.doc = doc;
      this.score = score;
    }

    DocAndScore(DocAndScore other) {
      this.doc = other.doc;
      this.score = other.score;
    }
  }

  private static class DSResults {
    TotalHits resultCount;
    List<DocAndScore> results = new ArrayList<>();
    Facets facets;
  }

  private static class CollectedResult {
    final DocAndScore docAndScore;
    final String id;

    CollectedResult(DocAndScore docAndScore, String id) {
      this.docAndScore = docAndScore;
      this.id = id;
    }
  }

  private abstract static class SimpleLeafCollector implements LeafCollector {
    protected Scorable scorer;
  }

  private static class SimpleCollector implements Collector {

    private final ScoreMode scoreMode;
    final List<CollectedResult> hits = new ArrayList<>();

    SimpleCollector(ScoreMode scoreMode) {
      this.scoreMode = scoreMode;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {

      int docBase = context.docBase;
      SortedDocValues idVals = DocValues.getSorted(context.reader(), "id");

      return new SimpleLeafCollector() {
        @Override
        public void setScorer(Scorable scorer) {
          super.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
          float score = scorer.score();
          String id = null;
          if (idVals.advanceExact(doc)) {
            id = idVals.lookupOrd(idVals.ordValue()).utf8ToString();
          }
          hits.add(new CollectedResult(new DocAndScore(docBase + doc, score), id));
        }
      };
    }

    @Override
    public ScoreMode scoreMode() {
      return scoreMode;
    }
  }

  private static class SimpleCollectorManager
      implements CollectorManager<SimpleCollector, List<DocAndScore>> {
    private final int numDocs;
    private final Comparator<CollectedResult> comparator;
    private final ScoreMode scoreMode;

    SimpleCollectorManager(int numDocs, Comparator<CollectedResult> comparator) {
      this(numDocs, comparator, ScoreMode.COMPLETE);
    }

    SimpleCollectorManager(
        int numDocs, Comparator<CollectedResult> comparator, ScoreMode scoreMode) {
      this.numDocs = numDocs;
      this.comparator = comparator;
      this.scoreMode = scoreMode;
    }

    @Override
    public SimpleCollector newCollector() {
      return new SimpleCollector(scoreMode);
    }

    @Override
    public List<DocAndScore> reduce(Collection<SimpleCollector> collectors) {
      List<CollectedResult> all = new ArrayList<>();
      for (SimpleCollector c : collectors) {
        all.addAll(c.hits);
      }

      // Just sorts using the provided comparator returns up to 'numDocs'.
      // This is an inefficient way to go about doing this, but it's only for unit testing.
      return all.stream()
          .sorted(comparator)
          .map(cr -> new DocAndScore(cr.docAndScore))
          .limit(numDocs)
          .collect(Collectors.toList());
    }
  }

  private int[] getTopNOrds(final int[] counts, final String[] values, int topN) {
    final int[] ids = new int[counts.length];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = i;
    }

    // Naive (on purpose, to reduce bug in tester/gold):
    // sort all ids, then return top N slice:
    new InPlaceMergeSorter() {

      @Override
      protected void swap(int i, int j) {
        int id = ids[i];
        ids[i] = ids[j];
        ids[j] = id;
      }

      @Override
      protected int compare(int i, int j) {
        int counti = counts[ids[i]];
        int countj = counts[ids[j]];
        // Sort by count descending...
        if (counti > countj) {
          return -1;
        } else if (counti < countj) {
          return 1;
        } else {
          // ... then by label ascending:
          return new BytesRef(values[ids[i]]).compareTo(new BytesRef(values[ids[j]]));
        }
      }

    }.sort(0, ids.length);

    if (topN > ids.length) {
      topN = ids.length;
    }

    int numSet = topN;
    for (int i = 0; i < topN; i++) {
      if (counts[ids[i]] == 0) {
        numSet = i;
        break;
      }
    }

    int[] topNIDs = new int[numSet];
    System.arraycopy(ids, 0, topNIDs, 0, topNIDs.length);
    return topNIDs;
  }

  private TestFacetResult slowDrillSidewaysSearch(IndexSearcher s, List<Doc> docs, String contentToken,
          String[][] drillDowns, String[][] dimValues, Query onlyEven) throws Exception {
    int numDims = dimValues.length;

    List<Doc> hits = new ArrayList<>();
    Counters drillDownCounts = new Counters(dimValues);
    Counters[] drillSidewaysCounts = new Counters[dimValues.length];
    for (int dim = 0; dim < numDims; dim++) {
      drillSidewaysCounts[dim] = new Counters(dimValues);
    }

    if (VERBOSE) {
      System.out.println("  compute expected");
    }

    nextDoc:
    for (Doc doc : docs) {
      if (doc.deleted) {
        continue;
      }
      if (onlyEven != null & (Integer.parseInt(doc.id) & 1) != 0) {
        continue;
      }
      if (contentToken == null || doc.contentToken.equals(contentToken)) {
        int failDim = -1;
        for (int dim = 0; dim < numDims; dim++) {
          if (drillDowns[dim] != null) {
            String docValue = doc.dims[dim] == -1 ? null : dimValues[dim][doc.dims[dim]];
            String docValue2 = doc.dims2[dim] == -1 ? null : dimValues[dim][doc.dims2[dim]];
            boolean matches = false;
            for (String value : drillDowns[dim]) {
              if (value.equals(docValue) || value.equals(docValue2)) {
                matches = true;
                break;
              }
            }
            if (!matches) {
              if (failDim == -1) {
                // Doc could be a near-miss, if no other dim fails
                failDim = dim;
              } else {
                // Doc isn't a hit nor a near-miss
                continue nextDoc;
              }
            }
          }
        }

        if (failDim == -1) {
          if (VERBOSE) {
            System.out.println("    exp: id=" + doc.id + " is a hit");
          }
          // Hit:
          hits.add(doc);
          drillDownCounts.inc(doc.dims, doc.dims2);
          for (int dim = 0; dim < dimValues.length; dim++) {
            drillSidewaysCounts[dim].inc(doc.dims, doc.dims2);
          }
        } else {
          if (VERBOSE) {
            System.out.println("    exp: id=" + doc.id + " is a near-miss on dim=" + failDim);
          }
          drillSidewaysCounts[failDim].inc(doc.dims, doc.dims2, failDim);
        }
      }
    }

    Map<String, Integer> idToDocID = new HashMap<>();
    for (int i = 0; i < s.getIndexReader().maxDoc(); i++) {
      idToDocID.put(s.doc(i).get("id"), i);
    }

    Collections.sort(hits);

    TestFacetResult res = new TestFacetResult();
    res.hits = hits;
    res.counts = new int[numDims][];
    res.uniqueCounts = new int[numDims];
    for (int dim = 0; dim < numDims; dim++) {
      if (drillDowns[dim] != null) {
        res.counts[dim] = drillSidewaysCounts[dim].counts[dim];
      } else {
        res.counts[dim] = drillDownCounts.counts[dim];
      }
      int uniqueCount = 0;
      for (int j = 0; j < res.counts[dim].length; j++) {
        if (res.counts[dim][j] != 0) {
          uniqueCount++;
        }
      }
      res.uniqueCounts[dim] = uniqueCount;
    }

    return res;
  }

  void verifyEquals(
      String[][] dimValues,
      IndexSearcher s,
      TestFacetResult expected,
      DSResults actual,
          Map<String, Float> scores, boolean isSortedSetDV) throws Exception {
    if (VERBOSE) {
      System.out.println("  verify totHits=" + expected.hits.size());
    }
    assertEquals(expected.hits.size(), actual.resultCount.value);
    assertEquals(expected.hits.size(), actual.results.size());
    for (int i = 0; i < expected.hits.size(); i++) {
      if (VERBOSE) {
        System.out.println("    hit " + i + " expected=" + expected.hits.get(i).id);
      }
      assertEquals(expected.hits.get(i).id, s.doc(actual.results.get(i).doc).get("id"));
      // Score should be IDENTICAL:
      assertEquals(scores.get(expected.hits.get(i).id), actual.results.get(i).score, 0.0f);
    }

    for (int dim = 0; dim < expected.counts.length; dim++) {
      int topN = random().nextBoolean() ? dimValues[dim].length : TestUtil.nextInt(random(), 1, dimValues[dim].length);
      FacetResult fr = actual.facets.getTopChildren(topN, "dim" + dim);
      if (VERBOSE) {
        System.out.println("    dim" + dim + " topN=" + topN + " (vs " + dimValues[dim].length + " unique values)");
        System.out.println("      actual");
      }

      int idx = 0;
      Map<String, Integer> actualValues = new HashMap<>();

      if (fr != null) {
        for (LabelAndValue labelValue : fr.labelValues) {
          actualValues.put(labelValue.label, labelValue.value.intValue());
          if (VERBOSE) {
            System.out.println("        " + idx + ": " + new BytesRef(labelValue.label) + ": " + labelValue.value);
            idx++;
          }
        }
        assertEquals("dim=" + dim, expected.uniqueCounts[dim], fr.childCount);
      }

      if (topN < dimValues[dim].length) {
        int[] topNIDs = getTopNOrds(expected.counts[dim], dimValues[dim], topN);
        if (VERBOSE) {
          idx = 0;
          System.out.println("      expected (sorted)");
          for (int i = 0; i < topNIDs.length; i++) {
            int expectedOrd = topNIDs[i];
            String value = dimValues[dim][expectedOrd];
            System.out.println(
                    "        " + idx + ": " + new BytesRef(value) + ": " + expected.counts[dim][expectedOrd]);
            idx++;
          }
        }
        if (VERBOSE) {
          System.out.println("      topN=" + topN + " expectedTopN=" + topNIDs.length);
        }

        if (fr != null) {
          assertEquals(topNIDs.length, fr.labelValues.length);
        } else {
          assertEquals(0, topNIDs.length);
        }
        for (int i = 0; i < topNIDs.length; i++) {
          int expectedOrd = topNIDs[i];
          assertEquals(expected.counts[dim][expectedOrd], fr.labelValues[i].value.intValue());
          if (isSortedSetDV) {
            // Tie-break facet labels are only in unicode
            // order with SortedSetDVFacets:
            assertEquals("value @ idx=" + i, dimValues[dim][expectedOrd], fr.labelValues[i].label);
          }
        }
      } else {

        if (VERBOSE) {
          idx = 0;
          System.out.println("      expected (unsorted)");
          for (int i = 0; i < dimValues[dim].length; i++) {
            String value = dimValues[dim][i];
            if (expected.counts[dim][i] != 0) {
              System.out.println("        " + idx + ": " + new BytesRef(value) + ": " + expected.counts[dim][i]);
              idx++;
            }
          }
        }

        int setCount = 0;
        for (int i = 0; i < dimValues[dim].length; i++) {
          String value = dimValues[dim][i];
          if (expected.counts[dim][i] != 0) {
            assertTrue(actualValues.containsKey(value));
            assertEquals(expected.counts[dim][i], actualValues.get(value).intValue());
            setCount++;
          } else {
            assertFalse(actualValues.containsKey(value));
          }
        }
        assertEquals(setCount, actualValues.size());
      }
    }
  }

  public void testEmptyIndex() throws Exception {
    // LUCENE-5045: make sure DrillSideways works with an empty index
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
    IndexSearcher searcher = getNewSearcher(writer.getReader());
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    // Count "Author"
    FacetsConfig config = new FacetsConfig();
    DrillSideways ds = getNewDrillSideways(searcher, config, taxoReader);
    DrillDownQuery ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");

    DrillSidewaysResult r = ds.search(ddq, 10); // this used to fail on IllegalArgEx
    assertEquals(0, r.hits.totalHits.value);

    r = ds.search(ddq, null, null, 10, new Sort(new SortField("foo", SortField.Type.INT)), false); // this used to fail on IllegalArgEx
    assertEquals(0, r.hits.totalHits.value);

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, dir, taxoDir);
  }

  public void testNoDrillDownFacetCollection() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = getNewSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    // Sometimes pass an ExecutorService to test both paths for providing null drill down collectors
    // The actual configuration of the ExecutorService doesn't matter at all:
    ExecutorService executorService = null;
    if (random().nextBoolean()) {
      executorService = Executors.newSingleThreadExecutor(new NamedThreadFactory("ds_test"));
    }

    // Don't collect for drill down faceting
    DrillSideways ds =
        new DrillSideways(searcher, config, taxoReader, null, executorService) {
          @Override
          protected FacetsCollector createDrillDownFacetsCollector() {
            return null;
          }

          @Override
          protected FacetsCollectorManager createDrillDownFacetsCollectorManager() {
            return null;
          }
        };

    // Another simple case: drill-down on single fields
    // but OR of two values
    DrillDownQuery ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    ddq.add("Author", "Bob");
    DrillSidewaysResult r = ds.search(null, ddq, 10);
    Facets facets = r.facets;
    assertEquals(3, r.hits.totalHits.value);
    assertEquals(
        "dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
        facets.getTopChildren(10, "Author").toString());
    // Because we don't collect drill-downs, we shouldn't be able to get counts for Publish Date
    expectThrows(IllegalArgumentException.class, () -> facets.getTopChildren(10, "Publish Date"));

    assertTrue(facets instanceof MultiFacets);
    List<FacetResult> allResults = facets.getAllDims(10);
    // Should only have the one dimension because we didn't collect for drill down
    assertEquals(1, allResults.size());
    assertEquals(
        "dim=Author path=[] value=5 childCount=4\n  Lisa (2)\n  Bob (1)\n  Susan (1)\n  Frank (1)\n",
        allResults.get(0).toString());

    // More interesting case: drill-down on two fields
    ddq = new DrillDownQuery(config);
    ddq.add("Author", "Lisa");
    ddq.add("Publish Date", "2010");
    r = ds.search(null, ddq, 10);
    assertEquals(1, r.hits.totalHits.value);
    // Should be able to count on both fields since they're both drill sideways cases
    assertEquals(
        "dim=Publish Date path=[] value=2 childCount=2\n  2010 (1)\n  2012 (1)\n",
        r.facets.getTopChildren(10, "Publish Date").toString());
    assertEquals(
        "dim=Author path=[] value=2 childCount=2\n  Bob (1)\n  Lisa (1)\n",
        r.facets.getTopChildren(10, "Author").toString());

    if (executorService != null) {
      executorService.shutdown();
    }
    writer.close();
    IOUtils.close(searcher.getIndexReader(), taxoReader, taxoWriter, dir, taxoDir);
  }
  public void testScorer() throws Exception {
    // LUCENE-6001 some scorers, eg ReqExlScorer, can hit NPE if cost is called after nextDoc
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(newTextField("field", "foo bar", Field.Store.NO));
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("dim", "a"));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = getNewSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    DrillSideways ds = getNewDrillSideways(searcher, config, taxoReader);

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("field", "foo")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("field", "bar")), BooleanClause.Occur.MUST_NOT);
    DrillDownQuery ddq = new DrillDownQuery(config, bq.build());
    ddq.add("field", "foo");
    ddq.add("author", bq.build());
    ddq.add("dim", bq.build());
    DrillSidewaysResult r = ds.search(null, ddq, 10);
    assertEquals(0, r.hits.totalHits.value);

    writer.close();
    IOUtils.close(searcher.getIndexReader(), taxoReader, taxoWriter, dir, taxoDir);
  }

  public void testExtendedDrillSidewaysResult() throws Exception {
    // LUCENE-9945: Extend DrillSideways to support exposing FacetCollectors directly
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("dim", true);

    Document doc = new Document();
    doc.add(new FacetField("dim", "a"));
    writer.addDocument(config.build(taxoWriter, doc));

    Document doc2 = new Document();
    doc.add(new FacetField("dim", "x"));
    writer.addDocument(config.build(taxoWriter, doc2));

    // open NRT
    IndexSearcher searcher = getNewSearcher(writer.getReader());
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    DrillDownQuery ddq = new DrillDownQuery(config);
    ddq.add("dim", "x");

    DrillSideways ds = getNewDrillSidewaysBuildFacetsResult(searcher, config, taxoReader);

    SimpleCollectorManager manager =
        new SimpleCollectorManager(
            10, (a, b) -> Float.compare(b.docAndScore.score, a.docAndScore.score));
    SimpleCollector collector = manager.newCollector();

    // Sometimes pass in a Collector and sometimes CollectorManager
    // so that we can test both DrillSidewaysResult and ConcurrentDrillSidewaysResult
    DrillSidewaysResult r;
    if (random().nextBoolean()) {
      r = ds.search(ddq, collector);
    } else {
      r = ds.search(ddq, manager);
    }

    // compute Facets using exposed FacetCollectors from DrillSidewaysResult
    Map<String, Facets> drillSidewaysFacets = new HashMap<>();
    Facets drillDownFacets = getTaxonomyFacetCounts(taxoReader, config, r.drillDownFacetsCollector);
    if (r.drillSidewaysFacetsCollector != null) {
      for (int i = 0; i < r.drillSidewaysFacetsCollector.length; i++) {
        drillSidewaysFacets.put(
            r.drillSidewaysDims[i],
            getTaxonomyFacetCounts(taxoReader, config, r.drillSidewaysFacetsCollector[i]));
      }
    }

    Facets facets;
    if (drillSidewaysFacets.isEmpty()) {
      facets = drillDownFacets;
    } else {
      facets = new MultiFacets(drillSidewaysFacets, drillDownFacets);
    }

    // Facets computed using FacetsCollecter exposed in DrillSidewaysResult
    // should match the Facets computed by {@link DrillSideways#buildFacetsResult}
    FacetResult facetResultActual = facets.getTopChildren(2, "dim");
    FacetResult facetResultExpected = r.facets.getTopChildren(2, "dim");

    assertEquals(facetResultExpected.dim, facetResultActual.dim);
    assertEquals(facetResultExpected.path.length, facetResultActual.path.length);
    assertEquals(facetResultExpected.value, facetResultActual.value);
    assertEquals(facetResultExpected.childCount, facetResultActual.childCount);

    writer.close();
    IOUtils.close(searcher.getIndexReader(), taxoReader, taxoWriter, dir, taxoDir);
  }
}

