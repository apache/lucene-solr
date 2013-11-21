package org.apache.lucene.facet.simple;

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

import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

public class TestSortedSetDocValuesFacets extends FacetTestCase {

  // NOTE: TestDrillSideways.testRandom also sometimes
  // randomly uses SortedSetDV

  public void testBasic() throws Exception {
    System.out.println("here: " + defaultCodecSupportsSortedSet());
    assumeTrue("Test requires SortedSetDV support", defaultCodecSupportsSortedSet());
    Directory dir = newDirectory();

    FacetsConfig config = new FacetsConfig();
    config.setMultiValued("a", true);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DocumentBuilder builder = new DocumentBuilder(null, config);

    Document doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo"));
    doc.add(new SortedSetDocValuesFacetField("a", "bar"));
    doc.add(new SortedSetDocValuesFacetField("a", "zoo"));
    doc.add(new SortedSetDocValuesFacetField("b", "baz"));
    System.out.println("TEST: now add");
    writer.addDocument(builder.build(doc));
    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo"));
    writer.addDocument(builder.build(doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    // Per-top-reader state:
    SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(searcher.getIndexReader());
    
    SimpleFacetsCollector c = new SimpleFacetsCollector();

    searcher.search(new MatchAllDocsQuery(), c);

    SortedSetDocValuesFacetCounts facets = new SortedSetDocValuesFacetCounts(state, c);

    assertEquals("a (4)\n  foo (2)\n  bar (1)\n  zoo (1)\n", facets.getTopChildren(10, "a").toString());
    assertEquals("b (1)\n  baz (1)\n", facets.getTopChildren(10, "b").toString());

    // DrillDown:
    SimpleDrillDownQuery q = new SimpleDrillDownQuery(config);
    q.add("a", "foo");
    q.add("b", "baz");
    TopDocs hits = searcher.search(q, 1);
    assertEquals(1, hits.totalHits);

    searcher.getIndexReader().close();
    dir.close();
  }

  // LUCENE-5090
  public void testStaleState() throws Exception {
    assumeTrue("Test requires SortedSetDV support", defaultCodecSupportsSortedSet());
    Directory dir = newDirectory();

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DocumentBuilder builder = new DocumentBuilder(null, new FacetsConfig());

    Document doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo"));
    writer.addDocument(builder.build(doc));

    IndexReader r = writer.getReader();
    SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(r);

    doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "bar"));
    writer.addDocument(builder.build(doc));

    doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "baz"));
    writer.addDocument(builder.build(doc));

    IndexSearcher searcher = newSearcher(writer.getReader());

    SimpleFacetsCollector c = new SimpleFacetsCollector();

    searcher.search(new MatchAllDocsQuery(), c);

    try {
      new SortedSetDocValuesFacetCounts(state, c);
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }

    r.close();
    writer.close();
    searcher.getIndexReader().close();
    dir.close();
  }

  // LUCENE-5333
  public void testSparseFacets() throws Exception {
    assumeTrue("Test requires SortedSetDV support", defaultCodecSupportsSortedSet());
    Directory dir = newDirectory();

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DocumentBuilder builder = new DocumentBuilder(null, new FacetsConfig());

    Document doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo1"));
    writer.addDocument(builder.build(doc));

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo2"));
    doc.add(new SortedSetDocValuesFacetField("b", "bar1"));
    writer.addDocument(builder.build(doc));

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo3"));
    doc.add(new SortedSetDocValuesFacetField("b", "bar2"));
    doc.add(new SortedSetDocValuesFacetField("c", "baz1"));
    writer.addDocument(builder.build(doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    // Per-top-reader state:
    SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(searcher.getIndexReader());

    SimpleFacetsCollector c = new SimpleFacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);    
    SortedSetDocValuesFacetCounts facets = new SortedSetDocValuesFacetCounts(state, c);

    // Ask for top 10 labels for any dims that have counts:
    List<SimpleFacetResult> results = facets.getAllDims(10);

    assertEquals(3, results.size());
    assertEquals("a (3)\n  foo1 (1)\n  foo2 (1)\n  foo3 (1)\n", results.get(0).toString());
    assertEquals("b (2)\n  bar1 (1)\n  bar2 (1)\n", results.get(1).toString());
    assertEquals("c (1)\n  baz1 (1)\n", results.get(2).toString());

    searcher.getIndexReader().close();
    dir.close();
  }

  // nocommit test different delim char & using the default
  // one in a dim

  // nocommit in the sparse case test that we are really
  // sorting by the correct dim count

  public void testSlowCompositeReaderWrapper() throws Exception {
    assumeTrue("Test requires SortedSetDV support", defaultCodecSupportsSortedSet());
    Directory dir = newDirectory();

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    DocumentBuilder builder = new DocumentBuilder(null, new FacetsConfig());

    Document doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo1"));
    writer.addDocument(builder.build(doc));

    writer.commit();

    doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo2"));
    writer.addDocument(builder.build(doc));

    // NRT open
    IndexSearcher searcher = new IndexSearcher(SlowCompositeReaderWrapper.wrap(writer.getReader()));

    // Per-top-reader state:
    SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(searcher.getIndexReader());

    SimpleFacetsCollector c = new SimpleFacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);    
    Facets facets = new SortedSetDocValuesFacetCounts(state, c);

    // Ask for top 10 labels for any dims that have counts:
    assertEquals("a (2)\n  foo1 (1)\n  foo2 (1)\n", facets.getTopChildren(10, "a").toString());

    IOUtils.close(writer, searcher.getIndexReader(), dir);
  }
}
