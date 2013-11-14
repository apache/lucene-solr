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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.simple.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.simple.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

public class TestSortedSetDocValuesFacets extends FacetTestCase {

  // NOTE: TestDrillSideways.testRandom also sometimes
  // randomly uses SortedSetDV

  public void testBasic() throws Exception {
    assumeTrue("Test requires SortedSetDV support", defaultCodecSupportsSortedSet());
    Directory dir = newDirectory();

    IndexWriter writer = new FacetIndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    Document doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo"));
    doc.add(new SortedSetDocValuesFacetField("a", "bar"));
    doc.add(new SortedSetDocValuesFacetField("a", "zoo"));
    doc.add(new SortedSetDocValuesFacetField("b", "baz"));
    writer.addDocument(doc);
    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo"));
    writer.addDocument(doc);

    // NRT open
    IndexSearcher searcher = newSearcher(DirectoryReader.open(writer, true));
    writer.close();

    // Per-top-reader state:
    SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(searcher.getIndexReader());
    
    SimpleFacetsCollector c = new SimpleFacetsCollector();

    searcher.search(new MatchAllDocsQuery(), c);

    SortedSetDocValuesFacetCounts facets = new SortedSetDocValuesFacetCounts(state, c);

    assertEquals("a (4)\n  foo (2)\n  bar (1)\n  zoo (1)\n", facets.getDim("a", 10).toString());
    assertEquals("b (1)\n  baz (1)\n", facets.getDim("b", 10).toString());

    // DrillDown:
    SimpleDrillDownQuery q = new SimpleDrillDownQuery();
    q.add(new CategoryPath("a", "foo"));
    q.add(new CategoryPath("b", "baz"));
    TopDocs hits = searcher.search(q, 1);
    assertEquals(1, hits.totalHits);

    searcher.getIndexReader().close();
    dir.close();
  }

  // LUCENE-5090
  public void testStaleState() throws Exception {
    assumeTrue("Test requires SortedSetDV support", defaultCodecSupportsSortedSet());
    Directory dir = newDirectory();

    IndexWriter writer = new FacetIndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    Document doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo"));
    writer.addDocument(doc);

    IndexReader r = DirectoryReader.open(writer, true);
    SortedSetDocValuesReaderState state = new SortedSetDocValuesReaderState(r);

    doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "bar"));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "baz"));
    writer.addDocument(doc);

    IndexSearcher searcher = newSearcher(DirectoryReader.open(writer, true));

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

    IndexWriter writer = new FacetIndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    Document doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo1"));
    writer.addDocument(doc);

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo2"));
    doc.add(new SortedSetDocValuesFacetField("b", "bar1"));
    writer.addDocument(doc);

    if (random().nextBoolean()) {
      writer.commit();
    }

    doc = new Document();
    doc.add(new SortedSetDocValuesFacetField("a", "foo3"));
    doc.add(new SortedSetDocValuesFacetField("b", "bar2"));
    doc.add(new SortedSetDocValuesFacetField("c", "baz1"));
    writer.addDocument(doc);

    // NRT open
    IndexSearcher searcher = newSearcher(DirectoryReader.open(writer, true));
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
}
