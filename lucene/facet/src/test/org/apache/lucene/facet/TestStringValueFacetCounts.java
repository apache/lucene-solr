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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestStringValueFacetCounts extends FacetTestCase {

  public void testBasicSingleValued() throws Exception {

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("foo")));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("foo")));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("bar")));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("bar")));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("baz")));
    writer.addDocument(doc);

    Map<String, Integer> expectedCounts = new HashMap<>();
    expectedCounts.put("foo", 2);
    expectedCounts.put("bar", 2);
    expectedCounts.put("baz", 1);
    int expectedTotalDocCount = 5;

    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    checkFacetResult(expectedCounts, expectedTotalDocCount, searcher, 10, 2, 1, 0);

    IOUtils.close(searcher.getIndexReader(), dir);
  }

  // See: LUCENE-10070
  public void testCountAll() throws Exception {

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    doc.add(new SortedSetDocValuesField("field", new BytesRef("foo")));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.NO));
    doc.add(new SortedSetDocValuesField("field", new BytesRef("foo")));
    writer.addDocument(doc);

    writer.deleteDocuments(new Term("id", "0"));

    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    StringDocValuesReaderState state =
        new StringDocValuesReaderState(searcher.getIndexReader(), "field");

    StringValueFacetCounts facets = new StringValueFacetCounts(state);
    assertEquals(
        "dim=field path=[] value=1 childCount=1\n  foo (1)",
        facets.getTopChildren(10, "field").toString().trim());

    IOUtils.close(searcher.getIndexReader(), dir);
  }

  public void testBasicSingleValuedUsingSortedDoc() throws Exception {

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new SortedDocValuesField("field", new BytesRef("foo")));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new SortedDocValuesField("field", new BytesRef("foo")));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new SortedDocValuesField("field", new BytesRef("bar")));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new SortedDocValuesField("field", new BytesRef("bar")));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new SortedDocValuesField("field", new BytesRef("baz")));
    writer.addDocument(doc);

    Map<String, Integer> expectedCounts = new HashMap<>();
    expectedCounts.put("foo", 2);
    expectedCounts.put("bar", 2);
    expectedCounts.put("baz", 1);
    int expectedTotalDocCount = 5;

    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    checkFacetResult(expectedCounts, expectedTotalDocCount, searcher, 10, 2, 1, 0);

    IOUtils.close(searcher.getIndexReader(), dir);
  }

  public void testBasicMultiValued() throws Exception {

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("foo")));
    doc.add(new SortedSetDocValuesField("field", new BytesRef("bar")));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("foo")));
    doc.add(new SortedSetDocValuesField("field", new BytesRef("baz")));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("baz")));
    writer.addDocument(doc);

    Map<String, Integer> expectedCounts = new HashMap<>();
    expectedCounts.put("foo", 2);
    expectedCounts.put("bar", 1);
    expectedCounts.put("baz", 2);
    int expectedTotalDocCount = 3;

    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    checkFacetResult(expectedCounts, expectedTotalDocCount, searcher, 10, 2, 1, 0);

    IOUtils.close(searcher.getIndexReader(), dir);
  }

  public void testSparseMultiSegmentCase() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Map<String, Integer> expectedCounts = new HashMap<>();

    // Create two segments, each with only one doc that has a large number of SSDV field values.
    // This ensures "sparse" counting will occur in StringValueFacetCounts (i.e., small number
    // of hits relative to the field cardinality):
    Document doc = new Document();
    for (int i = 0; i < 100; i++) {
      doc.add(new SortedSetDocValuesField("field", new BytesRef("foo_" + i)));
      expectedCounts.put("foo_" + i, 1);
    }
    writer.addDocument(doc);
    writer.commit();

    doc = new Document();
    for (int i = 0; i < 100; i++) {
      doc.add(new SortedSetDocValuesField("field", new BytesRef("bar_" + i)));
      expectedCounts.put("bar_" + i, 1);
    }
    writer.addDocument(doc);

    int expectedTotalDocCount = 2;

    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    checkFacetResult(expectedCounts, expectedTotalDocCount, searcher, 10, 2, 1, 0);

    IOUtils.close(searcher.getIndexReader(), dir);
  }

  public void testMissingSegment() throws Exception {

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("foo")));
    doc.add(new SortedSetDocValuesField("field", new BytesRef("bar")));
    writer.addDocument(doc);
    writer.commit();

    // segment with no values
    doc = new Document();
    writer.addDocument(doc);
    writer.commit();

    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("baz")));
    writer.addDocument(doc);
    writer.commit();

    Map<String, Integer> expectedCounts = new HashMap<>();
    expectedCounts.put("foo", 1);
    expectedCounts.put("bar", 1);
    expectedCounts.put("baz", 1);
    int expectedTotalDocCount = 2;

    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    checkFacetResult(expectedCounts, expectedTotalDocCount, searcher, 10, 2, 1, 0);

    IOUtils.close(searcher.getIndexReader(), dir);
  }

  public void testStaleState() throws Exception {

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("foo")));
    writer.addDocument(doc);

    IndexReader reader = writer.getReader();
    StringDocValuesReaderState state = new StringDocValuesReaderState(reader, "field");

    doc = new Document();
    doc.add(new SortedSetDocValuesField("field", new BytesRef("bar")));
    writer.addDocument(doc);

    IndexSearcher searcher = newSearcher(writer.getReader());
    writer.close();

    FacetsCollector c = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);

    // using a stale state
    expectThrows(IllegalStateException.class, () -> new StringValueFacetCounts(state, c));

    IOUtils.close(reader, searcher.getIndexReader(), dir);
  }

  public void testRandom() throws Exception {

    int fullIterations = LuceneTestCase.TEST_NIGHTLY ? 20 : 3;
    for (int iter = 0; iter < fullIterations; iter++) {
      Directory dir = newDirectory();
      RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

      // Build up test data
      String[] tokens = getRandomTokens(50); // 50 random values to pick from
      int numDocs = atLeast(1000);
      int expectedTotalDocCount = 0;
      Map<String, Integer> expected = new HashMap<>();
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        // Sometimes we restrict docs to be single-valued, but most of the time they can have up to
        // 5:
        int maxValuesPerDoc;
        if (random().nextInt(10) < 8) {
          maxValuesPerDoc = 5;
        } else {
          maxValuesPerDoc = 1;
        }
        int valCount = random().nextInt(maxValuesPerDoc);
        Set<String> docVals = new HashSet<>();
        for (int j = 0; j < valCount; j++) {
          int tokenIdx = random().nextInt(tokens.length);
          String val = tokens[tokenIdx];
          // values should only be counted once per document
          if (docVals.contains(val) == false) {
            expected.put(val, expected.getOrDefault(val, 0) + 1);
          }
          docVals.add(val);
          doc.add(new SortedSetDocValuesField("field", new BytesRef(val)));
        }
        // only docs with at least one value in the field should be counted in the total
        if (docVals.isEmpty() == false) {
          expectedTotalDocCount++;
        }
        writer.addDocument(doc);
        if (random().nextInt(10) == 0) {
          writer.commit(); // sometimes commit
        }
      }

      IndexSearcher searcher = newSearcher(writer.getReader());
      writer.close();

      // run iterations with random values of topN
      int iterations = LuceneTestCase.TEST_NIGHTLY ? 10_000 : 50;
      int[] topNs = new int[iterations];
      for (int i = 0; i < iterations; i++) {
        topNs[i] = atLeast(1);
      }

      checkFacetResult(expected, expectedTotalDocCount, searcher, topNs);

      IOUtils.close(searcher.getIndexReader(), dir);
    }
  }

  private void checkFacetResult(
      Map<String, Integer> expectedCounts,
      int expectedTotalDocsWithValue,
      IndexSearcher searcher,
      int... topNs)
      throws IOException {

    StringDocValuesReaderState state =
        new StringDocValuesReaderState(searcher.getIndexReader(), "field");

    FacetsCollector c = new FacetsCollector();
    searcher.search(new MatchAllDocsQuery(), c);

    for (int topN : topNs) {

      StringValueFacetCounts facets;
      // should get the same result whether-or-not we provide a FacetsCollector since it's doing
      // a MatchAllDocsQuery:
      if (random().nextBoolean()) {
        facets = new StringValueFacetCounts(state, c);
      } else {
        facets = new StringValueFacetCounts(state);
      }

      // sort expected counts by count, value
      List<Map.Entry<String, Integer>> expectedCountsSorted =
          new ArrayList<>(expectedCounts.entrySet());
      expectedCountsSorted.sort(
          (a, b) -> {
            int cmp = b.getValue().compareTo(a.getValue()); // high-to-low
            if (cmp == 0) {
              cmp = a.getKey().compareTo(b.getKey()); // low-to-high
            }
            return cmp;
          });

      // number of labels we expect is the number with a non-zero count
      int expectedLabelCount =
          (int) expectedCountsSorted.stream().filter(e -> e.getValue() > 0).count();

      // topN == 0 is intentionally unsupported
      if (topN == 0) {
        assertThrows(IllegalArgumentException.class, () -> facets.getTopChildren(topN, "field"));
        return;
      }

      FacetResult facetResult = facets.getTopChildren(topN, "field");

      assertEquals("field", facetResult.dim);
      assertEquals(0, facetResult.path.length);
      assertEquals(expectedTotalDocsWithValue, facetResult.value);
      assertEquals(expectedLabelCount, facetResult.childCount);

      // getAllDims should return a singleton list with the same results as getTopChildren
      List<FacetResult> allDims = facets.getAllDims(topN);
      assertEquals(1, allDims.size());
      assertEquals(facetResult, allDims.get(0));

      // This is a little strange, but we request all labels at this point so that when we
      // secondarily sort by label value in order to compare to the expected results, we have
      // all the values. See LUCENE-9991:
      int maxTopN = expectedCountsSorted.size();
      facetResult = facets.getTopChildren(maxTopN, "field");

      // also sort expected labels by count, value (these will be sorted by count, ord -- but since
      // we have no insight into the ordinals assigned to the values, we resort)
      Arrays.sort(
          facetResult.labelValues,
          (a, b) -> {
            int cmp = Long.compare(b.value.longValue(), a.value.longValue()); // high-to-low
            if (cmp == 0) {
              cmp = a.label.compareTo(b.label); // low-to-high
            }
            return cmp;
          });

      for (int i = 0; i < Math.min(topN, maxTopN); i++) {
        String expectedKey = expectedCountsSorted.get(i).getKey();
        int expectedValue = expectedCountsSorted.get(i).getValue();
        assertEquals(expectedKey, facetResult.labelValues[i].label);
        assertEquals(expectedValue, facetResult.labelValues[i].value);
        // make sure getSpecificValue reports the same count
        assertEquals(expectedValue, facets.getSpecificValue("field", expectedKey));
      }

      // execute a "drill down" query on one of the values at random and make sure the total hits
      // match the expected count provided by faceting
      if (expectedCountsSorted.isEmpty() == false) {
        DrillDownQuery q = new DrillDownQuery(new FacetsConfig());
        int randomTestValIdx = random().nextInt(Math.min(expectedCountsSorted.size(), topN));
        q.add("field", expectedCountsSorted.get(randomTestValIdx).getKey());
        searcher.search(q, 1);
        assertEquals(
            expectedCountsSorted.get(randomTestValIdx).getValue(),
            facetResult.labelValues[randomTestValIdx].value);
      }
    }
  }
}
