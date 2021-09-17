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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** Tests long value facets. */
public class TestLongValueFacetCounts extends LuceneTestCase {

  public void testBasic() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    for (long l = 0; l < 100; l++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("field", l % 5));
      w.addDocument(doc);
    }

    // Also add Long.MAX_VALUE
    Document doc = new Document();
    doc.add(new NumericDocValuesField("field", Long.MAX_VALUE));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    LongValueFacetCounts facets = new LongValueFacetCounts("field", fc, false);

    FacetResult result = facets.getAllChildrenSortByValue();
    assertEquals("dim=field path=[] value=101 childCount=6\n  0 (20)\n  1 (20)\n  2 (20)\n  3 (20)\n  " +
                 "4 (20)\n  9223372036854775807 (1)\n",
                 result.toString());
    r.close();
    d.close();
  }

  // See: LUCENE-10070
  public void testCountAll() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", String.valueOf(i), Field.Store.NO));
      doc.add(new NumericDocValuesField("field", i % 2));
      w.addDocument(doc);
    }

    w.deleteDocuments(new Term("id", "0"));

    IndexReader r = w.getReader();
    w.close();

    LongValueFacetCounts facets = new LongValueFacetCounts("field", r, false);

    FacetResult result = facets.getAllChildrenSortByValue();
    assertEquals("dim=field path=[] value=9 childCount=2\n  0 (4)\n  1 (5)\n", result.toString());
    r.close();
    d.close();
  }

  public void testOnlyBigLongs() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    for (long l = 0; l < 3; l++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("field", Long.MAX_VALUE - l));
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    LongValueFacetCounts facets = new LongValueFacetCounts("field", fc, false);

    FacetResult result = facets.getAllChildrenSortByValue();
    assertEquals("dim=field path=[] value=3 childCount=3\n  9223372036854775805 (1)\n  " +
                 "9223372036854775806 (1)\n  9223372036854775807 (1)\n",
                 result.toString());
    r.close();
    d.close();
  }

  public void testGetAllDims() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);
    for (long l = 0; l < 100; l++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("field", l % 5));
      w.addDocument(doc);
    }

    // Also add Long.MAX_VALUE
    Document doc = new Document();
    doc.add(new NumericDocValuesField("field", Long.MAX_VALUE));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    FacetsCollector fc = new FacetsCollector();
    IndexSearcher s = newSearcher(r);
    s.search(new MatchAllDocsQuery(), fc);

    Facets facets = new LongValueFacetCounts("field", fc, false);

    List<FacetResult> result = facets.getAllDims(10);
    assertEquals(1, result.size());
    assertEquals("dim=field path=[] value=101 childCount=6\n  0 (20)\n  1 (20)\n  2 (20)\n  " +
                 "3 (20)\n  4 (20)\n  9223372036854775807 (1)\n",
                 result.get(0).toString());
    r.close();
    d.close();
  }

  public void testRandomSingleValued() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int docCount = atLeast(1000);
    double missingChance = random().nextDouble();
    long maxValue;
    if (random().nextBoolean()) {
      maxValue = random().nextLong() & Long.MAX_VALUE;
    } else {
      maxValue = random().nextInt(1000);
    }
    if (VERBOSE) {
      System.out.println("TEST: valueCount=" + docCount + " valueRange=-" + maxValue +
                         "-" + maxValue + " missingChance=" + missingChance);
    }
    Long[] values = new Long[docCount];
    int missingCount = 0;
    for (int i = 0; i < docCount; i++) {
      Document doc = new Document();
      doc.add(new IntPoint("id", i));
      if (random().nextDouble() > missingChance) {
        long value = TestUtil.nextLong(random(), -maxValue, maxValue);
        doc.add(new NumericDocValuesField("field", value));
        values[i] = value;
      } else {
        missingCount++;
      }
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);

    int iters = atLeast(100);
    for (int iter = 0; iter < iters; iter++) {
      FacetsCollector fc = new FacetsCollector();
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
        System.out.println("  test all docs");
      }

      // all docs
      Map<Long, Integer> expected = new HashMap<>();
      int expectedChildCount = 0;
      for (int i = 0; i < docCount; i++) {
        if (values[i] != null) {
          Integer curCount = expected.get(values[i]);
          if (curCount == null) {
            curCount = 0;
            expectedChildCount++;
          }
          expected.put(values[i], curCount + 1);
        }
      }

      List<Map.Entry<Long, Integer>> expectedCounts = new ArrayList<>(expected.entrySet());

      // sort by value
      Collections.sort(expectedCounts,
                       (a, b) -> (Long.compare(a.getKey(), b.getKey())));

      LongValueFacetCounts facetCounts;
      if (random().nextBoolean()) {
        s.search(new MatchAllDocsQuery(), fc);
        if (random().nextBoolean()) {
          if (VERBOSE) {
            System.out.println("  use value source");
          }
          facetCounts = new LongValueFacetCounts("field", LongValuesSource.fromLongField("field"), fc);
        } else {
          if (VERBOSE) {
            System.out.println("  use doc values");
          }
          facetCounts = new LongValueFacetCounts("field", fc, false);
        }
      } else {
        // optimized count all:
        if (random().nextBoolean()) {
          if (VERBOSE) {
            System.out.println("  count all value source");
          }
          facetCounts = new LongValueFacetCounts("field", LongValuesSource.fromLongField("field"), r);
        } else {
          if (VERBOSE) {
            System.out.println("  count all doc values");
          }
          facetCounts = new LongValueFacetCounts("field", r, false);
        }          
      }

      FacetResult actual = facetCounts.getAllChildrenSortByValue();
      assertSame("all docs, sort facets by value", expectedCounts, expectedChildCount,
          docCount - missingCount, actual, Integer.MAX_VALUE);

      // sort by count
      Collections.sort(expectedCounts,
                       (a, b) -> {
                         int cmp = -Integer.compare(a.getValue(), b.getValue());
                         if (cmp == 0) {
                           // tie break by value
                           cmp = Long.compare(a.getKey(), b.getKey());
                         }
                         return cmp;
                       });
      int topN;
      if (random().nextBoolean()) {
        topN = docCount;
      } else {
        topN = random().nextInt(docCount);
      }
      if (VERBOSE) {
        System.out.println("  topN=" + topN);
      }
      actual = facetCounts.getTopChildrenSortByCount(topN);
      assertSame("all docs, sort facets by count", expectedCounts, expectedChildCount, docCount - missingCount, actual, topN);

      // subset of docs
      int minId = random().nextInt(docCount);
      int maxId = random().nextInt(docCount);
      if (minId > maxId) {
        int tmp = minId;
        minId = maxId;
        maxId = tmp;
      }
      if (VERBOSE) {
        System.out.println("  test id range " + minId + "-" + maxId);
      }

      fc = new FacetsCollector();
      s.search(IntPoint.newRangeQuery("id", minId, maxId), fc);
      if (random().nextBoolean()) {
        if (VERBOSE) {
          System.out.println("  use doc values");
        }
        facetCounts = new LongValueFacetCounts("field", fc, false);
      } else {
        if (VERBOSE) {
          System.out.println("  use value source");
        }
        facetCounts = new LongValueFacetCounts("field", LongValuesSource.fromLongField("field"), fc);
      }

      expected = new HashMap<>();
      expectedChildCount = 0;
      int totCount = 0;
      for (int i = minId; i <= maxId; i++) {
        if (values[i] != null) {
          totCount++;
          Integer curCount = expected.get(values[i]);
          if (curCount == null) {
            expectedChildCount++;
            curCount = 0;
          }
          expected.put(values[i], curCount + 1);
        }
      }
      expectedCounts = new ArrayList<>(expected.entrySet());

      // sort by value
      Collections.sort(expectedCounts,
                       (a, b) -> (Long.compare(a.getKey(), b.getKey())));
      actual = facetCounts.getAllChildrenSortByValue();
      assertSame("id " + minId + "-" + maxId + ", sort facets by value", expectedCounts,
                 expectedChildCount, totCount, actual, Integer.MAX_VALUE);

      // sort by count
      Collections.sort(expectedCounts,
                       (a, b) -> {
                         int cmp = -Integer.compare(a.getValue(), b.getValue());
                         if (cmp == 0) {
                           // tie break by value
                           cmp = Long.compare(a.getKey(), b.getKey());
                         }
                         return cmp;
                       });
      if (random().nextBoolean()) {
        topN = docCount;
      } else {
        topN = random().nextInt(docCount);
      }
      actual = facetCounts.getTopChildrenSortByCount(topN);
      assertSame("id " + minId + "-" + maxId + ", sort facets by count", expectedCounts, expectedChildCount, totCount, actual, topN);
    }
    r.close();
    dir.close();
  }

  public void testRandomMultiValued() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int docCount = atLeast(1000);
    double missingChance = random().nextDouble();

    // sometimes exercise codec optimizations when a claimed multi valued field is in fact single valued:
    boolean allSingleValued = rarely();
    long maxValue;

    if (random().nextBoolean()) {
      maxValue = random().nextLong() & Long.MAX_VALUE;
    } else {
      maxValue = random().nextInt(1000);
    }
    if (VERBOSE) {
      System.out.println("TEST: valueCount=" + docCount + " valueRange=-" + maxValue +
                         "-" + maxValue + " missingChance=" + missingChance + " allSingleValued=" + allSingleValued);
    }
    
    long[][] values = new long[docCount][];
    int missingCount = 0;
    for (int i = 0; i < docCount; i++) {
      Document doc = new Document();
      doc.add(new IntPoint("id", i));
      if (random().nextDouble() > missingChance) {
        if (allSingleValued) {
          values[i] = new long[1];
        } else {
          values[i] = new long[TestUtil.nextInt(random(), 1, 5)];
        }
        
        for (int j = 0; j < values[i].length; j++) {
          long value = TestUtil.nextLong(random(), -maxValue, maxValue);
          values[i][j] = value;
          doc.add(new SortedNumericDocValuesField("field", value));
        }

        if (VERBOSE) {
          System.out.println("  doc=" + i + " values=" + Arrays.toString(values[i]));
        }

        // sort values to enable duplicate detection by comparing with the previous value
        Arrays.sort(values[i]);
      } else {
        missingCount++;

        if (VERBOSE) {
          System.out.println("  doc=" + i + " missing values");
        }
      }
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);

    int iters = atLeast(100);
    for (int iter = 0; iter < iters; iter++) {
      FacetsCollector fc = new FacetsCollector();
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
        System.out.println("  test all docs");
      }

      // all docs
      Map<Long, Integer> expected = new HashMap<>();
      int expectedTotalCount = 0;
      for (int i = 0; i < docCount; i++) {
        if (values[i] != null && values[i].length > 0) {
          expectedTotalCount++;
          setExpectedFrequencies(values[i], expected);
        }
      }

      List<Map.Entry<Long, Integer>> expectedCounts = new ArrayList<>(expected.entrySet());
      int expectedChildCount = expected.size();

      // sort by value
      Collections.sort(expectedCounts,
                       (a, b) -> (Long.compare(a.getKey(), b.getKey())));

      LongValueFacetCounts facetCounts;
      if (random().nextBoolean()) {
        s.search(new MatchAllDocsQuery(), fc);
        if (VERBOSE) {
          System.out.println("  use doc values");
        }
        facetCounts = new LongValueFacetCounts("field", fc, true);
      } else {
        // optimized count all:
        if (VERBOSE) {
          System.out.println("  count all doc values");
        }
        facetCounts = new LongValueFacetCounts("field", r, true);
      }

      FacetResult actual = facetCounts.getAllChildrenSortByValue();
      assertSame("all docs, sort facets by value", expectedCounts, expectedChildCount,
                 expectedTotalCount, actual, Integer.MAX_VALUE);

      // sort by count
      Collections.sort(expectedCounts,
                       (a, b) -> {
                         int cmp = -Integer.compare(a.getValue(), b.getValue());
                         if (cmp == 0) {
                           // tie break by value
                           cmp = Long.compare(a.getKey(), b.getKey());
                         }
                         return cmp;
                       });
      int topN;
      if (random().nextBoolean()) {
        topN = docCount;
      } else {
        topN = random().nextInt(docCount);
      }
      if (VERBOSE) {
        System.out.println("  topN=" + topN);
      }
      actual = facetCounts.getTopChildrenSortByCount(topN);
      assertSame("all docs, sort facets by count", expectedCounts, expectedChildCount, expectedTotalCount, actual, topN);

      // subset of docs
      int minId = random().nextInt(docCount);
      int maxId = random().nextInt(docCount);
      if (minId > maxId) {
        int tmp = minId;
        minId = maxId;
        maxId = tmp;
      }
      if (VERBOSE) {
        System.out.println("  test id range " + minId + "-" + maxId);
      }

      fc = new FacetsCollector();
      s.search(IntPoint.newRangeQuery("id", minId, maxId), fc);
      // cannot use value source here because we are multi valued
      facetCounts = new LongValueFacetCounts("field", fc, true);

      expected = new HashMap<>();
      expectedTotalCount = 0;
      for (int i = minId; i <= maxId; i++) {
        if (values[i] != null && values[i].length > 0) {
          expectedTotalCount++;
          setExpectedFrequencies(values[i], expected);
        }
      }
      expectedCounts = new ArrayList<>(expected.entrySet());
      expectedChildCount = expected.size();

      // sort by value
      Collections.sort(expectedCounts,
                       (a, b) -> (Long.compare(a.getKey(), b.getKey())));
      actual = facetCounts.getAllChildrenSortByValue();
      assertSame("id " + minId + "-" + maxId + ", sort facets by value", expectedCounts,
                 expectedChildCount, expectedTotalCount, actual, Integer.MAX_VALUE);

      // sort by count
      Collections.sort(expectedCounts,
                       (a, b) -> {
                         int cmp = -Integer.compare(a.getValue(), b.getValue());
                         if (cmp == 0) {
                           // tie break by value
                           cmp = Long.compare(a.getKey(), b.getKey());
                         }
                         return cmp;
                       });
      if (random().nextBoolean()) {
        topN = docCount;
      } else {
        topN = random().nextInt(docCount);
      }
      actual = facetCounts.getTopChildrenSortByCount(topN);
      assertSame("id " + minId + "-" + maxId + ", sort facets by count", expectedCounts, expectedChildCount, expectedTotalCount, actual, topN);
    }
    r.close();
    dir.close();
  }

  private void setExpectedFrequencies(long[] values, Map<Long, Integer> expected) {
    long previousValue = 0;
    for (int j = 0; j < values.length; j++) {
      if (j == 0 || previousValue != values[j]) {
        Integer curCount = expected.getOrDefault(values[j], 0);
        expected.put(values[j], curCount + 1);
      }
      previousValue = values[j];
    }
  }

  private static void assertSame(String desc, List<Map.Entry<Long, Integer>> expectedCounts,
                                 int expectedChildCount, int expectedTotalCount, FacetResult actual, int topN) {
    int expectedTopN = Math.min(topN, expectedCounts.size());
    if (VERBOSE) {
      System.out.println("  expected topN=" + expectedTopN);
      for (int i = 0; i < expectedTopN; i++) {
        System.out.println("    " + i + ": value=" + expectedCounts.get(i).getKey() + " count=" + expectedCounts.get(i).getValue());
      }
      System.out.println("  actual topN=" + actual.labelValues.length);
      for (int i = 0; i < actual.labelValues.length; i++) {
        System.out.println("    " + i + ": value=" + actual.labelValues[i].label + " count=" + actual.labelValues[i].value);
      }
    }
    assertEquals(desc + ": topN", expectedTopN, actual.labelValues.length);
    assertEquals(desc + ": childCount", expectedChildCount, actual.childCount);
    assertEquals(desc + ": totCount", expectedTotalCount, actual.value.intValue());
    assertTrue(actual.labelValues.length <= topN);

    for (int i = 0; i < expectedTopN; i++) {
      assertEquals(desc + ": label[" + i + "]", Long.toString(expectedCounts.get(i).getKey()), actual.labelValues[i].label);
      assertEquals(desc + ": counts[" + i + "]", expectedCounts.get(i).getValue().intValue(), actual.labelValues[i].value.intValue());
    }
  }

  /**
   * LUCENE-9964: Duplicate long values in a document field should only be counted once when using
   * SortedNumericDocValuesFields
   */
  public void testDuplicateLongValues() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    // these two values are not unique in a document
    doc.add(new SortedNumericDocValuesField("field", 42));
    doc.add(new SortedNumericDocValuesField("field", 42));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new SortedNumericDocValuesField("field", 43));
    doc.add(new SortedNumericDocValuesField("field", 43));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();
    LongValueFacetCounts facetCounts = new LongValueFacetCounts("field", r, true);

    FacetResult fr = facetCounts.getAllChildrenSortByValue();
    for (LabelAndValue labelAndValue : fr.labelValues) {
      assert labelAndValue.value.equals(1);
    }

    r.close();
    dir.close();
  }
}
