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
package org.apache.solr.schema;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.index.SlowCompositeReaderWrapper;
import org.apache.solr.schema.IndexSchema.DynamicField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrQueryParser;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.RefCounted;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/** Tests for PointField functionality */
public class TestPointFields extends SolrTestCaseJ4 {

  // long overflow can occur in some date calculations if gaps are too large, so we limit to a million years BC & AD.
  private static final long MIN_DATE_EPOCH_MILLIS = LocalDateTime.parse("-1000000-01-01T00:00:00").toInstant(ZoneOffset.ofHours(0)).toEpochMilli();
  private static final long MAX_DATE_EPOCH_MILLIS = LocalDateTime.parse("+1000000-01-01T00:00:00").toInstant(ZoneOffset.ofHours(0)).toEpochMilli();

  private static final String[] FIELD_SUFFIXES = new String[] {
      "", "_dv", "_mv", "_mv_dv", "_ni", "_ni_dv", "_ni_dv_ns", "_ni_dv_ns_mv", 
      "_ni_mv", "_ni_mv_dv", "_ni_ns", "_ni_ns_mv", "_dv_ns", "_ni_ns_dv", "_dv_ns_mv",
      "_smf", "_dv_smf", "_mv_smf", "_mv_dv_smf", "_ni_dv_smf", "_ni_mv_dv_smf",
      "_sml", "_dv_sml", "_mv_sml", "_mv_dv_sml", "_ni_dv_sml", "_ni_mv_dv_sml"
  };

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-point.xml");
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    clearIndex();
    assertU(commit());
    super.tearDown();
  }
  
  @Test
  public void testIntPointFieldExactQuery() throws Exception {
    doTestIntPointFieldExactQuery("number_p_i", false);
    doTestIntPointFieldExactQuery("number_p_i_mv", false);
    doTestIntPointFieldExactQuery("number_p_i_dv", false);
    doTestIntPointFieldExactQuery("number_p_i_mv_dv", false);
    doTestIntPointFieldExactQuery("number_p_i_ni_dv", false);
    doTestIntPointFieldExactQuery("number_p_i_ni_ns_dv", false);
    doTestIntPointFieldExactQuery("number_p_i_ni_mv_dv", false);
  }
  
  @Test
  public void testIntPointFieldNonSearchableExactQuery() throws Exception {
    doTestIntPointFieldExactQuery("number_p_i_ni", false, false);
    doTestIntPointFieldExactQuery("number_p_i_ni_ns", false, false);
  }
  
  @Test
  public void testIntPointFieldReturn() throws Exception {
    int numValues = 10 * RANDOM_MULTIPLIER;
    String[] ints = toStringArray(getRandomInts(numValues, false));
    testPointFieldReturn("number_p_i", "int", ints);
    testPointFieldReturn("number_p_i_dv_ns", "int", ints);
    testPointFieldReturn("number_p_i_ni", "int", ints);
  }
  
  @Test
  public void testIntPointFieldRangeQuery() throws Exception {
    doTestIntPointFieldRangeQuery("number_p_i", "int", false);
    doTestIntPointFieldRangeQuery("number_p_i_ni_ns_dv", "int", false);
    doTestIntPointFieldRangeQuery("number_p_i_dv", "int", false);
  }
  
  @Test
  public void testIntPointFieldNonSearchableRangeQuery() throws Exception {
    doTestPointFieldNonSearchableRangeQuery("number_p_i_ni", toStringArray(getRandomInts(1, false)));
    doTestPointFieldNonSearchableRangeQuery("number_p_i_ni_ns", toStringArray(getRandomInts(1, false)));
    int numValues = 2 * RANDOM_MULTIPLIER;
    doTestPointFieldNonSearchableRangeQuery("number_p_i_ni_ns_mv", toStringArray(getRandomInts(numValues, false)));
  }
  
  @Test
  public void testIntPointFieldSortAndFunction() throws Exception {

    final SortedSet<String> regexToTest = dynFieldRegexesForType(IntPointField.class);
    final List<String> sequential = Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    final List<Integer> randomInts = getRandomInts(10, false);
    final List<Integer> randomIntsMissing = getRandomInts(10, true);
    
    for (String r : Arrays.asList("*_p_i", "*_p_i_dv", "*_p_i_dv_ns", "*_p_i_ni_dv",
                                  "*_p_i_ni_dv_ns", "*_p_i_ni_ns_dv")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSort(field, sequential);
      doTestPointFieldSort(field, randomInts);
      doTestIntPointFunctionQuery(field, "int");
    }
    for (String r : Arrays.asList("*_p_i_smf", "*_p_i_dv_smf", "*_p_i_ni_dv_smf",
                                  "*_p_i_sml", "*_p_i_dv_sml", "*_p_i_ni_dv_sml")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSort(field, sequential);
      doTestPointFieldSort(field, randomIntsMissing);
      doTestIntPointFunctionQuery(field, "int");
    }
    
    for (String r : Arrays.asList("*_p_i_ni", "*_p_i_ni_ns")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSortError(field, "w/o docValues", toStringArray(getRandomInts(1, false)));
      doTestPointFieldFunctionQueryError(field, "w/o docValues", toStringArray(getRandomInts(1, false)));
    }
    
    for (String r : Arrays.asList("*_p_i_mv", "*_p_i_ni_mv", "*_p_i_ni_mv_dv", "*_p_i_ni_dv_ns_mv",
                                  "*_p_i_ni_ns_mv", "*_p_i_dv_ns_mv", "*_p_i_mv_dv",
                                  "*_p_i_mv_smf", "*_p_i_mv_dv_smf", "*_p_i_ni_mv_dv_smf",
                                  "*_p_i_mv_sml", "*_p_i_mv_dv_sml", "*_p_i_ni_mv_dv_sml")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSortError(field, "multivalued", toStringArray(getRandomInts(1, false)));
      int numValues = 2 * RANDOM_MULTIPLIER;
      doTestPointFieldSortError(field, "multivalued", toStringArray(getRandomInts(numValues, false)));
      doTestPointFieldFunctionQueryError(field, "multivalued", toStringArray(getRandomInts(1, false)));
      doTestPointFieldFunctionQueryError(field, "multivalued", toStringArray(getRandomInts(numValues, false)));
   }
    
    assertEquals("Missing types in the test", Collections.<String>emptySet(), regexToTest);
  }
  
  @Test
  public void testIntPointFieldFacetField() throws Exception {
    testPointFieldFacetField("number_p_i", "number_p_i_dv", getSequentialStringArrayWithInts(10));
    clearIndex();
    assertU(commit());
    testPointFieldFacetField("number_p_i", "number_p_i_dv", toStringArray(getRandomInts(10, false)));
  }

  @Test
  public void testIntPointFieldRangeFacet() throws Exception {
    String docValuesField = "number_p_i_dv";
    String nonDocValuesField = "number_p_i";
    int numValues = 10 * RANDOM_MULTIPLIER;
    int numBuckets = numValues / 2;
    List<Integer> values;
    List<Integer> sortedValues;
    int max;
    do {
      values = getRandomInts(numValues, false);
      sortedValues = values.stream().sorted().collect(Collectors.toList());
    } while ((max = sortedValues.get(sortedValues.size() - 1)) >= Integer.MAX_VALUE - numValues); // leave room for rounding 
    int min = sortedValues.get(0);
    int gap = (int)(((long)(max + numValues) - (long)min) / (long)numBuckets);
    int[] bucketCount = new int[numBuckets];
    int bucketNum = 0;
    int minBucketVal = min;
    for (Integer value : sortedValues) {
      while (((long)value - (long)minBucketVal) >= (long)gap) {
        ++bucketNum;
        minBucketVal += gap;
      }
      ++bucketCount[bucketNum];
    }

    for (int i = 0 ; i < numValues ; i++) {
      assertU(adoc("id", String.valueOf(i), docValuesField, String.valueOf(values.get(i)), nonDocValuesField, String.valueOf(values.get(i))));
    }
    assertU(commit());

    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    String[] testStrings = new String[numBuckets + 1];
    testStrings[numBuckets] = "//*[@numFound='" + numValues + "']";
    minBucketVal = min;
    for (int i = 0 ; i < numBuckets ; minBucketVal += gap, ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField
          + "']/lst[@name='counts']/int[@name='" + minBucketVal + "'][.='" + bucketCount[i] + "']";
    }
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", String.valueOf(min),
        "facet.range.end", String.valueOf(max), "facet.range.gap", String.valueOf(gap)),
        testStrings);
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", String.valueOf(min),
        "facet.range.end", String.valueOf(max), "facet.range.gap", String.valueOf(gap), "facet.range.method", "dv"),
        testStrings);

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    minBucketVal = min;
    for (int i = 0 ; i < numBuckets ; minBucketVal += gap, ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField
          + "']/lst[@name='counts']/int[@name='" + minBucketVal + "'][.='" + bucketCount[i] + "']";
    }
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", String.valueOf(min),
        "facet.range.end", String.valueOf(max), "facet.range.gap", String.valueOf(gap), "facet.range.method", "filter"),
        testStrings);
    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", String.valueOf(min),
        "facet.range.end", String.valueOf(max), "facet.range.gap", String.valueOf(gap), "facet.range.method", "dv"),
        testStrings);
  }

  @Test
  public void testIntPointStats() throws Exception {
    int numValues = 10 * RANDOM_MULTIPLIER;
    // don't produce numbers with exponents, since XPath comparison operators can't handle them
    List<Integer> values  = getRandomInts(numValues, false, 9999999);
    // System.err.println(Arrays.toString(values.toArray(new Integer[values.size()])));
    List<Integer> sortedValues = values.stream().sorted().collect(Collectors.toList());
    double min = (double)sortedValues.get(0);
    double max = (double)sortedValues.get(sortedValues.size() - 1);

    String[] valArray = toStringArray(values);
    testPointStats("number_p_i", "number_p_i_dv", valArray, min, max, numValues, 1, 0D);
    testPointStats("number_p_i", "number_p_i_mv_dv", valArray, min, max, numValues, 1, 0D);
  }

  @Test
  public void testIntPointFieldMultiValuedExactQuery() throws Exception {
    String[] ints = toStringArray(getRandomInts(20, false));
    testPointFieldMultiValuedExactQuery("number_p_i_mv", ints);
    testPointFieldMultiValuedExactQuery("number_p_i_ni_mv_dv", ints);
  }

  @Test
  public void testIntPointFieldMultiValuedNonSearchableExactQuery() throws Exception {
    String[] ints = toStringArray(getRandomInts(20, false));
    testPointFieldMultiValuedExactQuery("number_p_i_ni_mv", ints, false);
    testPointFieldMultiValuedExactQuery("number_p_i_ni_ns_mv", ints, false);
  }
  
  @Test
  public void testIntPointFieldMultiValuedReturn() throws Exception {
    String[] ints = toStringArray(getRandomInts(20, false));
    testPointFieldMultiValuedReturn("number_p_i_mv", "int", ints);
    testPointFieldMultiValuedReturn("number_p_i_ni_mv_dv", "int", ints);
    testPointFieldMultiValuedReturn("number_p_i_dv_ns_mv", "int", ints);
  }
  
  @Test
  public void testIntPointFieldMultiValuedRangeQuery() throws Exception {
    String[] ints = toStringArray(getRandomInts(20, false).stream().sorted().collect(Collectors.toList()));
    testPointFieldMultiValuedRangeQuery("number_p_i_mv", "int", ints);
    testPointFieldMultiValuedRangeQuery("number_p_i_ni_mv_dv", "int", ints);
    testPointFieldMultiValuedRangeQuery("number_p_i_mv_dv", "int", ints);
  }
  
  @Test
  public void testIntPointFieldNotIndexed() throws Exception {
    String[] ints = toStringArray(getRandomInts(10, false));
    doTestFieldNotIndexed("number_p_i_ni", ints);
    doTestFieldNotIndexed("number_p_i_ni_mv", ints);
  }
  
  //TODO MV SORT?
  @Test
  public void testIntPointFieldMultiValuedFacetField() throws Exception {
    testPointFieldMultiValuedFacetField("number_p_i_mv", "number_p_i_mv_dv", getSequentialStringArrayWithInts(20));
    String[] randomSortedInts = toStringArray(getRandomInts(20, false).stream().sorted().collect(Collectors.toList()));
    testPointFieldMultiValuedFacetField("number_p_i_mv", "number_p_i_mv_dv", randomSortedInts);
  }

  @Test
  public void testIntPointFieldMultiValuedRangeFacet() throws Exception {
    String docValuesField = "number_p_i_mv_dv";
    String nonDocValuesField = "number_p_i_mv";
    int numValues = 20 * RANDOM_MULTIPLIER;
    int numBuckets = numValues / 2;
    List<Integer> values;
    List<PosVal<Integer>> sortedValues;
    int max;
    do {
      values = getRandomInts(numValues, false);
      sortedValues = toAscendingPosVals(values, true);
    } while ((max = sortedValues.get(sortedValues.size() - 1).val) >= Integer.MAX_VALUE - numValues); // leave room for rounding
    int min = sortedValues.get(0).val;
    int gap = (int)(((long)(max + numValues) - (long)min) / (long)numBuckets);
    List<Set<Integer>> docIdBucket = new ArrayList<>(numBuckets);
    for (int i = 0 ; i < numBuckets ; ++i) {
      docIdBucket.add(new HashSet<>());
    }
    int bucketNum = 0;
    int minBucketVal = min;
    for (PosVal<Integer> value : sortedValues) {
      while (value.val - minBucketVal >= gap) {
        ++bucketNum;
        minBucketVal += gap;
      }
      docIdBucket.get(bucketNum).add(value.pos / 2); // each doc gets two consecutive values 
    }
    for (int i = 0 ; i < numValues ; i += 2) {
      assertU(adoc("id", String.valueOf(i / 2),
          docValuesField, String.valueOf(values.get(i)),
          docValuesField, String.valueOf(values.get(i + 1)),
          nonDocValuesField, String.valueOf(values.get(i)),
          nonDocValuesField, String.valueOf(values.get(i + 1))));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    String[] testStrings = new String[numBuckets + 1];
    minBucketVal = min;
    testStrings[numBuckets] = "//*[@numFound='" + (numValues / 2) + "']";
    for (int i = 0 ; i < numBuckets ; minBucketVal += gap, ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField
          + "']/lst[@name='counts']/int[@name='" + minBucketVal + "'][.='" + docIdBucket.get(i).size() + "']";
    }

    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField,
        "facet.range.start", String.valueOf(min), "facet.range.end", String.valueOf(max),
        "facet.range.gap", String.valueOf(gap), "indent", "on"),
        testStrings);
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField,
        "facet.range.start", String.valueOf(min), "facet.range.end", String.valueOf(max),
        "facet.range.gap", String.valueOf(gap), "facet.range.method", "dv", "indent", "on"),
        testStrings);

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    minBucketVal = min;
    for (int i = 0 ; i < numBuckets ; minBucketVal += gap, ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField
          + "']/lst[@name='counts']/int[@name='" + minBucketVal + "'][.='" + docIdBucket.get(i).size() + "']";
    }
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField,
        "facet.range.start", String.valueOf(min), "facet.range.end", String.valueOf(max),
        "facet.range.gap", String.valueOf(gap), "facet.range.method", "filter", "indent", "on"),
        testStrings);
    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField,
        "facet.range.start", String.valueOf(min), "facet.range.end", String.valueOf(max),
        "facet.range.gap", String.valueOf(gap), "facet.range.method", "dv", "indent", "on"),
        testStrings);
  }

  @Test
  public void testIntPointMultiValuedFunctionQuery() throws Exception {       
    testPointMultiValuedFunctionQuery("number_p_i_mv", "number_p_i_mv_dv", "int", getSequentialStringArrayWithInts(20));
    testPointMultiValuedFunctionQuery("number_p_i_mv", "number_p_i_mv_dv", "int",
        toStringArray(getRandomInts(20, false).stream().sorted().collect(Collectors.toList())));
  }
  
  @Test
  public void testIntPointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testIntPointFieldsAtomicUpdates("number_p_i", "int");
    testIntPointFieldsAtomicUpdates("number_p_i_dv", "int");
    testIntPointFieldsAtomicUpdates("number_p_i_dv_ns", "int");
  }
  
  @Test
  public void testMultiValuedIntPointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testMultiValuedIntPointFieldsAtomicUpdates("number_p_i_mv", "int");
    testMultiValuedIntPointFieldsAtomicUpdates("number_p_i_ni_mv_dv", "int");
    testMultiValuedIntPointFieldsAtomicUpdates("number_p_i_dv_ns_mv", "int");
  }

  private <T> String[] toStringArray(List<T> list) {
    return list.stream().map(String::valueOf).collect(Collectors.toList()).toArray(new String[list.size()]);
  }

  private class PosVal <T extends Comparable<T>> {
    int pos;
    T val;

    PosVal(int pos, T val) {
      this.pos = pos;
      this.val = val;
    }
    public String toString() {
      return "(" + pos + ": " + val.toString() + ")";
    }
  }

  /** Primary sort by value, with nulls either first or last as specified, and then secondary sort by position. */
  private <T extends Comparable<T>> 
  Comparator<PosVal<T>> getPosValComparator(final boolean ascending, final boolean nullsFirst) {
    return (o1, o2) -> {
      if (o1.val == null) {
        if (o2.val == null) {
          return ascending ? Integer.compare(o1.pos, o2.pos) : Integer.compare(o2.pos, o1.pos);
        } else {
          return nullsFirst ? -1 : 1;
        }
      } else if (o2.val == null) {
        return nullsFirst ? 1 : -1;
      } else {
        return ascending ? o1.val.compareTo(o2.val) : o2.val.compareTo(o1.val);
      }
    };
  }

  /** 
   * Primary ascending sort by value, with missing values (represented as null) either first or last as specified,
   * and then secondary ascending sort by position. 
   */
  private <T extends Comparable<T>> String[] toAscendingStringArray(List<T> list, boolean missingFirst) {
    return toStringArray(toAscendingPosVals(list, missingFirst).stream().map(pv -> pv.val).collect(Collectors.toList()));
  }

  /**
   * Primary ascending sort by value, with missing values (represented as null) either first or last as specified,
   * and then secondary ascending sort by position. 
   * 
   * @return a list of the (originally) positioned values sorted as described above.
   */
  private <T extends Comparable<T>> List<PosVal<T>> toAscendingPosVals(List<T> list, boolean missingFirst) {
    List<PosVal<T>> posVals = IntStream.range(0, list.size())
        .mapToObj(i -> new PosVal<>(i, list.get(i))).collect(Collectors.toList());
    posVals.sort(getPosValComparator(true, missingFirst));
    return posVals;
  }

  /**
   * Primary descending sort by value, with missing values (represented as null) either first or last as specified,
   * and then secondary descending sort by position. 
   *
   * @return a list of the (originally) positioned values sorted as described above.
   */
  private <T extends Comparable<T>> List<PosVal<T>> toDescendingPosVals(List<T> list, boolean missingFirst) {
    List<PosVal<T>> posVals = IntStream.range(0, list.size())
        .mapToObj(i -> new PosVal<>(i, list.get(i))).collect(Collectors.toList());
    posVals.sort(getPosValComparator(false, missingFirst));
    return posVals;
  }

  @Test
  public void testIntPointSetQuery() throws Exception {
    doTestSetQueries("number_p_i", toStringArray(getRandomInts(20, false)), false);
    doTestSetQueries("number_p_i_mv", toStringArray(getRandomInts(20, false)), true);
    doTestSetQueries("number_p_i_ni_dv", toStringArray(getRandomInts(20, false)), false);
  }
  
  // DoublePointField

  @Test
  public void testDoublePointFieldExactQuery() throws Exception {
    doTestFloatPointFieldExactQuery("number_d", true);
    doTestFloatPointFieldExactQuery("number_p_d", true);
    doTestFloatPointFieldExactQuery("number_p_d_mv", true);
    doTestFloatPointFieldExactQuery("number_p_d_dv", true);
    doTestFloatPointFieldExactQuery("number_p_d_mv_dv", true);
    doTestFloatPointFieldExactQuery("number_p_d_ni_dv", true);
    doTestFloatPointFieldExactQuery("number_p_d_ni_ns_dv", true);
    doTestFloatPointFieldExactQuery("number_p_d_ni_dv_ns", true);
    doTestFloatPointFieldExactQuery("number_p_d_ni_mv_dv", true);
  }
  
  @Test
  public void testDoublePointFieldNonSearchableExactQuery() throws Exception {
    doTestFloatPointFieldExactQuery("number_p_d_ni", false, true);
    doTestFloatPointFieldExactQuery("number_p_d_ni_ns", false, true);
  }
 
  @Test
  public void testDoublePointFieldReturn() throws Exception {
    int numValues = 10 * RANDOM_MULTIPLIER;
    String[] doubles = toStringArray(getRandomDoubles(numValues, false));
    testPointFieldReturn("number_p_d", "double", doubles);
    testPointFieldReturn("number_p_d_dv_ns", "double", doubles);
  }
  
  @Test
  public void testDoublePointFieldRangeQuery() throws Exception {
    doTestFloatPointFieldRangeQuery("number_p_d", "double", true);
    doTestFloatPointFieldRangeQuery("number_p_d_ni_ns_dv", "double", true);
    doTestFloatPointFieldRangeQuery("number_p_d_dv", "double", true);
  }
  
  @Test
  public void testDoubleFieldNonSearchableRangeQuery() throws Exception {
    doTestPointFieldNonSearchableRangeQuery("number_p_d_ni", toStringArray(getRandomDoubles(1, false)));
    doTestPointFieldNonSearchableRangeQuery("number_p_d_ni_ns", toStringArray(getRandomDoubles(1, false)));
    int numValues = 2 * RANDOM_MULTIPLIER;
    doTestPointFieldNonSearchableRangeQuery("number_p_d_ni_ns_mv", toStringArray(getRandomDoubles(numValues, false)));
  }
  
  
  @Test
  public void testDoublePointFieldSortAndFunction() throws Exception {
    final SortedSet<String> regexToTest = dynFieldRegexesForType(DoublePointField.class);
    final List<String> sequential = Arrays.asList("0.0", "1.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "8.0", "9.0");
    List<Double> randomDoubles = getRandomDoubles(10, false);
    List<Double> randomDoublesMissing = getRandomDoubles(10, true);

    for (String r : Arrays.asList("*_p_d", "*_p_d_dv", "*_p_d_dv_ns", "*_p_d_ni_dv",
                                  "*_p_d_ni_dv_ns", "*_p_d_ni_ns_dv")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSort(field, sequential);
      doTestPointFieldSort(field, randomDoubles);
      doTestFloatPointFunctionQuery(field, "double");
    }

    for (String r : Arrays.asList("*_p_d_smf", "*_p_d_dv_smf", "*_p_d_ni_dv_smf",
                                  "*_p_d_sml", "*_p_d_dv_sml", "*_p_d_ni_dv_sml")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSort(field, sequential);
      doTestPointFieldSort(field, randomDoublesMissing);
      doTestFloatPointFunctionQuery(field, "double");
    }
    
    for (String r : Arrays.asList("*_p_d_ni", "*_p_d_ni_ns")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSortError(field, "w/o docValues", "42.34");
      doTestPointFieldFunctionQueryError(field, "w/o docValues", "42.34");
    }
    
    for (String r : Arrays.asList("*_p_d_mv", "*_p_d_ni_mv", "*_p_d_ni_mv_dv", "*_p_d_ni_dv_ns_mv",
                                  "*_p_d_ni_ns_mv", "*_p_d_dv_ns_mv", "*_p_d_mv_dv",
                                  "*_p_d_mv_smf", "*_p_d_mv_dv_smf", "*_p_d_ni_mv_dv_smf",
                                  "*_p_d_mv_sml", "*_p_d_mv_dv_sml", "*_p_d_ni_mv_dv_sml")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSortError(field, "multivalued", "42.34");
      doTestPointFieldSortError(field, "multivalued", "42.34", "66.6");
      doTestPointFieldFunctionQueryError(field, "multivalued", "42.34");
      doTestPointFieldFunctionQueryError(field, "multivalued", "42.34", "66.6");
    }
    
    assertEquals("Missing types in the test", Collections.<String>emptySet(), regexToTest);
  }
  
  @Test
  public void testDoublePointFieldFacetField() throws Exception {
    testPointFieldFacetField("number_p_d", "number_p_d_dv", getSequentialStringArrayWithDoubles(10));
    clearIndex();
    assertU(commit());
    testPointFieldFacetField("number_p_d", "number_p_d_dv", toStringArray(getRandomDoubles(10, false)));
  }

  @Test
  public void testDoublePointFieldRangeFacet() throws Exception {
    String docValuesField = "number_p_d_dv";
    String nonDocValuesField = "number_p_d";
    int numValues = 10 * RANDOM_MULTIPLIER;
    int numBuckets = numValues / 2;
    List<Double> values, sortedValues;
    double min, max, gap, buffer;
    do {
      values = getRandomDoubles(numValues, false);
      sortedValues = values.stream().sorted().collect(Collectors.toList());
      min = sortedValues.get(0);
      max = sortedValues.get(sortedValues.size() - 1);
      buffer = BigDecimal.valueOf(max).subtract(BigDecimal.valueOf(min))
          .divide(BigDecimal.valueOf(numValues), RoundingMode.HALF_UP)
          .divide(BigDecimal.valueOf(2.0D), RoundingMode.HALF_UP).doubleValue();
      gap = BigDecimal.valueOf(max + buffer).subtract(BigDecimal.valueOf(min - buffer))
          .divide(BigDecimal.valueOf(numBuckets), RoundingMode.HALF_UP).doubleValue();
    } while (max >= Double.MAX_VALUE - buffer || min <= -Double.MAX_VALUE + buffer);
    // System.err.println("min: " + min + "   max: " + max + "   gap: " + gap + "   buffer: " + buffer);
    int[] bucketCount = new int[numBuckets];
    int bucketNum = 0;
    double minBucketVal = min - buffer;
    // System.err.println("bucketNum: " + bucketNum + "   minBucketVal: " + minBucketVal);
    for (double value : sortedValues) {
      // System.err.println("value: " + value);
      while (value - minBucketVal >= gap) {
        ++bucketNum;
        minBucketVal += gap;
        // System.err.println("bucketNum: " + bucketNum + "   minBucketVal: " + minBucketVal);
      }
      ++bucketCount[bucketNum];
    }

    for (int i = 0 ; i < numValues ; i++) {
      assertU(adoc("id", String.valueOf(i),
          docValuesField, String.valueOf(values.get(i)), nonDocValuesField, String.valueOf(values.get(i))));
    }
    assertU(commit());

    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    String[] testStrings = new String[numBuckets + 1];
    testStrings[numBuckets] = "//*[@numFound='" + numValues + "']";
    minBucketVal = min - buffer;
    for (int i = 0 ; i < numBuckets ; minBucketVal += gap, ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField
          + "']/lst[@name='counts']/int[@name='" + minBucketVal + "'][.='" + bucketCount[i] + "']";
    }
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", String.valueOf(min - buffer),
        "facet.range.end", String.valueOf(max + buffer), "facet.range.gap", String.valueOf(gap)),
        testStrings);
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", String.valueOf(min - buffer),
        "facet.range.end", String.valueOf(max + buffer), "facet.range.gap", String.valueOf(gap), "facet.range.method", "dv"),
        testStrings);

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    minBucketVal = min - buffer;
    for (int i = 0 ; i < numBuckets ; minBucketVal += gap, ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField
          + "']/lst[@name='counts']/int[@name='" + minBucketVal + "'][.='" + bucketCount[i] + "']";
    }
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", String.valueOf(min - buffer),
        "facet.range.end", String.valueOf(max + buffer), "facet.range.gap", String.valueOf(gap), "facet.range.method", "filter"),
        testStrings);
    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", String.valueOf(min - buffer),
        "facet.range.end", String.valueOf(max + buffer), "facet.range.gap", String.valueOf(gap), "facet.range.method", "dv"),
        testStrings);
  }

  @Test
  public void testDoublePointStats() throws Exception {
    int numValues = 10 * RANDOM_MULTIPLIER;
    // don't produce numbers with exponents, since XPath comparison operators can't handle them: 7 digits of precision
    List<Float> values  = getRandomInts(numValues, false, 9999999).stream()
        .map(v -> (float)((double)v * Math.pow(10D, -1 * random().nextInt(8)))).collect(Collectors.toList());
    // System.err.println(Arrays.toString(values.toArray(new Float[values.size()])));
    List<Float> sortedValues = values.stream().sorted().collect(Collectors.toList());
    double min = (double)sortedValues.get(0);
    double max = (double)sortedValues.get(sortedValues.size() - 1);

    String[] valArray = toStringArray(values);
    testPointStats("number_p_d", "number_p_d_dv", valArray, min, max, numValues, 1, 1E-7D);
    testPointStats("number_p_d", "number_p_d_mv_dv", valArray, min, max, numValues, 1, 1E-7D);
  }
  
  @Test
  public void testDoublePointFieldMultiValuedExactQuery() throws Exception {
    String[] doubles = toStringArray(getRandomDoubles(20, false));
    testPointFieldMultiValuedExactQuery("number_p_d_mv", doubles);
    testPointFieldMultiValuedExactQuery("number_p_d_ni_mv_dv", doubles);
  }
  
  @Test
  public void testDoublePointFieldMultiValuedNonSearchableExactQuery() throws Exception {
    String[] doubles = toStringArray(getRandomDoubles(20, false));
    testPointFieldMultiValuedExactQuery("number_p_d_ni_mv", doubles, false);
    testPointFieldMultiValuedExactQuery("number_p_d_ni_ns_mv", doubles, false);
  }
  
  @Test
  public void testDoublePointFieldMultiValuedReturn() throws Exception {
    String[] doubles = toStringArray(getRandomDoubles(20, false));
    testPointFieldMultiValuedReturn("number_p_d_mv", "double", doubles);
    testPointFieldMultiValuedReturn("number_p_d_ni_mv_dv", "double", doubles);
    testPointFieldMultiValuedReturn("number_p_d_dv_ns_mv", "double", doubles);
  }
  
  @Test
  public void testDoublePointFieldMultiValuedRangeQuery() throws Exception {
    String[] doubles = toStringArray(getRandomDoubles(20, false).stream().sorted().collect(Collectors.toList()));
    testPointFieldMultiValuedRangeQuery("number_p_d_mv", "double", doubles);
    testPointFieldMultiValuedRangeQuery("number_p_d_ni_mv_dv", "double", doubles);
    testPointFieldMultiValuedRangeQuery("number_p_d_mv_dv", "double", doubles);
  }
  
  @Test
  public void testDoublePointFieldMultiValuedFacetField() throws Exception {
    testPointFieldMultiValuedFacetField("number_p_d_mv", "number_p_d_mv_dv", getSequentialStringArrayWithDoubles(20));
    testPointFieldMultiValuedFacetField("number_p_d_mv", "number_p_d_mv_dv", toStringArray(getRandomDoubles(20, false)));
  }

  @Test
  public void testDoublePointFieldMultiValuedRangeFacet() throws Exception {
    doTestDoublePointFieldMultiValuedRangeFacet("number_p_d_mv_dv", "number_p_d_mv");
  }
  
  @Test
  public void testDoublePointMultiValuedFunctionQuery() throws Exception {
    testPointMultiValuedFunctionQuery("number_p_d_mv", "number_p_d_mv_dv", "double", getSequentialStringArrayWithDoubles(20));
    testPointMultiValuedFunctionQuery("number_p_d_mv", "number_p_d_mv_dv", "double", toAscendingStringArray(getRandomFloats(20, false), true));
  }
  
  @Test
  public void testDoublePointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    doTestFloatPointFieldsAtomicUpdates("number_p_d", "double");
    doTestFloatPointFieldsAtomicUpdates("number_p_d_dv", "double");
    doTestFloatPointFieldsAtomicUpdates("number_p_d_dv_ns", "double");
  }
  
  @Test
  public void testMultiValuedDoublePointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testMultiValuedFloatPointFieldsAtomicUpdates("number_p_d_mv", "double");
    testMultiValuedFloatPointFieldsAtomicUpdates("number_p_d_ni_mv_dv", "double");
    testMultiValuedFloatPointFieldsAtomicUpdates("number_p_d_dv_ns_mv", "double");
  }
  
  @Test
  public void testDoublePointFieldNotIndexed() throws Exception {
    String[] doubles = toStringArray(getRandomDoubles(10, false));
    doTestFieldNotIndexed("number_p_d_ni", doubles);
    doTestFieldNotIndexed("number_p_d_ni_mv", doubles);
  }
  
  
  private void doTestFloatPointFieldsAtomicUpdates(String field, String type) throws Exception {
    assertU(adoc(sdoc("id", "1", field, "1.1234")));
    assertU(commit());

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("inc", 1.1F))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='2.2234']");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("inc", -1.1F))));
    assertU(commit());
    
    // TODO: can this test be better?
    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.>'1.1233']",
        "//result/doc[1]/" + type + "[@name='" + field + "'][.<'1.1235']");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", 3.123F))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='3.123']");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", 3.14F))));
    assertU(commit());
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("inc", 1F))));
    assertU(commit());
    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.>'4.13']",
        "//result/doc[1]/" + type + "[@name='" + field + "'][.<'4.15']");
  }
  
  @Test
  public void testDoublePointSetQuery() throws Exception {
    doTestSetQueries("number_p_d", toStringArray(getRandomDoubles(20, false)), false);
    doTestSetQueries("number_p_d_mv", toStringArray(getRandomDoubles(20, false)), true);
    doTestSetQueries("number_p_d_ni_dv", toStringArray(getRandomDoubles(20, false)), false);
  }
  
  // Float

  @Test
  public void testFloatPointFieldExactQuery() throws Exception {
    doTestFloatPointFieldExactQuery("number_p_f", false);
    doTestFloatPointFieldExactQuery("number_p_f_mv", false);
    doTestFloatPointFieldExactQuery("number_p_f_dv", false);
    doTestFloatPointFieldExactQuery("number_p_f_mv_dv", false);
    doTestFloatPointFieldExactQuery("number_p_f_ni_dv", false);
    doTestFloatPointFieldExactQuery("number_p_f_ni_ns_dv", false);
    doTestFloatPointFieldExactQuery("number_p_f_ni_dv_ns", false);
    doTestFloatPointFieldExactQuery("number_p_f_ni_mv_dv", false);
  }
  
  @Test
  public void testFloatPointFieldNonSearchableExactQuery() throws Exception {
    doTestFloatPointFieldExactQuery("number_p_f_ni", false, false);
    doTestFloatPointFieldExactQuery("number_p_f_ni_ns", false, false);
  }
  
  @Test
  public void testFloatPointFieldReturn() throws Exception {
    int numValues = 10 * RANDOM_MULTIPLIER;
    String[] floats = toStringArray(getRandomFloats(numValues, false));
    testPointFieldReturn("number_p_f", "float", floats);
    testPointFieldReturn("number_p_f_dv_ns", "float", floats);
  }
  
  @Test
  public void testFloatPointFieldRangeQuery() throws Exception {
    doTestFloatPointFieldRangeQuery("number_p_f", "float", false);
    doTestFloatPointFieldRangeQuery("number_p_f_ni_ns_dv", "float", false);
    doTestFloatPointFieldRangeQuery("number_p_f_dv", "float", false);
  }
  
  @Test
  public void testFloatPointFieldNonSearchableRangeQuery() throws Exception {
    doTestPointFieldNonSearchableRangeQuery("number_p_f_ni", toStringArray(getRandomFloats(1, false)));
    doTestPointFieldNonSearchableRangeQuery("number_p_f_ni_ns", toStringArray(getRandomFloats(1, false)));
    int numValues = 2 * RANDOM_MULTIPLIER;
    doTestPointFieldNonSearchableRangeQuery("number_p_f_ni_ns_mv", toStringArray(getRandomFloats(numValues, false)));
  }
  
  @Test
  public void testFloatPointFieldSortAndFunction() throws Exception {
    final SortedSet<String> regexToTest = dynFieldRegexesForType(FloatPointField.class);
    final List<String> sequential = Arrays.asList("0.0", "1.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "8.0", "9.0");
    final List<Float> randomFloats = getRandomFloats(10, false);
    final List<Float> randomFloatsMissing = getRandomFloats(10, true);
    
    for (String r : Arrays.asList("*_p_f", "*_p_f_dv", "*_p_f_dv_ns", "*_p_f_ni_dv", 
                                  "*_p_f_ni_dv_ns", "*_p_f_ni_ns_dv")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSort(field, sequential);
      doTestPointFieldSort(field, randomFloats);

      doTestFloatPointFunctionQuery(field, "float");
    }
    for (String r : Arrays.asList("*_p_f_smf", "*_p_f_dv_smf", "*_p_f_ni_dv_smf",
                                  "*_p_f_sml", "*_p_f_dv_sml", "*_p_f_ni_dv_sml")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSort(field, sequential);
      doTestPointFieldSort(field, randomFloatsMissing);
      doTestFloatPointFunctionQuery(field, "float");
    }
    
    for (String r : Arrays.asList("*_p_f_ni", "*_p_f_ni_ns")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSortError(field, "w/o docValues", "42.34");
      doTestPointFieldFunctionQueryError(field, "w/o docValues", "42.34");
    }
    
    for (String r : Arrays.asList("*_p_f_mv", "*_p_f_ni_mv", "*_p_f_ni_mv_dv", "*_p_f_ni_dv_ns_mv",
                                  "*_p_f_ni_ns_mv", "*_p_f_dv_ns_mv", "*_p_f_mv_dv",  
                                  "*_p_f_mv_smf", "*_p_f_mv_dv_smf", "*_p_f_ni_mv_dv_smf",
                                  "*_p_f_mv_sml", "*_p_f_mv_dv_sml", "*_p_f_ni_mv_dv_sml")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSortError(field, "multivalued", "42.34");
      doTestPointFieldSortError(field, "multivalued", "42.34", "66.6");
      doTestPointFieldFunctionQueryError(field, "multivalued", "42.34");
      doTestPointFieldFunctionQueryError(field, "multivalued", "42.34", "66.6");
    }
    
    assertEquals("Missing types in the test", Collections.<String>emptySet(), regexToTest);
  }
  
  @Test
  public void testFloatPointFieldFacetField() throws Exception {
    testPointFieldFacetField("number_p_f", "number_p_f_dv", getSequentialStringArrayWithDoubles(10));
    clearIndex();
    assertU(commit());
    testPointFieldFacetField("number_p_f", "number_p_f_dv", toStringArray(getRandomFloats(10, false)));
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-11043")
  public void testFloatPointFieldRangeFacet() throws Exception {
    String docValuesField = "number_p_f_dv";
    String nonDocValuesField = "number_p_f";
    int numValues = 10 * RANDOM_MULTIPLIER;
    int numBuckets = numValues / 2;
    List<Float> values, sortedValues;
    float min, max, gap, buffer;
    do {
      values = getRandomFloats(numValues, false);
      sortedValues = values.stream().sorted().collect(Collectors.toList());
      min = sortedValues.get(0);
      max = sortedValues.get(sortedValues.size() - 1);
      buffer = (float)(((double)max - (double)min) / (double)numValues / 2.0D);
      gap = (float)(((double)max + (double)buffer - (double)min + (double)buffer) / (double)numBuckets);
    } while (max >= Float.MAX_VALUE - buffer || min <= -Float.MAX_VALUE + buffer); 
    // System.err.println("min: " + min + "   max: " + max + "   gap: " + gap + "   buffer: " + buffer);
    int[] bucketCount = new int[numBuckets];
    int bucketNum = 0;
    float minBucketVal = min - buffer;
    // System.err.println("bucketNum: " + bucketNum + "   minBucketVal: " + minBucketVal);
    for (float value : sortedValues) {
      // System.err.println("value: " + value);
      while (value - minBucketVal >= gap) {
        ++bucketNum;
        minBucketVal += gap;
        // System.err.println("bucketNum: " + bucketNum + "   minBucketVal: " + minBucketVal);
      }
      ++bucketCount[bucketNum];
    }

    for (int i = 0 ; i < numValues ; i++) {
      assertU(adoc("id", String.valueOf(i), 
          docValuesField, String.valueOf(values.get(i)), nonDocValuesField, String.valueOf(values.get(i))));
    }
    assertU(commit());

    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    String[] testStrings = new String[numBuckets + 1];
    testStrings[numBuckets] = "//*[@numFound='" + numValues + "']";
    minBucketVal = min - buffer;
    for (int i = 0 ; i < numBuckets ; minBucketVal += gap, ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField
          + "']/lst[@name='counts']/int[@name='" + minBucketVal + "'][.='" + bucketCount[i] + "']";
    }
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", String.valueOf(min - buffer),
        "facet.range.end", String.valueOf(max + buffer), "facet.range.gap", String.valueOf(gap)),
        testStrings);
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", String.valueOf(min - buffer),
        "facet.range.end", String.valueOf(max + buffer), "facet.range.gap", String.valueOf(gap), "facet.range.method", "dv"),
        testStrings);

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    minBucketVal = min - buffer;
    for (int i = 0 ; i < numBuckets ; minBucketVal += gap, ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField
          + "']/lst[@name='counts']/int[@name='" + minBucketVal + "'][.='" + bucketCount[i] + "']";
    }
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", String.valueOf(min - buffer),
        "facet.range.end", String.valueOf(max + buffer), "facet.range.gap", String.valueOf(gap), "facet.range.method", "filter"),
        testStrings);
    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", String.valueOf(min - buffer),
        "facet.range.end", String.valueOf(max + buffer), "facet.range.gap", String.valueOf(gap), "facet.range.method", "dv"),
        testStrings);
  }

  @Test
  public void testFloatPointStats() throws Exception {
    int numValues = 10 * RANDOM_MULTIPLIER;
    // don't produce numbers with exponents, since XPath comparison operators can't handle them: 7 digits of precision
    List<Float> values  = getRandomInts(numValues, false, 9999999).stream()
        .map(v -> (float)((double)v * Math.pow(10D, -1 * random().nextInt(8)))).collect(Collectors.toList());
    // System.err.println(Arrays.toString(values.toArray(new Float[values.size()])));
    List<Float> sortedValues = values.stream().sorted().collect(Collectors.toList());
    double min = (double)sortedValues.get(0);
    double max = (double)sortedValues.get(sortedValues.size() - 1);

    String[] valArray = toStringArray(values);
    testPointStats("number_p_f", "number_p_f_dv", valArray, min, max, numValues, 1, 1E-7D);
    testPointStats("number_p_f", "number_p_f_mv_dv", valArray, min, max, numValues, 1, 1E-7D);
  }
  
  @Test
  public void testFloatPointFieldMultiValuedExactQuery() throws Exception {
    String[] floats = toStringArray(getRandomFloats(20, false));
    testPointFieldMultiValuedExactQuery("number_p_f_mv", floats);
    testPointFieldMultiValuedExactQuery("number_p_f_ni_mv_dv", floats);
  }
  
  @Test
  public void testFloatPointFieldMultiValuedNonSearchableExactQuery() throws Exception {
    String[] floats = toStringArray(getRandomFloats(20, false));
    testPointFieldMultiValuedExactQuery("number_p_f_ni_mv", floats, false);
    testPointFieldMultiValuedExactQuery("number_p_f_ni_ns_mv", floats, false);
  }
  
  @Test
  public void testFloatPointFieldMultiValuedReturn() throws Exception {
    String[] floats = toStringArray(getRandomFloats(20, false));
    testPointFieldMultiValuedReturn("number_p_f_mv", "float", floats);
    testPointFieldMultiValuedReturn("number_p_f_ni_mv_dv", "float", floats);
    testPointFieldMultiValuedReturn("number_p_f_dv_ns_mv", "float", floats);
  }
  
  @Test
  public void testFloatPointFieldMultiValuedRangeQuery() throws Exception {
    String[] floats = toStringArray(getRandomFloats(20, false).stream().sorted().collect(Collectors.toList()));
    testPointFieldMultiValuedRangeQuery("number_p_f_mv", "float", floats);
    testPointFieldMultiValuedRangeQuery("number_p_f_ni_mv_dv", "float", floats);
    testPointFieldMultiValuedRangeQuery("number_p_f_mv_dv", "float", floats);
  }
  
  @Test
  public void testFloatPointFieldMultiValuedRangeFacet() throws Exception {
    doTestDoublePointFieldMultiValuedRangeFacet("number_p_f_mv_dv", "number_p_f_mv");
  }
  
  @Test
  public void testFloatPointFieldMultiValuedFacetField() throws Exception {
    testPointFieldMultiValuedFacetField("number_p_f_mv", "number_p_f_mv_dv", getSequentialStringArrayWithDoubles(20));
    testPointFieldMultiValuedFacetField("number_p_f_mv", "number_p_f_mv_dv", toStringArray(getRandomFloats(20, false)));
  }
  
  @Test
  public void testFloatPointMultiValuedFunctionQuery() throws Exception {
    testPointMultiValuedFunctionQuery("number_p_f_mv", "number_p_f_mv_dv", "float", getSequentialStringArrayWithDoubles(20));
    testPointMultiValuedFunctionQuery("number_p_f_mv", "number_p_f_mv_dv", "float", toAscendingStringArray(getRandomFloats(20, false), true));
  }
  
  
  @Test
  public void testFloatPointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    doTestFloatPointFieldsAtomicUpdates("number_p_f", "float");
    doTestFloatPointFieldsAtomicUpdates("number_p_f_dv", "float");
    doTestFloatPointFieldsAtomicUpdates("number_p_f_dv_ns", "float");
  }
  
  @Test
  public void testMultiValuedFloatePointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testMultiValuedFloatPointFieldsAtomicUpdates("number_p_f_mv", "float");
    testMultiValuedFloatPointFieldsAtomicUpdates("number_p_f_ni_mv_dv", "float");
    testMultiValuedFloatPointFieldsAtomicUpdates("number_p_f_dv_ns_mv", "float");
  }

  @Test
  public void testFloatPointSetQuery() throws Exception {
    doTestSetQueries("number_p_f", toStringArray(getRandomFloats(20, false)), false);
    doTestSetQueries("number_p_f_mv", toStringArray(getRandomFloats(20, false)), true);
    doTestSetQueries("number_p_f_ni_dv", toStringArray(getRandomFloats(20, false)), false);
  }
  
  @Test
  public void testFloatPointFieldNotIndexed() throws Exception {
    String[] floats = toStringArray(getRandomFloats(10, false));
    doTestFieldNotIndexed("number_p_f_ni", floats);
    doTestFieldNotIndexed("number_p_f_ni_mv", floats);
  }
  
  // Long
  
  @Test
  public void testLongPointFieldExactQuery() throws Exception {
    doTestIntPointFieldExactQuery("number_p_l", true);
    doTestIntPointFieldExactQuery("number_p_l_mv", true);
    doTestIntPointFieldExactQuery("number_p_l_dv", true);
    doTestIntPointFieldExactQuery("number_p_l_mv_dv", true);
    doTestIntPointFieldExactQuery("number_p_l_ni_dv", true);
    doTestIntPointFieldExactQuery("number_p_l_ni_ns_dv", true);
    doTestIntPointFieldExactQuery("number_p_l_ni_dv_ns", true);
    doTestIntPointFieldExactQuery("number_p_l_ni_mv_dv", true);
  }
  
  @Test
  public void testLongPointFieldNonSearchableExactQuery() throws Exception {
    doTestIntPointFieldExactQuery("number_p_l_ni", true, false);
    doTestIntPointFieldExactQuery("number_p_l_ni_ns", true, false);
  }
  
  @Test
  public void testLongPointFieldReturn() throws Exception {
    int numValues = 10 * RANDOM_MULTIPLIER;
    String[] longs = toStringArray(getRandomLongs(numValues, false));
    testPointFieldReturn("number_p_l", "long", longs);
    testPointFieldReturn("number_p_l_dv_ns", "long", longs);
  }
  
  @Test
  public void testLongPointFieldRangeQuery() throws Exception {
    doTestIntPointFieldRangeQuery("number_p_l", "long", true);
    doTestIntPointFieldRangeQuery("number_p_l_ni_ns_dv", "long", true);
    doTestIntPointFieldRangeQuery("number_p_l_dv", "long", true);
  }
  
  @Test
  public void testLongPointFieldNonSearchableRangeQuery() throws Exception {
    doTestPointFieldNonSearchableRangeQuery("number_p_l_ni", toStringArray(getRandomLongs(1, false)));
    doTestPointFieldNonSearchableRangeQuery("number_p_l_ni_ns", toStringArray(getRandomLongs(1, false)));
    int numValues = 2 * RANDOM_MULTIPLIER;
    doTestPointFieldNonSearchableRangeQuery("number_p_l_ni_ns_mv", toStringArray(getRandomLongs(numValues, false)));
  }

  @Test
  public void testLongPointFieldSortAndFunction() throws Exception {
    final SortedSet<String> regexToTest = dynFieldRegexesForType(LongPointField.class);
    final List<Long> vals = Arrays.asList((long)Integer.MIN_VALUE, 
                                          1L, 2L, 3L, 4L, 5L, 6L, 7L, 
                                          (long)Integer.MAX_VALUE, Long.MAX_VALUE);
    final List<Long> randomLongs = getRandomLongs(10, false);
    final List<Long> randomLongsMissing = getRandomLongs(10, true);
    
    for (String r : Arrays.asList("*_p_l", "*_p_l_dv", "*_p_l_dv_ns", "*_p_l_ni_dv",
                                  "*_p_l_ni_dv_ns", "*_p_l_ni_ns_dv")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSort(field, vals);
      doTestPointFieldSort(field, randomLongs);
      doTestIntPointFunctionQuery(field, "long");
    }

    for (String r : Arrays.asList("*_p_l_smf", "*_p_l_dv_smf", "*_p_l_ni_dv_smf",
                                  "*_p_l_sml", "*_p_l_dv_sml", "*_p_l_ni_dv_sml")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSort(field, vals);
      doTestPointFieldSort(field, randomLongsMissing);
      doTestIntPointFunctionQuery(field, "long");
    }

    for (String r : Arrays.asList("*_p_l_ni", "*_p_l_ni_ns")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSortError(field, "w/o docValues", toStringArray(getRandomLongs(1, false)));
      doTestPointFieldFunctionQueryError(field, "w/o docValues", toStringArray(getRandomLongs(1, false)));
    }
    
    for (String r : Arrays.asList("*_p_l_mv", "*_p_l_ni_mv", "*_p_l_ni_mv_dv", "*_p_l_ni_dv_ns_mv",
                                  "*_p_l_ni_ns_mv", "*_p_l_dv_ns_mv", "*_p_l_mv_dv",
                                  "*_p_l_mv_smf", "*_p_l_mv_dv_smf", "*_p_l_ni_mv_dv_smf",
                                  "*_p_l_mv_sml", "*_p_l_mv_dv_sml", "*_p_l_ni_mv_dv_sml")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSortError(field, "multivalued", toStringArray(getRandomLongs(1, false)));
      int numValues = 2 * RANDOM_MULTIPLIER;
      doTestPointFieldSortError(field, "multivalued", toStringArray(getRandomLongs(numValues, false)));
      doTestPointFieldFunctionQueryError(field, "multivalued", toStringArray(getRandomLongs(1, false)));
      doTestPointFieldFunctionQueryError(field, "multivalued", toStringArray(getRandomLongs(numValues, false)));
    }
    
    assertEquals("Missing types in the test", Collections.<String>emptySet(), regexToTest);
  }
  
  @Test
  public void testLongPointFieldFacetField() throws Exception {
    testPointFieldFacetField("number_p_l", "number_p_l_dv", getSequentialStringArrayWithInts(10));
    clearIndex();
    assertU(commit());
    testPointFieldFacetField("number_p_l", "number_p_l_dv", toStringArray(getRandomLongs(10, false)));
  }
  
  @Test
  public void testLongPointFieldRangeFacet() throws Exception {
    String docValuesField = "number_p_l_dv";
    String nonDocValuesField = "number_p_l";
    int numValues = 10 * RANDOM_MULTIPLIER;
    int numBuckets = numValues / 2;
    List<Long> values;
    List<Long> sortedValues;
    long max;
    do {
      values = getRandomLongs(numValues, false);
      sortedValues = values.stream().sorted().collect(Collectors.toList());
    } while ((max = sortedValues.get(sortedValues.size() - 1)) >= Long.MAX_VALUE - numValues); // leave room for rounding 
    long min = sortedValues.get(0);
    BigInteger bigIntGap =  BigInteger.valueOf(max + numValues).subtract(BigInteger.valueOf(min))
        .divide(BigInteger.valueOf(numBuckets));
    long gap = bigIntGap.longValueExact();
    int[] bucketCount = new int[numBuckets];
    int bucketNum = 0;
    long minBucketVal = min;
    // System.err.println("min:" + min + "   max: " + max + "   gap: " + gap);
    // System.err.println("bucketNum: " + bucketNum + "   minBucketVal: " + minBucketVal);
    for (Long value : sortedValues) {
      // System.err.println("value: " + value);
      while (BigInteger.valueOf(value).subtract(BigInteger.valueOf(minBucketVal)).compareTo(bigIntGap) > 0) {
        ++bucketNum;
        minBucketVal += gap;
        // System.err.println("bucketNum: " + bucketNum + "   minBucketVal: " + minBucketVal);
      }
      ++bucketCount[bucketNum];
    }

    for (int i = 0 ; i < numValues ; i++) {
      assertU(adoc("id", String.valueOf(i), docValuesField, String.valueOf(values.get(i)), nonDocValuesField, String.valueOf(values.get(i))));
    }
    assertU(commit());

    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    String[] testStrings = new String[numBuckets + 1];
    testStrings[numBuckets] = "//*[@numFound='" + numValues + "']";
    minBucketVal = min;
    for (int i = 0 ; i < numBuckets ; minBucketVal += gap, ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField
          + "']/lst[@name='counts']/int[@name='" + minBucketVal + "'][.='" + bucketCount[i] + "']";
    }
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", String.valueOf(min),
        "facet.range.end", String.valueOf(max), "facet.range.gap", String.valueOf(gap)),
        testStrings);
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", String.valueOf(min),
        "facet.range.end", String.valueOf(max), "facet.range.gap", String.valueOf(gap), "facet.range.method", "dv"),
        testStrings);

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    minBucketVal = min;
    for (int i = 0 ; i < numBuckets ; minBucketVal += gap, ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField
          + "']/lst[@name='counts']/int[@name='" + minBucketVal + "'][.='" + bucketCount[i] + "']";
    }
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", String.valueOf(min),
        "facet.range.end", String.valueOf(max), "facet.range.gap", String.valueOf(gap), "facet.range.method", "filter"),
        testStrings);
    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", String.valueOf(min),
        "facet.range.end", String.valueOf(max), "facet.range.gap", String.valueOf(gap), "facet.range.method", "dv"),
        testStrings);
  }
  
  @Test
  public void testLongPointStats() throws Exception {
    int numValues = 10 * RANDOM_MULTIPLIER;
    // don't produce numbers with exponents, since XPath comparison operators can't handle them
    List<Long> values  = getRandomLongs(numValues, false, 9999999L);
    List<Long> sortedValues = values.stream().sorted().collect(Collectors.toList());
    double min = (double)sortedValues.get(0);
    double max = (double)sortedValues.get(sortedValues.size() - 1);

    String[] valArray = toStringArray(values);
    testPointStats("number_p_l", "number_p_l_dv", valArray, min, max, numValues, 1, 0D);
    testPointStats("number_p_l", "number_p_l_mv_dv", valArray, min, max, numValues, 1, 0D);
  }
  
  @Test
  public void testLongPointFieldMultiValuedExactQuery() throws Exception {
    String[] ints = toStringArray(getRandomInts(20, false));
    testPointFieldMultiValuedExactQuery("number_p_l_mv", ints);
    testPointFieldMultiValuedExactQuery("number_p_l_ni_mv_dv", ints);
  }
  
  @Test
  public void testLongPointFieldMultiValuedNonSearchableExactQuery() throws Exception {
    String[] longs = toStringArray(getRandomLongs(20, false));
    testPointFieldMultiValuedExactQuery("number_p_l_ni_mv", longs, false);
    testPointFieldMultiValuedExactQuery("number_p_l_ni_ns_mv", longs, false);
  }
  
  @Test
  public void testLongPointFieldMultiValuedReturn() throws Exception {
    String[] longs = toStringArray(getRandomLongs(20, false));
    testPointFieldMultiValuedReturn("number_p_l_mv", "long", longs);
    testPointFieldMultiValuedReturn("number_p_l_ni_mv_dv", "long", longs);
    testPointFieldMultiValuedReturn("number_p_l_dv_ns_mv", "long", longs);
  }
  
  @Test
  public void testLongPointFieldMultiValuedRangeQuery() throws Exception {
    String[] longs = toStringArray(getRandomLongs(20, false).stream().sorted().collect(Collectors.toList()));
    testPointFieldMultiValuedRangeQuery("number_p_l_mv", "long", longs);
    testPointFieldMultiValuedRangeQuery("number_p_l_ni_mv_dv", "long", longs);
    testPointFieldMultiValuedRangeQuery("number_p_l_mv_dv", "long", longs);
  }
  
  @Test
  public void testLongPointFieldMultiValuedFacetField() throws Exception {
    testPointFieldMultiValuedFacetField("number_p_l_mv", "number_p_l_mv_dv", getSequentialStringArrayWithInts(20));
    testPointFieldMultiValuedFacetField("number_p_l_mv", "number_p_l_mv_dv", toStringArray(getRandomLongs(20, false)));
  }
  
  @Test
  public void testLongPointFieldMultiValuedRangeFacet() throws Exception {
    String docValuesField = "number_p_l_mv_dv";
    String nonDocValuesField = "number_p_l_mv";
    int numValues = 20 * RANDOM_MULTIPLIER;
    int numBuckets = numValues / 2;
    List<Long> values;
    List<PosVal<Long>> sortedValues;
    long max;
    do {
      values = getRandomLongs(numValues, false);
      sortedValues = toAscendingPosVals(values, true);
    } while ((max = sortedValues.get(sortedValues.size() - 1).val) >= Long.MAX_VALUE - numValues); // leave room for rounding 
    long min = sortedValues.get(0).val;
    long gap = BigInteger.valueOf(max + numValues).subtract(BigInteger.valueOf(min))
        .divide(BigInteger.valueOf(numBuckets)).longValueExact();
    List<Set<Integer>> docIdBucket = new ArrayList<>(numBuckets);
    for (int i = 0 ; i < numBuckets ; ++i) {
      docIdBucket.add(new HashSet<>());
    }
    int bucketNum = 0;
    long minBucketVal = min;
    for (PosVal<Long> value : sortedValues) {
      while (value.val - minBucketVal >= gap) {
        ++bucketNum;
        minBucketVal += gap;
      }
      docIdBucket.get(bucketNum).add(value.pos / 2); // each doc gets two consecutive values 
    }
    for (int i = 0 ; i < numValues ; i += 2) {
      assertU(adoc("id", String.valueOf(i / 2),
          docValuesField, String.valueOf(values.get(i)),
          docValuesField, String.valueOf(values.get(i + 1)),
          nonDocValuesField, String.valueOf(values.get(i)),
          nonDocValuesField, String.valueOf(values.get(i + 1))));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    String[] testStrings = new String[numBuckets + 1];
    testStrings[numBuckets] = "//*[@numFound='" + (numValues / 2) + "']";
    minBucketVal = min;
    for (int i = 0 ; i < numBuckets ; minBucketVal += gap, ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField
          + "']/lst[@name='counts']/int[@name='" + minBucketVal + "'][.='" + docIdBucket.get(i).size() + "']";
    }

    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField,
        "facet.range.start", String.valueOf(min), "facet.range.end", String.valueOf(max),
        "facet.range.gap", String.valueOf(gap), "indent", "on"),
        testStrings);
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField,
        "facet.range.start", String.valueOf(min), "facet.range.end", String.valueOf(max),
        "facet.range.gap", String.valueOf(gap), "facet.range.method", "dv", "indent", "on"),
        testStrings);

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    minBucketVal = min;
    for (int i = 0 ; i < numBuckets ; minBucketVal += gap, ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField
          + "']/lst[@name='counts']/int[@name='" + minBucketVal + "'][.='" + docIdBucket.get(i).size() + "']";
    }
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField,
        "facet.range.start", String.valueOf(min), "facet.range.end", String.valueOf(max),
        "facet.range.gap", String.valueOf(gap), "facet.range.method", "filter", "indent", "on"),
        testStrings);
    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField,
        "facet.range.start", String.valueOf(min), "facet.range.end", String.valueOf(max),
        "facet.range.gap", String.valueOf(gap), "facet.range.method", "dv", "indent", "on"),
        testStrings);
  }
  
  @Test
  public void testLongPointMultiValuedFunctionQuery() throws Exception {
    testPointMultiValuedFunctionQuery("number_p_l_mv", "number_p_l_mv_dv", "long", getSequentialStringArrayWithInts(20));
  }
  
  @Test
  public void testLongPointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testIntPointFieldsAtomicUpdates("number_p_l", "long");
    testIntPointFieldsAtomicUpdates("number_p_l_dv", "long");
    testIntPointFieldsAtomicUpdates("number_p_l_dv_ns", "long");
  }
  
  @Test
  public void testMultiValuedLongPointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testMultiValuedIntPointFieldsAtomicUpdates("number_p_l_mv", "long");
    testMultiValuedIntPointFieldsAtomicUpdates("number_p_l_ni_mv_dv", "long");
    testMultiValuedIntPointFieldsAtomicUpdates("number_p_l_dv_ns_mv", "long");
  }
  
  @Test
  public void testLongPointSetQuery() throws Exception {
    doTestSetQueries("number_p_l", toStringArray(getRandomLongs(20, false)), false);
    doTestSetQueries("number_p_l_mv", toStringArray(getRandomLongs(20, false)), true);
    doTestSetQueries("number_p_l_ni_dv", toStringArray(getRandomLongs(20, false)), false);
  }
  
  @Test
  public void testLongPointFieldNotIndexed() throws Exception {
    String[] longs = toStringArray(getRandomLongs(10, false));
    doTestFieldNotIndexed("number_p_l_ni", longs);
    doTestFieldNotIndexed("number_p_l_ni_mv", longs);
  }

  // Date

  @Test
  public void testDatePointFieldExactQuery() throws Exception {
    doTestDatePointFieldExactQuery("number_p_dt", "1995-12-31T23:59:59Z");
    doTestDatePointFieldExactQuery("number_p_dt_mv", "2015-12-31T23:59:59Z-1DAY");
    doTestDatePointFieldExactQuery("number_p_dt_dv", "2000-12-31T23:59:59Z+3DAYS");
    doTestDatePointFieldExactQuery("number_p_dt_mv_dv", "2000-12-31T23:59:59Z+3DAYS");
    doTestDatePointFieldExactQuery("number_p_dt_ni_dv", "2000-12-31T23:59:59Z+3DAYS");
    doTestDatePointFieldExactQuery("number_p_dt_ni_ns_dv", "1995-12-31T23:59:59Z-1MONTH");
    doTestDatePointFieldExactQuery("number_p_dt_ni_mv_dv", "1995-12-31T23:59:59Z+2MONTHS");
  }
  @Test
  public void testDatePointFieldNonSearchableExactQuery() throws Exception {
    doTestDatePointFieldExactQuery("number_p_dt_ni", "1995-12-31T23:59:59Z", false);
    doTestDatePointFieldExactQuery("number_p_dt_ni_ns", "1995-12-31T23:59:59Z", false);

  }

  @Test
  public void testDatePointFieldReturn() throws Exception {
    int numValues = 10 * RANDOM_MULTIPLIER;
    String[] dates = toStringArray(getRandomInstants(numValues, false));
    testPointFieldReturn("number_p_dt", "date", dates);
    testPointFieldReturn("number_p_dt_dv_ns", "date", dates);
  }

  @Test
  public void testDatePointFieldRangeQuery() throws Exception {
    doTestDatePointFieldRangeQuery("number_p_dt");
    doTestDatePointFieldRangeQuery("number_p_dt_ni_ns_dv");
  }
  
  @Test
  public void testDatePointFieldNonSearchableRangeQuery() throws Exception {
    doTestPointFieldNonSearchableRangeQuery("number_p_dt_ni", toStringArray(getRandomInstants(1, false)));
    doTestPointFieldNonSearchableRangeQuery("number_p_dt_ni_ns", toStringArray(getRandomInstants(1, false)));
    int numValues = 2 * RANDOM_MULTIPLIER;
    doTestPointFieldNonSearchableRangeQuery("number_p_dt_ni_ns_mv", toStringArray(getRandomInstants(numValues, false)));
  }

  @Test
  public void testDatePointFieldSortAndFunction() throws Exception {
    final SortedSet<String> regexToTest = dynFieldRegexesForType(DatePointField.class);
    final List<String> sequential = Arrays.asList(getSequentialStringArrayWithDates(10));
    final List<Instant> randomDates = getRandomInstants(10, false);
    final List<Instant> randomDatesMissing = getRandomInstants(10, true);
    
    for (String r : Arrays.asList("*_p_dt", "*_p_dt_dv", "*_p_dt_dv_ns", "*_p_dt_ni_dv",
                                  "*_p_dt_ni_dv_ns", "*_p_dt_ni_ns_dv")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSort(field, sequential);
      doTestPointFieldSort(field, randomDates);
      doTestDatePointFunctionQuery(field, "date");
    }
    for (String r : Arrays.asList("*_p_dt_smf", "*_p_dt_dv_smf", "*_p_dt_ni_dv_smf",
                                  "*_p_dt_sml", "*_p_dt_dv_sml", "*_p_dt_ni_dv_sml")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSort(field, sequential);
      doTestPointFieldSort(field, randomDatesMissing);
      doTestDatePointFunctionQuery(field, "date");
    }
    
    for (String r : Arrays.asList("*_p_dt_ni", "*_p_dt_ni_ns")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSortError(field, "w/o docValues", "1995-12-31T23:59:59Z");
      doTestPointFieldFunctionQueryError(field, "w/o docValues", "1995-12-31T23:59:59Z");
    }
    
    for (String r : Arrays.asList("*_p_dt_mv", "*_p_dt_ni_mv", "*_p_dt_ni_mv_dv", "*_p_dt_ni_dv_ns_mv",
                                  "*_p_dt_ni_ns_mv", "*_p_dt_dv_ns_mv", "*_p_dt_mv_dv",
                                  "*_p_dt_mv_smf", "*_p_dt_mv_dv_smf", "*_p_dt_ni_mv_dv_smf",
                                  "*_p_dt_mv_sml", "*_p_dt_mv_dv_sml", "*_p_dt_ni_mv_dv_sml")) {
      assertTrue(r, regexToTest.remove(r));
      String field = r.replace("*", "number");
      doTestPointFieldSortError(field, "multivalued", "1995-12-31T23:59:59Z");
      doTestPointFieldSortError(field, "multivalued", "1995-12-31T23:59:59Z", "2000-12-31T23:59:59Z");
      doTestPointFieldFunctionQueryError(field, "multivalued", "1995-12-31T23:59:59Z");
      doTestPointFieldFunctionQueryError(field, "multivalued", "1995-12-31T23:59:59Z", "2000-12-31T23:59:59Z");
                                
    }
    
    assertEquals("Missing types in the test", Collections.<String>emptySet(), regexToTest);
  }

  @Test
  public void testDatePointFieldFacetField() throws Exception {
    testPointFieldFacetField("number_p_dt", "number_p_dt_dv", getSequentialStringArrayWithDates(10));
    clearIndex();
    assertU(commit());
    testPointFieldFacetField("number_p_dt", "number_p_dt_dv", toStringArray(getRandomInstants(10, false)));
  }

  private static class DateGapCeiling {
    String calendarUnit = "MILLIS";
    long inCalendarUnits;
    boolean negative = false;
  
    /** Maximize calendar unit size given initialGapMillis; performs ceiling on each conversion */
    DateGapCeiling(long initialGapMillis) {
      negative = initialGapMillis < 0;
      inCalendarUnits = Math.abs(initialGapMillis);
      if (inCalendarUnits >= 1000L) {
        calendarUnit = "SECS";
        inCalendarUnits = (inCalendarUnits + 999L) / 1000L;
        if (inCalendarUnits >= 60L) {
          calendarUnit = "MINUTES";
          inCalendarUnits = (inCalendarUnits + 59L) / 60L;
          if (inCalendarUnits >= 60L) {
            calendarUnit = "HOURS";
            inCalendarUnits = (inCalendarUnits + 59L) / 60L;
            if (inCalendarUnits >= 24L) {
              calendarUnit = "DAYS";
              inCalendarUnits = (inCalendarUnits + 23L) / 24L;
              if (inCalendarUnits >= 12L) {
                calendarUnit = "MONTHS";
                inCalendarUnits = (inCalendarUnits + 11L) / 12L;
                if ((inCalendarUnits * 16) >= 487) {  // 487 = 365.25 / 12 * 16   (365.25 days/year, -ish)
                  calendarUnit = "YEARS";
                  inCalendarUnits = (16L * inCalendarUnits + 486) / 487L; 
                }
              }
            }
          }
        }
      }
    }
    @Override
    public String toString() {
      return (negative ? "-" : "+") + inCalendarUnits + calendarUnit;
    }

    public long addTo(long millis) {  // Instant.plus() doesn't work with estimated durations (MONTHS and YEARS)
      LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.ofHours(0));
      if (negative) {
        time = time.minus(inCalendarUnits, DateMathParser.CALENDAR_UNITS.get(calendarUnit));
      } else {
        time = time.plus(inCalendarUnits, DateMathParser.CALENDAR_UNITS.get(calendarUnit));
      }
      return time.atZone(ZoneOffset.ofHours(0)).toInstant().toEpochMilli();
    }
  }
  
  @Test
  public void testDatePointFieldRangeFacet() throws Exception {
    String docValuesField = "number_p_dt_dv";
    String nonDocValuesField = "number_p_dt";
    int numValues = 10 * RANDOM_MULTIPLIER;
    int numBuckets = numValues / 2;
    List<Long> values, sortedValues;
    long min, max;
    DateGapCeiling gap;
    do {
      values = getRandomLongs(numValues, false, MAX_DATE_EPOCH_MILLIS);
      sortedValues = values.stream().sorted().collect(Collectors.toList());
      min = sortedValues.get(0);
      max = sortedValues.get(sortedValues.size() - 1);
    } while (max > MAX_DATE_EPOCH_MILLIS || min <= MIN_DATE_EPOCH_MILLIS);
    long initialGap = BigInteger.valueOf(max).subtract(BigInteger.valueOf(min))
        .divide(BigInteger.valueOf(numBuckets)).longValueExact();
    gap = new DateGapCeiling(BigInteger.valueOf(max + initialGap).subtract(BigInteger.valueOf(min)) // padding for rounding
        .divide(BigInteger.valueOf(numBuckets)).longValueExact());
    int[] bucketCount = new int[numBuckets];
    int bucketNum = 0;
    long minBucketVal = min;
    // System.err.println("min:" + Instant.ofEpochMilli(min) + "   max: " + Instant.ofEpochMilli(max) + "   gap: " + gap);
    // System.err.println("bucketNum: " + bucketNum + "   minBucketVal: " + Instant.ofEpochMilli(minBucketVal));
    for (long value : sortedValues) {
      // System.err.println("value: " + Instant.ofEpochMilli(value));
      while (value >= gap.addTo(minBucketVal)) {
        ++bucketNum;
        minBucketVal = gap.addTo(minBucketVal);
        // System.err.println("bucketNum: " + bucketNum + "   minBucketVal: " + Instant.ofEpochMilli(minBucketVal));
      }
      ++bucketCount[bucketNum];
    }

    for (int i = 0 ; i < numValues ; i++) {
      assertU(adoc("id", String.valueOf(i), docValuesField, Instant.ofEpochMilli(values.get(i)).toString(),
          nonDocValuesField, Instant.ofEpochMilli(values.get(i)).toString()));
    }
    assertU(commit());

    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    String[] testStrings = new String[numBuckets + 1];
    testStrings[numBuckets] = "//*[@numFound='" + numValues + "']";
    minBucketVal = min;
    for (int i = 0 ; i < numBuckets ; ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField
          + "']/lst[@name='counts']/int[@name='" + Instant.ofEpochMilli(minBucketVal).toString() 
          + "'][.='" + bucketCount[i] + "']";
      minBucketVal = gap.addTo(minBucketVal);
    }
    long maxPlusGap = gap.addTo(max);
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, 
        "facet.range.start", Instant.ofEpochMilli(min).toString(),
        "facet.range.end", Instant.ofEpochMilli(maxPlusGap).toString(),
        "facet.range.gap", gap.toString()),
        testStrings);
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, 
        "facet.range.start", Instant.ofEpochMilli(min).toString(),
        "facet.range.end", Instant.ofEpochMilli(maxPlusGap).toString(),   
        "facet.range.gap", gap.toString(), 
        "facet.range.method", "dv"),
        testStrings);

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    minBucketVal = min;
    for (int i = 0 ; i < numBuckets ; ++i) {
      testStrings[i] = "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField
          + "']/lst[@name='counts']/int[@name='" + Instant.ofEpochMilli(minBucketVal).toString() 
          + "'][.='" + bucketCount[i] + "']";
      minBucketVal = gap.addTo(minBucketVal);
    }
    maxPlusGap = gap.addTo(max);
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, 
        "facet.range.start", Instant.ofEpochMilli(min).toString(),
        "facet.range.end", Instant.ofEpochMilli(maxPlusGap).toString(), 
        "facet.range.gap", gap.toString(), 
        "facet.range.method", "filter"),
        testStrings);
    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, 
        "facet.range.start", Instant.ofEpochMilli(min).toString(),
        "facet.range.end", Instant.ofEpochMilli(maxPlusGap).toString(), 
        "facet.range.gap", gap.toString(), 
        "facet.range.method", "dv"),
        testStrings);
  }

  @Test
  public void testDatePointStats() throws Exception {
    testDatePointStats("number_p_dt", "number_p_dt_dv", getSequentialStringArrayWithDates(10));
    testDatePointStats("number_p_dt_mv", "number_p_dt_mv_dv", getSequentialStringArrayWithDates(10));
  }

  @Test
  public void testDatePointFieldMultiValuedExactQuery() throws Exception {
    String[] dates = toStringArray(getRandomInstants(20, false));
    testPointFieldMultiValuedExactQuery("number_p_dt_mv", dates);
    testPointFieldMultiValuedExactQuery("number_p_dt_ni_mv_dv", dates);
  }

  @Test
  public void testDatePointFieldMultiValuedNonSearchableExactQuery() throws Exception {
    String[] dates = toStringArray(getRandomInstants(20, false));
    testPointFieldMultiValuedExactQuery("number_p_dt_ni_mv", dates, false);
    testPointFieldMultiValuedExactQuery("number_p_dt_ni_ns_mv", dates, false);
  }
  
  @Test
  public void testDatePointFieldMultiValuedReturn() throws Exception {
    String[] dates = toStringArray(getRandomInstants(20, false));
    testPointFieldMultiValuedReturn("number_p_dt_mv", "date", dates);
    testPointFieldMultiValuedReturn("number_p_dt_ni_mv_dv", "date", dates);
    testPointFieldMultiValuedReturn("number_p_dt_dv_ns_mv", "date", dates);
  }

  @Test
  public void testDatePointFieldMultiValuedRangeQuery() throws Exception {
    String[] dates = toStringArray(getRandomInstants(20, false).stream().sorted().collect(Collectors.toList()));
    testPointFieldMultiValuedRangeQuery("number_p_dt_mv", "date", dates);
    testPointFieldMultiValuedRangeQuery("number_p_dt_ni_mv_dv", "date", dates);
  }

  @Test
  public void testDatePointFieldMultiValuedFacetField() throws Exception {
    testPointFieldMultiValuedFacetField("number_p_dt_mv", "number_p_dt_mv_dv", getSequentialStringArrayWithDates(20));
    testPointFieldMultiValuedFacetField("number_p_dt_mv", "number_p_dt_mv_dv", toStringArray(getRandomInstants(20, false)));
  }

  @Test
  public void testDatePointFieldMultiValuedRangeFacet() throws Exception {
    doTestDatePointFieldMultiValuedRangeFacet("number_p_dt_mv_dv", "number_p_dt_mv");
  }

  @Test
  public void testDatePointMultiValuedFunctionQuery() throws Exception {
    String[] dates = toStringArray(getRandomInstants(20, false).stream().sorted().collect(Collectors.toList()));
    testPointMultiValuedFunctionQuery("number_p_dt_mv", "number_p_dt_mv_dv", "date", dates);
  }

  @Test
  public void testDatePointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testDatePointFieldsAtomicUpdates("number_p_dt");
    testDatePointFieldsAtomicUpdates("number_p_dt_dv");
    testDatePointFieldsAtomicUpdates("number_p_dt_dv_ns");
  }

  @Test
  public void testMultiValuedDatePointFieldsAtomicUpdates() throws Exception {
    if (!Boolean.getBoolean("enable.update.log")) {
      return;
    }
    testMultiValuedDatePointFieldsAtomicUpdates("number_p_dt_mv");
    testMultiValuedDatePointFieldsAtomicUpdates("number_p_dt_ni_mv_dv");
    testMultiValuedDatePointFieldsAtomicUpdates("number_p_dt_dv_ns_mv");
  }

  @Test
  public void testDatePointSetQuery() throws Exception {
    doTestSetQueries("number_p_dt", toStringArray(getRandomInstants(20, false)), false);
    doTestSetQueries("number_p_dt_mv", toStringArray(getRandomInstants(20, false)), true);
    doTestSetQueries("number_p_dt_ni_dv", toStringArray(getRandomInstants(20, false)), false);
  }
  
  
  @Test
  public void testDatePointFieldNotIndexed() throws Exception {
    String[] dates = toStringArray(getRandomInstants(10, false));
    doTestFieldNotIndexed("number_p_dt_ni", dates);
    doTestFieldNotIndexed("number_p_dt_ni_mv", dates);
  }
  
  @Test
  public void testIndexOrDocValuesQuery() throws Exception {
    String[] fieldTypeNames = new String[] { "_p_i", "_p_l", "_p_d", "_p_f", "_p_dt" };
    FieldType[] fieldTypes = new FieldType[]
        { new IntPointField(), new LongPointField(), new DoublePointField(), new FloatPointField(), new DatePointField() };
    String[] ints = toStringArray(getRandomInts(2, false).stream().sorted().collect(Collectors.toList()));
    String[] longs = toStringArray(getRandomLongs(2, false).stream().sorted().collect(Collectors.toList()));
    String[] doubles = toStringArray(getRandomDoubles(2, false).stream().sorted().collect(Collectors.toList()));
    String[] floats = toStringArray(getRandomFloats(2, false).stream().sorted().collect(Collectors.toList()));
    String[] dates = toStringArray(getRandomInstants(2, false).stream().sorted().collect(Collectors.toList()));
    String[] min = new String[] { ints[0], longs[0], doubles[0], floats[0], dates[0] };
    String[] max = new String[] { ints[1], longs[1], doubles[1], floats[1], dates[1] };
    assert fieldTypeNames.length == fieldTypes.length
        && fieldTypeNames.length == max.length
        && fieldTypeNames.length == min.length;
    for (int i = 0; i < fieldTypeNames.length; i++) {
      SchemaField fieldIndexed = h.getCore().getLatestSchema().getField("foo_" + fieldTypeNames[i]);
      SchemaField fieldIndexedAndDv = h.getCore().getLatestSchema().getField("foo_" + fieldTypeNames[i] + "_dv");
      SchemaField fieldIndexedMv = h.getCore().getLatestSchema().getField("foo_" + fieldTypeNames[i] + "_mv");
      SchemaField fieldIndexedAndDvMv = h.getCore().getLatestSchema().getField("foo_" + fieldTypeNames[i] + "_mv_dv");
      assertTrue(fieldTypes[i].getRangeQuery(null, fieldIndexed, min[i], max[i], true, true) instanceof PointRangeQuery);
      assertTrue(fieldTypes[i].getRangeQuery(null, fieldIndexedAndDv, min[i], max[i], true, true) instanceof IndexOrDocValuesQuery);
      assertTrue(fieldTypes[i].getRangeQuery(null, fieldIndexedMv, min[i], max[i], true, true) instanceof PointRangeQuery);
      assertTrue(fieldTypes[i].getRangeQuery(null, fieldIndexedAndDvMv, min[i], max[i], true, true) instanceof IndexOrDocValuesQuery);
      assertTrue(fieldTypes[i].getFieldQuery(null, fieldIndexed, min[i]) instanceof PointRangeQuery);
      assertTrue(fieldTypes[i].getFieldQuery(null, fieldIndexedAndDv, min[i]) instanceof IndexOrDocValuesQuery);
      assertTrue(fieldTypes[i].getFieldQuery(null, fieldIndexedMv, min[i]) instanceof PointRangeQuery);
      assertTrue(fieldTypes[i].getFieldQuery(null, fieldIndexedAndDvMv, min[i]) instanceof IndexOrDocValuesQuery);
    }
  }
  
  public void testInternals() throws IOException {
    String[] types = new String[]{"i", "l", "f", "d", "dt"};
    String[][] values = new String[][] {
        toStringArray(getRandomInts(10, false)),
        toStringArray(getRandomLongs(10, false)),
        toStringArray(getRandomFloats(10, false)),
        toStringArray(getRandomDoubles(10, false)),
        toStringArray(getRandomInstants(10, false))
    };
    assertEquals(types.length, values.length);
    Set<String> typesTested = new HashSet<>();
    for (int i = 0 ; i < types.length ; ++i) {
      for (String suffix:FIELD_SUFFIXES) {
        doTestInternals("number_p_" + types[i] + suffix, values[i]);
        typesTested.add("*_p_" + types[i] + suffix);
      }
    }
    assertEquals("Missing types in the test", dynFieldRegexesForType(PointField.class), typesTested);
  }
  
  // Helper methods

  /**
   * Given a FieldType, return the list of DynamicField 'regexes' for all declared 
   * DynamicFields that use that FieldType.
   *
   * @see IndexSchema#getDynamicFields
   * @see DynamicField#getRegex
   */
  private static SortedSet<String> dynFieldRegexesForType(final Class<? extends FieldType> clazz) {
    SortedSet<String> typesToTest = new TreeSet<>();
    for (DynamicField dynField : h.getCore().getLatestSchema().getDynamicFields()) {
      if (clazz.isInstance(dynField.getPrototype().getType())) {
        typesToTest.add(dynField.getRegex());
      }
    }
    return typesToTest;
  }
  
  private <T> List<T> getRandomList(int length, boolean missingVals, Supplier<T> randomVal) {
    List<T> list = new ArrayList<>(length);
    for (int i = 0 ; i < length ; ++i) {
      T val = null; 
      // Sometimes leave val as null when we're producing missing values
      if (missingVals == false || usually()) {
        val = randomVal.get();
      }
      list.add(val);
    }
    return list;
  }

  private List<Double> getRandomDoubles(int length, boolean missingVals) {
    return getRandomList(length, missingVals, () -> {
      Double d = Double.NaN; 
      while (d.isNaN()) {
        d = Double.longBitsToDouble(random().nextLong());
      }
      return d; 
    });
  }

  private List<Float> getRandomFloats(int length, boolean missingVals) {
    return getRandomList(length, missingVals, () -> {
      Float f = Float.NaN;
      while (f.isNaN()) {
        f = Float.intBitsToFloat(random().nextInt());
      }
      return f;
    });
  }

  private List<Integer> getRandomInts(int length, boolean missingVals, int bound) {
    return getRandomList(length, missingVals, () -> random().nextInt() % bound);
  }

  private List<Integer> getRandomInts(int length, boolean missingVals) {
    return getRandomList(length, missingVals, () -> random().nextInt());
  }

  private List<Long> getRandomLongs(int length, boolean missingVals, long bound) {
    assert bound > 0L;
    return getRandomList(length, missingVals, () -> random().nextLong() % bound); // see Random.nextInt(int bound)
  }

  private List<Long> getRandomLongs(int length, boolean missingVals) {
    return getRandomList(length, missingVals, () -> random().nextLong());
  }

  private List<Instant> getRandomInstants(int length, boolean missingVals) {
    return getRandomList(length, missingVals, () -> Instant.ofEpochMilli(random().nextLong()));
  }
  
  private String[] getSequentialStringArrayWithInts(int length) {
    String[] arr = new String[length];
    for (int i = 0; i < length; i++) {
      arr[i] = String.valueOf(i);
    }
    return arr;
  }

  private String[] getSequentialStringArrayWithDates(int length) {
    assert length < 60;
    String[] arr = new String[length];
    for (int i = 0; i < length; i++) {
      arr[i] = String.format(Locale.ROOT, "1995-12-11T19:59:%02dZ", i);
    }
    return arr;
  }
  
  private String[] getSequentialStringArrayWithDoubles(int length) {
    String[] arr = new String[length];
    for (int i = 0; i < length; i++) {
      arr[i] = String.format(Locale.ROOT, "%d.0", i);
    }
    return arr;
  }

  private void doTestFieldNotIndexed(String field, String[] values) throws IOException {
    assert values.length == 10;
    // test preconditions
    SchemaField sf = h.getCore().getLatestSchema().getField(field);
    assertFalse("Field should be indexed=false", sf.indexed());
    assertFalse("Field should be docValues=false", sf.hasDocValues());
    
    for (int i=0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), field, values[i]));
    }
    assertU(commit());
    assertQ(req("q", "*:*"), "//*[@numFound='10']");
    assertQ("Can't search on index=false docValues=false field", req("q", field + ":[* TO *]"), "//*[@numFound='0']");
    IndexReader ir;
    RefCounted<SolrIndexSearcher> ref = null;
    try {
      ref = h.getCore().getSearcher();
      ir = ref.get().getIndexReader();
      assertEquals("Field " + field + " should have no point values", 0, PointValues.size(ir, field));
    } finally {
      ref.decref();
    }
  }
  
   
  private void doTestIntPointFieldExactQuery(final String field, final boolean testLong) throws Exception {
    doTestIntPointFieldExactQuery(field, testLong, true);
  }

  private String getTestString(boolean searchable, int numFound) {
    return "//*[@numFound='" + (searchable ? Integer.toString(numFound) : "0") + "']";
  }
  
  /**
   * @param field the field to use for indexing and searching against
   * @param testLong set to true if "field" is expected to support long values, false if only integers
   * @param searchable set to true if searches against "field" should succeed, false if field is only stored and searches should always get numFound=0
   */
  private void doTestIntPointFieldExactQuery(final String field, final boolean testLong, final boolean searchable) throws Exception {
    int numValues = 10 * RANDOM_MULTIPLIER;
    Map<String,Integer> randCount = new HashMap<>(numValues);
    String[] rand = testLong ? toStringArray(getRandomLongs(numValues, false))
                             : toStringArray(getRandomInts(numValues, false));
    for (int i = 0 ; i < numValues ; i++) {
      randCount.merge(rand[i], 1, (a, b) -> a + b); // count unique values
      assertU(adoc("id", String.valueOf(i), field, rand[i]));
    }
    assertU(commit());

    for (int i = 0 ; i < numValues ; i++) {
      assertQ(req("q", field + ":" + (rand[i].startsWith("-") ? "\\" : "") + rand[i],
          "fl", "id," + field), getTestString(searchable, randCount.get(rand[i])));
    }
    
    StringBuilder builder = new StringBuilder();
    for (String value : randCount.keySet()) {
      if (builder.length() != 0) {
        builder.append(" OR ");
      }
      if (value.startsWith("-")) {
        builder.append("\\"); // escape negative sign
      }
      builder.append(value);
    }
    assertQ(req("debug", "true", "q", field + ":(" + builder.toString() + ")"), getTestString(searchable, numValues));
    
    assertU(adoc("id", String.valueOf(Integer.MAX_VALUE), field, String.valueOf(Integer.MAX_VALUE)));
    assertU(commit());
    assertQ(req("q", field + ":"+Integer.MAX_VALUE, "fl", "id, " + field), getTestString(searchable, 1));
    
    clearIndex();
    assertU(commit());
  }

  private void testPointFieldReturn(String field, String type, String[] values) throws Exception {
    SchemaField sf = h.getCore().getLatestSchema().getField(field);
    assert sf.stored() || (sf.hasDocValues() && sf.useDocValuesAsStored()): 
      "Unexpected field definition for " + field; 
    for (int i=0; i < values.length; i++) {
      assertU(adoc("id", String.valueOf(i), field, values[i]));
    }
    // Check using RTG
    if (Boolean.getBoolean("enable.update.log")) {
      for (int i = 0; i < values.length; i++) {
        assertQ(req("qt", "/get", "id", String.valueOf(i)),
            "//doc/" + type + "[@name='" + field + "'][.='" + values[i] + "']");
      }
    }
    assertU(commit());
    String[] expected = new String[values.length + 1];
    expected[0] = "//*[@numFound='" + values.length + "']"; 
    for (int i = 0; i < values.length; i++) {
      expected[i + 1] = "//result/doc[str[@name='id']='" + i + "']/" + type + "[@name='" + field + "'][.='" + values[i] + "']";
    }
    assertQ(req("q", "*:*", "fl", "id, " + field, "rows", String.valueOf(values.length)), expected);

    // Check using RTG
    if (Boolean.getBoolean("enable.update.log")) {
      for (int i = 0; i < values.length; i++) {
        assertQ(req("qt", "/get", "id", String.valueOf(i)),
            "//doc/" + type + "[@name='" + field + "'][.='" + values[i] + "']");
      }
    }
    clearIndex();
    assertU(commit());
  }

  private void doTestPointFieldNonSearchableRangeQuery(String fieldName, String... values) throws Exception {
    for (int i = 9; i >= 0; i--) {
      SolrInputDocument doc = sdoc("id", String.valueOf(i));
      for (String value : values) {
        doc.addField(fieldName, value);
      }
      assertU(adoc(doc));
    }
    assertU(commit());
    assertQ(req("q", fieldName + ":[* TO *]", "fl", "id, " + fieldName, "sort", "id asc"), 
            "//*[@numFound='0']");
  }

  private void doTestIntPointFieldRangeQuery(String fieldName, String type, boolean testLong) throws Exception {
    for (int i = 9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), fieldName, String.valueOf(i)));
    }
    assertU(commit());
    assertQ(req("q", fieldName + ":[0 TO 3]", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='4']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='2']",
        "//result/doc[4]/" + type + "[@name='" + fieldName + "'][.='3']");
    
    assertQ(req("q", fieldName + ":{0 TO 3]", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='3']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='2']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='3']");
    
    assertQ(req("q", fieldName + ":[0 TO 3}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='3']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='2']");
    
    assertQ(req("q", fieldName + ":{0 TO 3}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='2']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='2']");
    
    assertQ(req("q", fieldName + ":{0 TO *}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='9']",
        "0=count(//result/doc/" + type + "[@name='" + fieldName + "'][.='0'])",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1']");
    
    assertQ(req("q", fieldName + ":{* TO 3}", "fl", "id, " + fieldName, "sort", "id desc"), 
        "//*[@numFound='3']",
        "0=count(//result/doc/" + type + "[@name='" + fieldName + "'][.='3'])",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='2']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='0']");
    
    assertQ(req("q", fieldName + ":[* TO 3}", "fl", "id, " + fieldName, "sort", "id desc"), 
        "//*[@numFound='3']",
        "0=count(//result/doc/" + type + "[@name='" + fieldName + "'][.='3'])",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='2']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='0']");
    
    assertQ(req("q", fieldName + ":[* TO *}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0']",
        "//result/doc[10]/" + type + "[@name='" + fieldName + "'][.='9']");
    
    assertQ(req("q", fieldName + ":[0 TO 1] OR " + fieldName + ":[8 TO 9]" , "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='4']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='8']",
        "//result/doc[4]/" + type + "[@name='" + fieldName + "'][.='9']");
    
    assertQ(req("q", fieldName + ":[0 TO 1] AND " + fieldName + ":[1 TO 2]" , "fl", "id, " + fieldName), 
        "//*[@numFound='1']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1']");
    
    assertQ(req("q", fieldName + ":[0 TO 1] AND NOT " + fieldName + ":[1 TO 2]" , "fl", "id, " + fieldName), 
        "//*[@numFound='1']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0']");

    clearIndex();
    assertU(commit());
    
    String[] arr;
    if (testLong) {
      arr = toAscendingStringArray(getRandomLongs(100, false), true);
    } else {
      arr = toAscendingStringArray(getRandomInts(100, false), true);
    }
    for (int i = 0; i < arr.length; i++) {
      assertU(adoc("id", String.valueOf(i), fieldName, arr[i]));
    }
    assertU(commit());
    for (int i = 0; i < arr.length; i++) {
      assertQ(req("q", fieldName + ":[" + arr[0] + " TO " + arr[i] + "]", "fl", "id, " + fieldName), 
          "//*[@numFound='" + (i + 1) + "']");
      assertQ(req("q", fieldName + ":{" + arr[0] + " TO " + arr[i] + "}", "fl", "id, " + fieldName), 
          "//*[@numFound='" + (Math.max(0,  i-1)) + "']");
      assertQ(req("q", fieldName + ":[" + arr[0] + " TO " + arr[i] + "] AND " + fieldName + ":" + arr[0].replace("-", "\\-"), "fl", "id, " + fieldName), 
          "//*[@numFound='1']");
    }
  }
  
  private void testPointFieldFacetField(String nonDocValuesField, String docValuesField, String[] numbers) throws Exception {
    assert numbers != null && numbers.length == 10;
    
    assertFalse(h.getCore().getLatestSchema().getField(docValuesField).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), docValuesField, numbers[i], nonDocValuesField, numbers[i]));
    }
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "id, " + docValuesField, "facet", "true", "facet.field", docValuesField), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[1] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[2] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[3] + "'][.='1']");
    
    assertU(adoc("id", "10", docValuesField, numbers[1], nonDocValuesField, numbers[1]));
    
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "id, " + docValuesField, "facet", "true", "facet.field", docValuesField), 
        "//*[@numFound='11']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[1] + "'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[2] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + docValuesField +"']/int[@name='" + numbers[3] + "'][.='1']");
    
    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    assertQEx("Expecting Exception", 
        "Can't facet on a PointField without docValues", 
        req("q", "*:*", "fl", "id, " + nonDocValuesField, "facet", "true", "facet.field", nonDocValuesField), 
        SolrException.ErrorCode.BAD_REQUEST);
  }
  
  private void doTestIntPointFunctionQuery(String field, String type) throws Exception {
    for (int i = 9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), field, String.valueOf(i)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(field).getType() instanceof PointField);
    
    assertQ(req("q", "*:*", "fl", "id, " + field, "sort", "product(-1," + field + ") asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='9']",
        "//result/doc[2]/" + type + "[@name='" + field + "'][.='8']",
        "//result/doc[3]/" + type + "[@name='" + field + "'][.='7']",
        "//result/doc[10]/" + type + "[@name='" + field + "'][.='0']");
    
    assertQ(req("q", "*:*", "fl", "id, " + field + ", product(-1," + field + ")", "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/float[@name='product(-1," + field + ")'][.='-0.0']",
        "//result/doc[2]/float[@name='product(-1," + field + ")'][.='-1.0']",
        "//result/doc[3]/float[@name='product(-1," + field + ")'][.='-2.0']",
        "//result/doc[10]/float[@name='product(-1," + field + ")'][.='-9.0']");
    
    assertQ(req("q", "*:*", "fl", "id, " + field + ", field(" + field + ")", "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='field(" + field + ")'][.='0']",
        "//result/doc[2]/" + type + "[@name='field(" + field + ")'][.='1']",
        "//result/doc[3]/" + type + "[@name='field(" + field + ")'][.='2']",
        "//result/doc[10]/" + type + "[@name='field(" + field + ")'][.='9']");
    
  }

  /** 
   * Checks that the specified field can not be used as a value source, even if there are documents 
   * with (all) the specified values in the index.
   *
   * @param field the field name to try and sort on
   * @param errSubStr substring to look for in the error msg
   * @param values one or more values to put into the doc(s) in the index - may be more then one for multivalued fields
   */
  private void doTestPointFieldFunctionQueryError(String field, String errSubStr, String...values) throws Exception {
    final int numDocs = atLeast(random(), 10);
    for (int i = 0; i < numDocs; i++) {
      SolrInputDocument doc = sdoc("id", String.valueOf(i));
      for (String v: values) {
        doc.addField(field, v);
      }
      assertU(adoc(doc));
    }

    assertQEx("Should not be able to use field in function: " + field, errSubStr,
              req("q", "*:*", "fl", "id", "fq", "{!frange l=0 h=100}product(-1, " + field + ")"), 
              SolrException.ErrorCode.BAD_REQUEST);
    
    clearIndex();
    assertU(commit());
    
    // empty index should (also) give same error
    assertQEx("Should not be able to use field in function: " + field, errSubStr,
              req("q", "*:*", "fl", "id", "fq", "{!frange l=0 h=100}product(-1, " + field + ")"), 
              SolrException.ErrorCode.BAD_REQUEST);
    
  }

  
  private void testPointStats(String field, String dvField, String[] numbers, double min, double max, int count, int missing, double delta) {
    String minMin = String.valueOf(min - Math.abs(delta*min));
    String maxMin = String.valueOf(min + Math.abs(delta*min));
    String minMax = String.valueOf(max - Math.abs(delta*max));
    String maxMax = String.valueOf(max + Math.abs(delta*max));
    for (int i = 0; i < numbers.length; i++) {
      assertU(adoc("id", String.valueOf(i), dvField, numbers[i], field, numbers[i]));
    }
    assertU(adoc("id", String.valueOf(numbers.length)));
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(dvField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(dvField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id, " + dvField, "stats", "true", "stats.field", dvField), 
        "//*[@numFound='" + (numbers.length + 1) + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/double[@name='min'][.>=" + minMin + "]",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/double[@name='min'][.<=" + maxMin+ "]",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/double[@name='max'][.>=" + minMax + "]",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/double[@name='max'][.<=" + maxMax + "]",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/long[@name='count'][.='" + count + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/long[@name='missing'][.='" + missing + "']");
    
    assertFalse(h.getCore().getLatestSchema().getField(field).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(field).getType() instanceof PointField);
    assertQEx("Expecting Exception", 
        "Can't calculate stats on a PointField without docValues", 
        req("q", "*:*", "fl", "id, " + field, "stats", "true", "stats.field", field), 
        SolrException.ErrorCode.BAD_REQUEST);
  }


  private void testPointFieldMultiValuedExactQuery(final String fieldName, final String[] numbers) throws Exception {
    testPointFieldMultiValuedExactQuery(fieldName, numbers, true);
  }

  /**
   * @param fieldName the field to use for indexing and searching against
   * @param numbers list of 20 values to index in 10 docs (pairwise)
   * @param searchable set to true if searches against "field" should succeed, false if field is only stored and searches should always get numFound=0
   */
  private void testPointFieldMultiValuedExactQuery(final String fieldName, final String[] numbers,
                                                   final boolean searchable) throws Exception {
    
    final String MATCH_ONE = "//*[@numFound='" + (searchable ? "1" : "0") + "']";
    final String MATCH_TWO = "//*[@numFound='" + (searchable ? "2" : "0") + "']";
    
    assert numbers != null && numbers.length == 20;
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).getType() instanceof PointField);
    for (int i=0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), fieldName, numbers[i], fieldName, numbers[i+10]));
    }
    assertU(commit());
    FieldType type = h.getCore().getLatestSchema().getField(fieldName).getType();
    for (int i = 0; i < 20; i++) {
      if (type instanceof DatePointField) {
        assertQ(req("q", fieldName + ":\"" + numbers[i] + "\""),
                MATCH_ONE);
      } else {
        assertQ(req("q", fieldName + ":" + numbers[i].replace("-", "\\-")),
                MATCH_ONE);
      }
    }
    
    for (int i = 0; i < 20; i++) {
      if (type instanceof DatePointField) {
        assertQ(req("q", fieldName + ":\"" + numbers[i] + "\"" + " OR " + fieldName + ":\"" + numbers[(i+1)%10]+"\""),
                MATCH_TWO);
      } else {
        assertQ(req("q", fieldName + ":" + numbers[i].replace("-", "\\-") + " OR " + fieldName + ":" + numbers[(i+1)%10].replace("-", "\\-")),
                MATCH_TWO);
      }
    }
  }
  
  private void testPointFieldMultiValuedReturn(String fieldName, String type, String[] numbers) throws Exception {
    assert numbers != null && numbers.length == 20;
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).getType() instanceof PointField);
    for (int i=9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), fieldName, numbers[i], fieldName, numbers[i+10]));
    }
    // Check using RTG before commit
    if (Boolean.getBoolean("enable.update.log")) {
      for (int i = 0; i < 10; i++) {
        assertQ(req("qt", "/get", "id", String.valueOf(i)),
            "//doc/arr[@name='" + fieldName + "']/" + type + "[.='" + numbers[i] + "']",
            "//doc/arr[@name='" + fieldName + "']/" + type + "[.='" + numbers[i+10] + "']",
            "count(//doc/arr[@name='" + fieldName + "']/" + type + ")=2");
      }
    }
    // Check using RTG after commit
    assertU(commit());
    if (Boolean.getBoolean("enable.update.log")) {
      for (int i = 0; i < 10; i++) {
        assertQ(req("qt", "/get", "id", String.valueOf(i)),
            "//doc/arr[@name='" + fieldName + "']/" + type + "[.='" + numbers[i] + "']",
            "//doc/arr[@name='" + fieldName + "']/" + type + "[.='" + numbers[i+10] + "']",
            "count(//doc/arr[@name='" + fieldName + "']/" + type + ")=2");
      }
    }
    String[] expected = new String[21];
    expected[0] = "//*[@numFound='10']"; 
    for (int i = 1; i <= 10; i++) {
      // checks for each doc's two values aren't next to eachother in array, but that doesn't matter for correctness
      expected[i] = "//result/doc[" + i + "]/arr[@name='" + fieldName + "']/" + type + "[.='" + numbers[i-1] + "']";
      expected[i+10] = "//result/doc[" + i + "]/arr[@name='" + fieldName + "']/" + type + "[.='" + numbers[i + 9] + "']";
    }
    assertQ(req("q", "*:*", "fl", "id, " + fieldName, "sort","id asc"), expected);
  }

  private void testPointFieldMultiValuedRangeQuery(String fieldName, String type, String[] numbers) throws Exception {
    assert numbers != null && numbers.length == 20;
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(fieldName).getType() instanceof PointField);
    for (int i=9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), fieldName, numbers[i], fieldName, numbers[i+10]));
    }
    assertU(commit());
    assertQ(req("q", String.format(Locale.ROOT, "%s:[%s TO %s]", fieldName, numbers[0], numbers[3]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='4']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[2][.='" + numbers[10] + "']",
        "//result/doc[2]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[1] + "']",
        "//result/doc[2]/arr[@name='" + fieldName + "']/" + type + "[2][.='" + numbers[11] + "']",
        "//result/doc[3]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[2] + "']",
        "//result/doc[3]/arr[@name='" + fieldName + "']/" + type + "[2][.='" + numbers[12] + "']",
        "//result/doc[4]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[3] + "']",
        "//result/doc[4]/arr[@name='" + fieldName + "']/" + type + "[2][.='" + numbers[13] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:{%s TO %s]", fieldName, numbers[0], numbers[3]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[1] + "']",
        "//result/doc[2]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[2] + "']",
        "//result/doc[3]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[3] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:[%s TO %s}", fieldName, numbers[0], numbers[3]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']",
        "//result/doc[2]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[1] + "']",
        "//result/doc[3]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[2] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:{%s TO %s}", fieldName, numbers[0], numbers[3]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[1] + "']",
        "//result/doc[2]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[2] + "']");

    assertQ(req("q", String.format(Locale.ROOT, "%s:{%s TO *}", fieldName, numbers[0]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='10']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:{%s TO *}", fieldName, numbers[10]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='9']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[1] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:{* TO %s}", fieldName, numbers[3]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:[* TO %s}", fieldName, numbers[3]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']");
    
    assertQ(req("q", fieldName + ":[* TO *}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']",
        "//result/doc[10]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[9] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:[%s TO %s] OR %s:[%s TO %s]", fieldName, numbers[0], numbers[1], fieldName, numbers[8], numbers[9]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='4']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']",
        "//result/doc[2]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[1] + "']",
        "//result/doc[3]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[8] + "']",
        "//result/doc[4]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[9] + "']");
    
    assertQ(req("q", String.format(Locale.ROOT, "%s:[%s TO %s] OR %s:[%s TO %s]", fieldName, numbers[0], numbers[0], fieldName, numbers[10], numbers[10]),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/arr[@name='" + fieldName + "']/" + type + "[1][.='" + numbers[0] + "']");
  }

  private void testPointFieldMultiValuedFacetField(String nonDocValuesField, String dvFieldName, String[] numbers) throws Exception {
    assert numbers != null && numbers.length == 20;
    assertTrue(h.getCore().getLatestSchema().getField(dvFieldName).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(dvFieldName).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(dvFieldName).getType() instanceof PointField);
    
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), dvFieldName, numbers[i], dvFieldName, numbers[i + 10], 
          nonDocValuesField, numbers[i], nonDocValuesField, numbers[i + 10]));
     if (rarely()) {
       assertU(commit());
     }
    }
    assertU(commit());
    
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[1] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[2] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[3] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[10] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[11] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[12] + "'][.='1']");
    
    assertU(adoc("id", "10", dvFieldName, numbers[1], nonDocValuesField, numbers[1]));
    
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName), 
        "//*[@numFound='11']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[1] + "'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[2] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[3] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[10] + "'][.='1']");
    
    assertU(adoc("id", "10", dvFieldName, numbers[1], nonDocValuesField, numbers[1], dvFieldName, numbers[1], nonDocValuesField, numbers[1]));
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName, "facet.missing", "true"), 
        "//*[@numFound='11']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[1] + "'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[2] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[3] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[10] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[not(@name)][.='0']"
        );
    
    assertU(adoc("id", "10")); // add missing values
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName, "facet.missing", "true"), 
        "//*[@numFound='11']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[1] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[2] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[3] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[10] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[not(@name)][.='1']"
        );
    
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName, "facet.mincount", "3"), 
        "//*[@numFound='11']",
        "count(//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int)=0");
    
    assertQ(req("q", "id:0", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName), 
        "//*[@numFound='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[0] + "'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + numbers[10] + "'][.='1']",
        "count(//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int)=2");
    
    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    assertQEx("Expecting Exception", 
        "Can't facet on a PointField without docValues", 
        req("q", "*:*", "fl", "id, " + nonDocValuesField, "facet", "true", "facet.field", nonDocValuesField), 
        SolrException.ErrorCode.BAD_REQUEST);
    clearIndex();
    assertU(commit());
    
    String smaller, larger;
    try {
      if (Long.parseLong(numbers[1]) < Long.parseLong(numbers[2])) {
        smaller = numbers[1];
        larger = numbers[2];
      } else {
        smaller = numbers[2];
        larger = numbers[1];
      }
    } catch (NumberFormatException e) {
      try {
        if (Double.valueOf(numbers[1]) < Double.valueOf(numbers[2])) {
          smaller = numbers[1];
          larger = numbers[2];
        } else {
          smaller = numbers[2];
          larger = numbers[1];
        }
      } catch (NumberFormatException e2) {
        if (DateMathParser.parseMath(null, numbers[1]).getTime() < DateMathParser.parseMath(null, numbers[2]).getTime()) {
          smaller = numbers[1];
          larger = numbers[2];
        } else {
          smaller = numbers[2];
          larger = numbers[1];
        }
      }
    }
    
    assertU(adoc("id", "1", dvFieldName, smaller, dvFieldName, larger));
    assertU(adoc("id", "2", dvFieldName, larger));
    assertU(commit());
    
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName), 
        "//*[@numFound='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + larger + "'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + smaller + "'][.='1']",
        "count(//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int)=2");
    
    assertQ(req("q", "*:*", "fl", "id, " + dvFieldName, "facet", "true", "facet.field", dvFieldName, "facet.sort", "index"), 
        "//*[@numFound='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='" + smaller +"'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int[@name='"+ larger + "'][.='2']",
        "count(//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + dvFieldName +"']/int)=2");
    
    clearIndex();
    assertU(commit());

  }

  private void testPointMultiValuedFunctionQuery(String nonDocValuesField, String docValuesField, String type, String[] numbers) throws Exception {
    assert numbers != null && numbers.length == 20;
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), docValuesField, numbers[i], docValuesField, numbers[i+10], 
          nonDocValuesField, numbers[i], nonDocValuesField, numbers[i+10]));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    String function = "field(" + docValuesField + ", min)";
    
    assertQ(req("q", "*:*", "fl", "id, " + docValuesField, "sort", function + " desc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/str[@name='id'][.='9']",
        "//result/doc[2]/str[@name='id'][.='8']",
        "//result/doc[3]/str[@name='id'][.='7']",
        "//result/doc[10]/str[@name='id'][.='0']");

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);

    function = "field(" + nonDocValuesField + ",min)";
    
    assertQEx("Expecting Exception", 
        "sort param could not be parsed as a query", 
        req("q", "*:*", "fl", "id", "sort", function + " desc"), 
        SolrException.ErrorCode.BAD_REQUEST);
    
    assertQEx("Expecting Exception", 
        "docValues='true' is required to select 'min' value from multivalued field (" + nonDocValuesField + ") at query time", 
        req("q", "*:*", "fl", "id, " + function), 
        SolrException.ErrorCode.BAD_REQUEST);
    
    function = "field(" + docValuesField + ",foo)";
    assertQEx("Expecting Exception", 
        "Multi-Valued field selector 'foo' not supported", 
        req("q", "*:*", "fl", "id, " + function), 
        SolrException.ErrorCode.BAD_REQUEST);
  }

  private void testMultiValuedIntPointFieldsAtomicUpdates(String field, String type) throws Exception {
    assertU(adoc(sdoc("id", "1", field, "1")));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='1']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=1");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("add", 2))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='1']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='2']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=2");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("remove", 1))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='2']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=1");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", ImmutableList.of(1, 2, 3)))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='1']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='2']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='3']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=3");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("removeregex", ".*"))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=0");
    
  }
  
  private void testMultiValuedFloatPointFieldsAtomicUpdates(String field, String type) throws Exception {
    assertU(adoc(sdoc("id", "1", field, "1.0")));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='1.0']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=1");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("add", 2.1f))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='1.0']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='2.1']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=2");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("remove", 1f))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='2.1']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=1");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", ImmutableList.of(1f, 2f, 3f)))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='1.0']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='2.0']",
        "//result/doc[1]/arr[@name='" + field + "']/" + type + "[.='3.0']",
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=3");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("removeregex", ".*"))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "count(//result/doc[1]/arr[@name='" + field + "']/" + type + ")=0");
    
  }
  
  private void testIntPointFieldsAtomicUpdates(String field, String type) throws Exception {
    assertU(adoc(sdoc("id", "1", field, "1")));
    assertU(commit());

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("inc", 1))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='2']");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("inc", -1))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='1']");
    
    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", 3))));
    assertU(commit());
    
    assertQ(req("q", "id:1"),
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='3']");
  }

  
  private void doTestFloatPointFieldExactQuery(final String field, boolean testDouble) throws Exception {
    doTestFloatPointFieldExactQuery(field, true, testDouble);
  }
  /**
   * @param field the field to use for indexing and searching against
   * @param searchable set to true if searches against "field" should succeed, false if field is only stored and searches should always get numFound=0
   */
  private void doTestFloatPointFieldExactQuery(final String field, final boolean searchable, final boolean testDouble) 
      throws Exception {
    int numValues = 10 * RANDOM_MULTIPLIER;
    Map<String,Integer> randCount = new HashMap<>(numValues);
    String[] rand = testDouble ? toStringArray(getRandomDoubles(numValues, false)) 
                               : toStringArray(getRandomFloats(numValues, false));
    for (int i = 0 ; i < numValues ; i++) {
      randCount.merge(rand[i], 1, (a, b) -> a + b); // count unique values
      assertU(adoc("id", String.valueOf(i), field, rand[i]));
    }
    assertU(commit());

    for (int i = 0 ; i < numValues ; i++) {
      assertQ(req("q", field + ":" + (rand[i].startsWith("-") ? "\\" : "") + rand[i],
          "fl", "id," + field), getTestString(searchable, randCount.get(rand[i])));
    }

    StringBuilder builder = new StringBuilder();
    for (String value : randCount.keySet()) {
      if (builder.length() != 0) {
        builder.append(" OR ");
      }
      if (value.startsWith("-")) {
        builder.append("\\"); // escape negative sign
      }
      builder.append(value);
    }
    assertQ(req("debug", "true", "q", field + ":(" + builder.toString() + ")"), getTestString(searchable, numValues));

    clearIndex();
    assertU(commit());
  }

  /**
   * For each value, creates a doc with that value in the specified field and then asserts that
   * asc/desc sorts on that field succeeds and that the docs are in the (relatively) expected order
   *
   * @param field name of field to sort on
   * @param values list of values in ascending order
   */
  private <T extends Comparable<T>> void doTestPointFieldSort(String field, List<T> values) throws Exception {
    assert values != null && 2 <= values.size();
 
    final List<SolrInputDocument> docs = new ArrayList<>(values.size());
    final String[] ascXpathChecks = new String[values.size() + 1];
    final String[] descXpathChecks = new String[values.size() + 1];
    ascXpathChecks[values.size()] = "//*[@numFound='" + values.size() + "']";
    descXpathChecks[values.size()] = "//*[@numFound='" + values.size() + "']";
    
    boolean missingFirst = field.endsWith("_sml") == false;
    
    List<PosVal<T>> ascendingPosVals = toAscendingPosVals(values, missingFirst);
    for (int i = ascendingPosVals.size() - 1 ; i >= 0 ; --i) {
      T value = ascendingPosVals.get(i).val;
      if (value == null) {
        docs.add(sdoc("id", String.valueOf(i))); // null => missing value
      } else {
        docs.add(sdoc("id", String.valueOf(i), field, String.valueOf(value)));
      }
      // reminder: xpath array indexes start at 1
      ascXpathChecks[i]= "//result/doc["+ (1 + i)+"]/str[@name='id'][.='"+i+"']";
    }
    List<PosVal<T>> descendingPosVals = toDescendingPosVals
        (ascendingPosVals.stream().map(pv->pv.val).collect(Collectors.toList()), missingFirst);
    for (int i = descendingPosVals.size() - 1 ; i >= 0 ; --i) {
      descXpathChecks[i]= "//result/doc[" + (i + 1) + "]/str[@name='id'][.='" + descendingPosVals.get(i).pos + "']";
    }
    
    // ensure doc add order doesn't affect results
    Collections.shuffle(docs, random());
    for (SolrInputDocument doc : docs) {
      assertU(adoc(doc));
    }
    assertU(commit());

    assertQ(req("q", "*:*", "fl", "id, " + field, "sort", field + " asc, id asc"), 
            ascXpathChecks);
    assertQ(req("q", "*:*", "fl", "id, " + field, "sort", field + " desc, id desc"), 
            descXpathChecks);
        
    clearIndex();
    assertU(commit());
  }


  /** 
   * Checks that the specified field can not be sorted on, even if there are documents 
   * with (all) the specified values in the index.
   *
   * @param field the field name to try and sort on
   * @param errSubStr substring to look for in the error msg
   * @param values one or more values to put into the doc(s) in the index - may be more then one for multivalued fields
   */
  private void doTestPointFieldSortError(String field, String errSubStr, String... values) throws Exception {

    final int numDocs = atLeast(random(), 10);
    for (int i = 0; i < numDocs; i++) {
      SolrInputDocument doc = sdoc("id", String.valueOf(i));
      for (String v: values) {
        doc.addField(field, v);
      }
      assertU(adoc(doc));
    }

    assertQEx("Should not be able to sort on field: " + field, errSubStr,
              req("q", "*:*", "fl", "id", "sort", field + " desc"), 
              SolrException.ErrorCode.BAD_REQUEST);
    
    clearIndex();
    assertU(commit());
    
    // empty index should (also) give same error
    assertQEx("Should not be able to sort on field: " + field, errSubStr,
              req("q", "*:*", "fl", "id", "sort", field + " desc"), 
              SolrException.ErrorCode.BAD_REQUEST);
    
  }
  
  private void doTestFloatPointFieldRangeQuery(String fieldName, String type, boolean testDouble) throws Exception {
    for (int i = 9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), fieldName, String.valueOf(i)));
    }
    assertU(commit());
    assertQ(req("q", fieldName + ":[0 TO 3]", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='4']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0.0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1.0']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='2.0']",
        "//result/doc[4]/" + type + "[@name='" + fieldName + "'][.='3.0']");
    
    assertQ(req("q", fieldName + ":{0 TO 3]", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='3']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1.0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='2.0']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='3.0']");
    
    assertQ(req("q", fieldName + ":[0 TO 3}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='3']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0.0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1.0']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='2.0']");
    
    assertQ(req("q", fieldName + ":{0 TO 3}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='2']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1.0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='2.0']");
    
    assertQ(req("q", fieldName + ":{0 TO *}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='9']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1.0']");
    
    assertQ(req("q", fieldName + ":{* TO 3}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='3']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0.0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1.0']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='2.0']");
    
    assertQ(req("q", fieldName + ":[* TO 3}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='3']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0.0']",
        "//result/doc[2]/" + type + "[@name='" + fieldName + "'][.='1.0']",
        "//result/doc[3]/" + type + "[@name='" + fieldName + "'][.='2.0']");
    
    assertQ(req("q", fieldName + ":[* TO *}", "fl", "id, " + fieldName, "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='0.0']",
        "//result/doc[10]/" + type + "[@name='" + fieldName + "'][.='9.0']");
    
    assertQ(req("q", fieldName + ":[0.9 TO 1.01]", "fl", "id, " + fieldName), 
        "//*[@numFound='1']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1.0']");
    
    assertQ(req("q", fieldName + ":{0.9 TO 1.01}", "fl", "id, " + fieldName), 
        "//*[@numFound='1']",
        "//result/doc[1]/" + type + "[@name='" + fieldName + "'][.='1.0']");
    
    clearIndex();
    assertU(commit());
    
    String[] arr;
    if (testDouble) {
      arr = toAscendingStringArray(getRandomDoubles(10, false), true);
    } else {
      arr = toAscendingStringArray(getRandomFloats(10, false), true);
    }
    for (int i = 0; i < arr.length; i++) {
      assertU(adoc("id", String.valueOf(i), fieldName, arr[i]));
    }
    assertU(commit());
    for (int i = 0; i < arr.length; i++) {
      assertQ(req("q", fieldName + ":[" + arr[0] + " TO " + arr[i] + "]", "fl", "id, " + fieldName), 
          "//*[@numFound='" + (i + 1) + "']");
      assertQ(req("q", fieldName + ":{" + arr[0] + " TO " + arr[i] + "}", "fl", "id, " + fieldName), 
          "//*[@numFound='" + (Math.max(0,  i-1)) + "']");
    }
  }
  
  private void doTestFloatPointFunctionQuery(String field, String type) throws Exception {
    for (int i = 9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), field, String.format(Locale.ROOT, "%f", (float)i*1.1)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(field).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id, " + field, "sort", "product(-1," + field + ") asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='" + field + "'][.='9.9']",
        "//result/doc[2]/" + type + "[@name='" + field + "'][.='8.8']",
        "//result/doc[3]/" + type + "[@name='" + field + "'][.='7.7']",
        "//result/doc[10]/" + type + "[@name='" + field + "'][.='0.0']");
    
    assertQ(req("q", "*:*", "fl", "id, " + field + ", product(-1," + field + ")", "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/float[@name='product(-1," + field + ")'][.='-0.0']",
        "//result/doc[2]/float[@name='product(-1," + field + ")'][.='-1.1']",
        "//result/doc[3]/float[@name='product(-1," + field + ")'][.='-2.2']",
        "//result/doc[10]/float[@name='product(-1," + field + ")'][.='-9.9']");
    
    assertQ(req("q", "*:*", "fl", "id, " + field + ", field(" + field + ")", "sort", "id asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/" + type + "[@name='field(" + field + ")'][.='0.0']",
        "//result/doc[2]/" + type + "[@name='field(" + field + ")'][.='1.1']",
        "//result/doc[3]/" + type + "[@name='field(" + field + ")'][.='2.2']",
        "//result/doc[10]/" + type + "[@name='field(" + field + ")'][.='9.9']");
  }
  
  private void doTestSetQueries(String fieldName, String[] values, boolean multiValued) {
    for (int i = 0; i < values.length; i++) {
      assertU(adoc("id", String.valueOf(i), fieldName, values[i]));
    }
    assertU(commit());
    SchemaField sf = h.getCore().getLatestSchema().getField(fieldName); 
    assertTrue(sf.getType() instanceof PointField);
    
    for (int i = 0; i < values.length; i++) {
      assertQ(req("q", "{!term f='" + fieldName + "'}" + values[i], "fl", "id," + fieldName), 
          "//*[@numFound='1']");
    }
    
    for (int i = 0; i < values.length; i++) {
      assertQ(req("q", "{!terms f='" + fieldName + "'}" + values[i] + "," + values[(i + 1)%values.length], "fl", "id," + fieldName), 
          "//*[@numFound='2']");
    }
    
    assertTrue(values.length > SolrQueryParser.TERMS_QUERY_THRESHOLD);
    int numTerms = SolrQueryParser.TERMS_QUERY_THRESHOLD + 1;
    StringBuilder builder = new StringBuilder(fieldName + ":(");
    for (int i = 0; i < numTerms; i++) {
      if (sf.getType().getNumberType() == NumberType.DATE) {
        builder.append(values[i].replaceAll("(:|^[-+])", "\\\\$1") + ' ');
      } else {
        builder.append(String.valueOf(values[i]).replace("-", "\\-") + ' ');
      }
    }
    builder.append(')');
    if (sf.indexed()) { // SolrQueryParser should also be generating a PointInSetQuery if indexed
      assertQ(req(CommonParams.DEBUG, CommonParams.QUERY, "q", "*:*", "fq", builder.toString(), "fl", "id," + fieldName), 
          "//*[@numFound='" + numTerms + "']",
          "//*[@name='parsed_filter_queries']/str[.='(" + getSetQueryToString(fieldName, values, numTerms) + ")']");
    } else {
      // Won't use PointInSetQuery if the field is not indexed, but should match the same docs
      assertQ(req(CommonParams.DEBUG, CommonParams.QUERY, "q", "*:*", "fq", builder.toString(), "fl", "id," + fieldName), 
          "//*[@numFound='" + numTerms + "']");
    }

    if (multiValued) {
      clearIndex();
      assertU(commit());
      for (int i = 0; i < values.length; i++) {
        assertU(adoc("id", String.valueOf(i), fieldName, values[i], fieldName, values[(i+1)%values.length]));
      }
      assertU(commit());
      for (int i = 0; i < values.length; i++) {
        assertQ(req("q", "{!term f='" + fieldName + "'}" + values[i], "fl", "id," + fieldName), 
            "//*[@numFound='2']");
      }
      
      for (int i = 0; i < values.length; i++) {
        assertQ(req("q", "{!terms f='" + fieldName + "'}" + values[i] + "," + values[(i + 1)%values.length], "fl", "id," + fieldName), 
            "//*[@numFound='3']");
      }
    }
  }
  
  private String getSetQueryToString(String fieldName, String[] values, int numTerms) {
    SchemaField sf = h.getCore().getLatestSchema().getField(fieldName);
    return sf.getType().getSetQuery(null, sf, Arrays.asList(Arrays.copyOf(values, numTerms))).toString();
  }

  private void doTestDoublePointFieldMultiValuedRangeFacet(String docValuesField, String nonDocValuesField) throws Exception {
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), docValuesField, String.valueOf(i), docValuesField, String.valueOf(i + 10), 
          nonDocValuesField, String.valueOf(i), nonDocValuesField, String.valueOf(i + 10)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "-10", "facet.range.end", "20", "facet.range.gap", "2"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='2.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='4.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='6.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='8.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='10.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='12.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='14.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='16.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='18.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='-10.0'][.='0']");
    
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "-10", "facet.range.end", "20", "facet.range.gap", "2", "facet.range.method", "dv"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='2.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='4.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='6.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='8.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='10.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='12.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='14.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='16.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='18.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='-10.0'][.='0']");
    
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "0", "facet.range.end", "20", "facet.range.gap", "100"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='10']");
    
    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "-10", "facet.range.end", "20", "facet.range.gap", "2", "facet.range.method", "filter"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='2.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='4.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='6.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='8.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='10.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='12.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='14.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='16.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='18.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='-10.0'][.='0']");
    
    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "-10", "facet.range.end", "20", "facet.range.gap", "2", "facet.range.method", "dv"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='0.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='2.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='4.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='6.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='8.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='10.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='12.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='14.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='16.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='18.0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='-10.0'][.='0']");
  }
  
  private void doTestDatePointFieldExactQuery(final String field, final String baseDate) throws Exception {
    doTestDatePointFieldExactQuery(field, baseDate, true);
  }
  
  /**
   * @param field the field to use for indexing and searching against
   * @param baseDate basic value to use for indexing and searching
   * @param searchable set to true if searches against "field" should succeed, false if field is only stored and searches should always get numFound=0
   */
  private void doTestDatePointFieldExactQuery(final String field, final String baseDate, final boolean searchable) throws Exception {
    final String MATCH_ONE = "//*[@numFound='" + (searchable ? "1" : "0") + "']";
    final String MATCH_TWO = "//*[@numFound='" + (searchable ? "2" : "0") + "']";
    
    for (int i=0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), field, String.format(Locale.ROOT, "%s+%dMINUTES", baseDate, i+1)));
    }
    assertU(commit());
    for (int i = 0; i < 10; i++) {
      String date = String.format(Locale.ROOT, "%s+%dMINUTES", baseDate, i+1);
      assertQ(req("q", field + ":\""+date+"\"", "fl", "id, " + field),
              MATCH_ONE);
    }

    for (int i = 0; i < 10; i++) {
      String date1 = String.format(Locale.ROOT, "%s+%dMINUTES", baseDate, i+1);
      String date2 = String.format(Locale.ROOT, "%s+%dMINUTES", baseDate, ((i+1)%10 + 1));
      assertQ(req("q", field + ":\"" + date1 + "\""
                  + " OR " + field + ":\"" + date2 + "\""),
              MATCH_TWO);
    }

    clearIndex();
    assertU(commit());
  }
  
  private void doTestDatePointFieldRangeQuery(String fieldName) throws Exception {
    String baseDate = "1995-12-31T10:59:59Z";
    for (int i = 9; i >= 0; i--) {
      assertU(adoc("id", String.valueOf(i), fieldName, String.format(Locale.ROOT, "%s+%dHOURS", baseDate, i)));
    }
    assertU(commit());
    assertQ(req("q", fieldName + ":" + String.format(Locale.ROOT, "[%s+0HOURS TO %s+3HOURS]", baseDate, baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='4']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']",
        "//result/doc[2]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']",
        "//result/doc[3]/date[@name='" + fieldName + "'][.='1995-12-31T12:59:59Z']",
        "//result/doc[4]/date[@name='" + fieldName + "'][.='1995-12-31T13:59:59Z']");

    assertQ(req("q", fieldName + ":" + String.format(Locale.ROOT, "{%s+0HOURS TO %s+3HOURS]", baseDate, baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']",
        "//result/doc[2]/date[@name='" + fieldName + "'][.='1995-12-31T12:59:59Z']",
        "//result/doc[3]/date[@name='" + fieldName + "'][.='1995-12-31T13:59:59Z']");

    assertQ(req("q", fieldName + ":"+ String.format(Locale.ROOT, "[%s+0HOURS TO %s+3HOURS}",baseDate,baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']",
        "//result/doc[2]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']",
        "//result/doc[3]/date[@name='" + fieldName + "'][.='1995-12-31T12:59:59Z']");

    assertQ(req("q", fieldName + ":"+ String.format(Locale.ROOT, "{%s+0HOURS TO %s+3HOURS}",baseDate,baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']",
        "//result/doc[2]/date[@name='" + fieldName + "'][.='1995-12-31T12:59:59Z']");

    assertQ(req("q", fieldName + ":" + String.format(Locale.ROOT, "{%s+0HOURS TO *}",baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='9']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']");

    assertQ(req("q", fieldName + ":" + String.format(Locale.ROOT, "{* TO %s+3HOURS}",baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']");

    assertQ(req("q", fieldName + ":" + String.format(Locale.ROOT, "[* TO %s+3HOURS}",baseDate),
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']");

    assertQ(req("q", fieldName + ":[* TO *}", "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='10']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']",
        "//result/doc[10]/date[@name='" + fieldName + "'][.='1995-12-31T19:59:59Z']");

    assertQ(req("q", fieldName + ":" + String.format(Locale.ROOT, "[%s+0HOURS TO %s+1HOURS]",baseDate,baseDate)
                + " OR " + fieldName + ":" + String.format(Locale.ROOT, "[%s+8HOURS TO %s+9HOURS]",baseDate,baseDate) ,
                "fl", "id, " + fieldName, "sort", "id asc"),
        "//*[@numFound='4']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']",
        "//result/doc[2]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']",
        "//result/doc[3]/date[@name='" + fieldName + "'][.='1995-12-31T18:59:59Z']",
        "//result/doc[4]/date[@name='" + fieldName + "'][.='1995-12-31T19:59:59Z']");

    assertQ(req("q", fieldName + ":"+String.format(Locale.ROOT, "[%s+0HOURS TO %s+1HOURS]",baseDate,baseDate)
            +" AND " + fieldName + ":"+String.format(Locale.ROOT, "[%s+1HOURS TO %s+2HOURS]",baseDate,baseDate) , "fl", "id, " + fieldName),
        "//*[@numFound='1']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T11:59:59Z']");

    assertQ(req("q", fieldName + ":"+String.format(Locale.ROOT, "[%s+0HOURS TO %s+1HOURS]",baseDate,baseDate)
            +" AND NOT " + fieldName + ":"+String.format(Locale.ROOT, "[%s+1HOURS TO %s+2HOURS]",baseDate,baseDate) , "fl", "id, " + fieldName),
        "//*[@numFound='1']",
        "//result/doc[1]/date[@name='" + fieldName + "'][.='1995-12-31T10:59:59Z']");

    clearIndex();
    assertU(commit());
  }

  private void doTestDatePointFieldRangeFacet(String docValuesField, String nonDocValuesField) throws Exception {
    String baseDate = "1995-01-10T10:59:59Z";
    for (int i = 0; i < 10; i++) {
      String date = String.format(Locale.ROOT, "%s+%dDAYS", baseDate, i);
      assertU(adoc("id", String.valueOf(i), docValuesField, date, nonDocValuesField, date));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+10DAYS", "facet.range.gap", "+2DAYS"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-08T10:59:59Z'][.='0']");

    assertQ(req("q", "*:*", "facet", "true", "facet.range", docValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+10DAYS", "facet.range.gap", "+2DAYS", "facet.range.method", "dv"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-08T10:59:59Z'][.='0']");

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+10DAYS", "facet.range.gap", "+2DAYS", "facet.range.method", "filter"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-08T10:59:59Z'][.='0']");

    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+10DAYS", "facet.range.gap", "+2DAYS", "facet.range.method", "dv"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-08T10:59:59Z'][.='0']");
  }

  private void doTestDatePointFieldMultiValuedRangeFacet(String docValuesField, String nonDocValuesField) throws Exception {
    String baseDate = "1995-01-10T10:59:59Z";
    for (int i = 0; i < 10; i++) {
      String date1 = String.format(Locale.ROOT, "%s+%dDAYS", baseDate, i);
      String date2 = String.format(Locale.ROOT, "%s+%dDAYS", baseDate, i+10);
      assertU(adoc("id", String.valueOf(i), docValuesField, date1, docValuesField, date2,
          nonDocValuesField, date1, nonDocValuesField, date2));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+20DAYS", "facet.range.gap", "+2DAYS"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-20T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-22T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-24T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-26T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-28T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1994-12-31T10:59:59Z'][.='0']");

    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+20DAYS", "facet.range.gap", "+2DAYS", "facet.range.method", "dv"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-20T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-22T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-24T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-26T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-28T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1994-12-31T10:59:59Z'][.='0']");

    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", docValuesField, "facet.range.start", "1995-01-10T10:59:59Z",
        "facet.range.end", "1995-01-10T10:59:59Z+20DAYS", "facet.range.gap", "+100DAYS"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + docValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='10']");

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);
    // Range Faceting with method = filter should work
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+20DAYS", "facet.range.gap", "+2DAYS", "facet.range.method", "filter"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-20T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-22T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-24T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-26T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-28T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1994-12-31T10:59:59Z'][.='0']");

    // this should actually use filter method instead of dv
    assertQ(req("q", "*:*", "fl", "id", "facet", "true", "facet.range", nonDocValuesField, "facet.range.start", "1995-01-10T10:59:59Z-10DAYS",
        "facet.range.end", "1995-01-10T10:59:59Z+20DAYS", "facet.range.gap", "+2DAYS", "facet.range.method", "dv"),
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-10T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-12T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-14T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-16T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-18T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-20T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-22T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-24T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-26T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1995-01-28T10:59:59Z'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='" + nonDocValuesField + "']/lst[@name='counts']/int[@name='1994-12-31T10:59:59Z'][.='0']");
  }

  private void doTestDatePointFunctionQuery(String field, String nonDvFieldName) throws Exception {
    final String baseDate = "1995-01-10T10:59:10Z";
    
    for (int i = 9; i >= 0; i--) {
      String date = String.format(Locale.ROOT, "%s+%dSECONDS", baseDate, i+1);
      assertU(adoc("id", String.valueOf(i), field, date));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(field).getType() instanceof DatePointField);
    assertQ(req("q", "*:*", "fl", "id, " + field, "sort", "product(-1,ms(" + field + "," + baseDate +")) asc"),
        "//*[@numFound='10']",
        "//result/doc[1]/date[@name='" + field + "'][.='1995-01-10T10:59:20Z']",
        "//result/doc[2]/date[@name='" + field + "'][.='1995-01-10T10:59:19Z']",
        "//result/doc[3]/date[@name='" + field + "'][.='1995-01-10T10:59:18Z']",
        "//result/doc[10]/date[@name='" + field + "'][.='1995-01-10T10:59:11Z']");

    assertQ(req("q", "*:*", "fl", "id, " + field + ", ms(" + field + ","+baseDate+")", "sort", "id asc"),
        "//*[@numFound='10']",
        "//result/doc[1]/float[@name='ms(" + field + "," + baseDate + ")'][.='1000.0']",
        "//result/doc[2]/float[@name='ms(" + field + "," + baseDate + ")'][.='2000.0']",
        "//result/doc[3]/float[@name='ms(" + field + "," + baseDate + ")'][.='3000.0']",
        "//result/doc[10]/float[@name='ms(" + field + "," + baseDate + ")'][.='10000.0']");

    assertQ(req("q", "*:*", "fl", "id, " + field + ", field(" + field + ")", "sort", "id asc"),
        "//*[@numFound='10']",
        "//result/doc[1]/date[@name='field(" + field + ")'][.='1995-01-10T10:59:11Z']",
        "//result/doc[2]/date[@name='field(" + field + ")'][.='1995-01-10T10:59:12Z']",
        "//result/doc[3]/date[@name='field(" + field + ")'][.='1995-01-10T10:59:13Z']",
        "//result/doc[10]/date[@name='field(" + field + ")'][.='1995-01-10T10:59:20Z']");

  }

  private void testDatePointStats(String field, String dvField, String[] dates) {
    for (int i = 0; i < dates.length; i++) {
      assertU(adoc("id", String.valueOf(i), dvField, dates[i], field, dates[i]));
    }
    assertU(adoc("id", String.valueOf(dates.length)));
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField(dvField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(dvField).getType() instanceof PointField);
    assertQ(req("q", "*:*", "fl", "id, " + dvField, "stats", "true", "stats.field", dvField),
        "//*[@numFound='" + (dates.length + 1) + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/date[@name='min'][.='" + dates[0] + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/date[@name='max'][.='" + dates[dates.length-1] + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/long[@name='count'][.='" + dates.length + "']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='" + dvField+ "']/long[@name='missing'][.='1']");

    assertFalse(h.getCore().getLatestSchema().getField(field).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(field).getType() instanceof PointField);
    assertQEx("Expecting Exception",
        "Can't calculate stats on a PointField without docValues",
        req("q", "*:*", "fl", "id, " + field, "stats", "true", "stats.field", field),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  private void testDatePointFieldsAtomicUpdates(String field) throws Exception {
    long millis1 = random().nextLong() % MAX_DATE_EPOCH_MILLIS;
    BigInteger bigMillis1 = BigInteger.valueOf(millis1);
    long millis2;
    BigInteger maxLong = BigInteger.valueOf(Long.MAX_VALUE);
    DateGapCeiling gap;
    for ( ; ; ) {
      millis2 = random().nextLong() % MAX_DATE_EPOCH_MILLIS;
      gap = new DateGapCeiling(millis2 - millis1);
      millis2 = gap.addTo(millis1); // adjust millis2 to the closest +/-UNIT gap
      break;
    }
    String date1 = Instant.ofEpochMilli(millis1).toString();
    String date2 = Instant.ofEpochMilli(millis2).toString();
    assertU(adoc(sdoc("id", "1", field, date1)));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/date[@name='" + field + "'][.='" + date1 + "']");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", date1 + gap.toString()))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/date[@name='" + field + "'][.='" + date2 + "']");
  }

  private void testMultiValuedDatePointFieldsAtomicUpdates(String field) throws Exception {
    List<String> datesList = getRandomLongs(3, false, MAX_DATE_EPOCH_MILLIS)
        .stream().map(millis -> Instant.ofEpochMilli(millis).toString()).collect(Collectors.toList());
    String[] dates = datesList.toArray(new String[datesList.size()]);
    assertU(adoc(sdoc("id", "1", field, dates[0])));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/date[.='" + dates[0] + "']",
        "count(//result/doc[1]/arr[@name='" + field + "']/date)=1");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("add", dates[1]))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/date[.='" + dates[0] + "']",
        "//result/doc[1]/arr[@name='" + field + "']/date[.='" + dates[1] + "']",
        "count(//result/doc[1]/arr[@name='" + field + "']/date)=2");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("remove", dates[0]))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/date[.='" + dates[1] + "']",
        "count(//result/doc[1]/arr[@name='" + field + "']/date)=1");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("set", datesList))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "//result/doc[1]/arr[@name='" + field + "']/date[.='" + dates[0] + "']",
        "//result/doc[1]/arr[@name='" + field + "']/date[.='" + dates[1] + "']",
        "//result/doc[1]/arr[@name='" + field + "']/date[.='" + dates[2] + "']",
        "count(//result/doc[1]/arr[@name='" + field + "']/date)=3");

    assertU(adoc(sdoc("id", "1", field, ImmutableMap.of("removeregex", ".*"))));
    assertU(commit());

    assertQ(req("q", "id:1"),
        "count(//result/doc[1]/arr[@name='" + field + "']/date)=0");

  }
  
  private void doTestInternals(String field, String[] values) throws IOException {
    assertTrue(h.getCore().getLatestSchema().getField(field).getType() instanceof PointField);
    for (int i=0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), field, values[i]));
    }
    assertU(commit());
    IndexReader ir;
    RefCounted<SolrIndexSearcher> ref = null;
    SchemaField sf = h.getCore().getLatestSchema().getField(field);
    boolean ignoredField = !(sf.indexed() || sf.stored() || sf.hasDocValues());
    try {
      ref = h.getCore().getSearcher();
      SolrIndexSearcher searcher = ref.get(); 
      ir = searcher.getIndexReader();
      // our own SlowCompositeReader to check DocValues on disk w/o the UninvertingReader added by SolrIndexSearcher
      final LeafReader leafReaderForCheckingDVs = SlowCompositeReaderWrapper.wrap(searcher.getRawReader());
      
      if (sf.indexed()) {
        assertEquals("Field " + field + " should have point values", 10, PointValues.size(ir, field));
      } else {
        assertEquals("Field " + field + " should have no point values", 0, PointValues.size(ir, field));
      }
      if (ignoredField) {
        assertTrue("Field " + field + " should not have docValues",
            DocValues.getSortedNumeric(leafReaderForCheckingDVs, field).nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
        assertTrue("Field " + field + " should not have docValues", 
            DocValues.getNumeric(leafReaderForCheckingDVs, field).nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
        assertTrue("Field " + field + " should not have docValues", 
            DocValues.getSorted(leafReaderForCheckingDVs, field).nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
        assertTrue("Field " + field + " should not have docValues", 
            DocValues.getBinary(leafReaderForCheckingDVs, field).nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
      } else {
        if (sf.hasDocValues()) {
          if (sf.multiValued()) {
            assertFalse("Field " + field + " should have docValues", 
                DocValues.getSortedNumeric(leafReaderForCheckingDVs, field).nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
          } else {
            assertFalse("Field " + field + " should have docValues", 
                DocValues.getNumeric(leafReaderForCheckingDVs, field).nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
          }
        } else {
          expectThrows(IllegalStateException.class, ()->DocValues.getSortedNumeric(leafReaderForCheckingDVs, field));
          expectThrows(IllegalStateException.class, ()->DocValues.getNumeric(leafReaderForCheckingDVs, field));
        }
        expectThrows(IllegalStateException.class, ()->DocValues.getSorted(leafReaderForCheckingDVs, field));
        expectThrows(IllegalStateException.class, ()->DocValues.getBinary(leafReaderForCheckingDVs, field));
      }
      for (LeafReaderContext leave:ir.leaves()) {
        LeafReader reader = leave.reader();
        for (int i = 0; i < reader.numDocs(); i++) {
          Document doc = reader.document(i);
          if (sf.stored()) {
            assertNotNull("Field " + field + " not found. Doc: " + doc, doc.get(field));
          } else {
            assertNull(doc.get(field));
          }
        }
      }
    } finally {
      ref.decref();
    }
    clearIndex();
    assertU(commit());
  }

  public void testNonReturnable() throws Exception {
    
    doTestReturnNonStored("foo_p_i_ni_ns", false, "42");
    doTestReturnNonStored("foo_p_i_ni_dv_ns", true, "42");
    doTestReturnNonStored("foo_p_i_ni_ns_mv", false, "42", "666");
    doTestReturnNonStored("foo_p_i_ni_dv_ns_mv", true, "42", "666");

    doTestReturnNonStored("foo_p_l_ni_ns", false, "3333333333");
    doTestReturnNonStored("foo_p_l_ni_dv_ns", true, "3333333333");
    doTestReturnNonStored("foo_p_l_ni_ns_mv", false, "3333333333", "-4444444444");
    doTestReturnNonStored("foo_p_l_ni_dv_ns_mv", true, "3333333333", "-4444444444");

    doTestReturnNonStored("foo_p_f_ni_ns", false, "42.3");
    doTestReturnNonStored("foo_p_f_ni_dv_ns", true, "42.3");
    doTestReturnNonStored("foo_p_f_ni_ns_mv", false, "42.3", "-66.6");
    doTestReturnNonStored("foo_p_f_ni_dv_ns_mv", true, "42.3", "-66.6");
    
    doTestReturnNonStored("foo_p_d_ni_ns", false, "42.3");
    doTestReturnNonStored("foo_p_d_ni_dv_ns", true, "42.3");
    doTestReturnNonStored("foo_p_d_ni_ns_mv", false, "42.3", "-66.6");
    doTestReturnNonStored("foo_p_d_ni_dv_ns_mv", true, "42.3", "-66.6");

    doTestReturnNonStored("foo_p_dt_ni_ns", false, "1995-12-31T23:59:59Z");
    doTestReturnNonStored("foo_p_dt_ni_dv_ns", true, "1995-12-31T23:59:59Z");
    doTestReturnNonStored("foo_p_dt_ni_ns_mv", false, "1995-12-31T23:59:59Z", "2000-12-31T23:59:59Z+3DAYS");
    doTestReturnNonStored("foo_p_dt_ni_dv_ns_mv", true, "1995-12-31T23:59:59Z", "2000-12-31T23:59:59Z+3DAYS");
  }

  public void doTestReturnNonStored(final String fieldName, boolean shouldReturnFieldIfRequested, final String... values) throws Exception {
    final String RETURN_FIELD = "count(//doc/*[@name='" + fieldName + "'])=10";
    final String DONT_RETURN_FIELD = "count(//doc/*[@name='" + fieldName + "'])=0";
    assertFalse(h.getCore().getLatestSchema().getField(fieldName).stored());
    for (int i=0; i < 10; i++) {
      SolrInputDocument doc = sdoc("id", String.valueOf(i));
      for (String value : values) {
        doc.addField(fieldName, value);
      }
      assertU(adoc(doc));
    }
    assertU(commit());
    assertQ(req("q", "*:*", "rows", "100", "fl", "id," + fieldName), 
            "//*[@numFound='10']",
            "count(//doc)=10", // exactly 10 docs in response
            (shouldReturnFieldIfRequested?RETURN_FIELD:DONT_RETURN_FIELD)); // no field in any doc other then 'id'

    assertQ(req("q", "*:*", "rows", "100", "fl", "*"), 
        "//*[@numFound='10']",
        "count(//doc)=10", // exactly 10 docs in response
        DONT_RETURN_FIELD); // no field in any doc other then 'id'

    assertQ(req("q", "*:*", "rows", "100"), 
        "//*[@numFound='10']",
        "count(//doc)=10", // exactly 10 docs in response
        DONT_RETURN_FIELD); // no field in any doc other then 'id'
    clearIndex();
    assertU(commit());
  }

  public void testWhiteboxCreateFields() throws Exception {
    String[] typeNames = new String[]{"i", "l", "f", "d", "dt"};
    Class<?>[] expectedClasses = new Class[]{IntPoint.class, LongPoint.class, FloatPoint.class, DoublePoint.class, LongPoint.class};
    
    Date dateToTest = new Date();
    Object[][] values = new Object[][] {
      {42, "42"},
      {42, "42"},
      {42.123, "42.123"},
      {12345.6789, "12345.6789"},
      {dateToTest, new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT).format(dateToTest), "NOW"} // "NOW" won't be equal to the other dates
    };
    
    Set<String> typesTested = new HashSet<>();
    for (int i = 0; i < typeNames.length; i++) {
      for (String suffix:FIELD_SUFFIXES) {
        doWhiteboxCreateFields("whitebox_p_" + typeNames[i] + suffix, expectedClasses[i], values[i]);
        typesTested.add("*_p_" + typeNames[i] + suffix);
      }
    }
    Set<String> typesToTest = new HashSet<>();
    for (DynamicField dynField:h.getCore().getLatestSchema().getDynamicFields()) {
      if (dynField.getPrototype().getType() instanceof PointField) {
        typesToTest.add(dynField.getRegex());
      }
    }
    assertEquals("Missing types in the test", typesTested, typesToTest);
  }
  
  /** 
   * Calls {@link #callAndCheckCreateFields} on each of the specified values.
   * This is a convinience method for testing the same fieldname with multiple inputs.
   *
   * @see #callAndCheckCreateFields
   */
  private void doWhiteboxCreateFields(final String fieldName, final Class<?> pointType, final Object... values) throws Exception {
    
    for (Object value : values) {
      // ideally we should require that all input values be diff forms of the same logical value
      // (ie '"42"' vs 'new Integer(42)') and assert that each produces an equivilent list of IndexableField objects
      // but that doesn't seem to work -- appears not all IndexableField classes override Object.equals?
      final List<IndexableField> result = callAndCheckCreateFields(fieldName, pointType, value);
      assertNotNull(value + " => null", result);
    }
  }


  /** 
   * Calls {@link SchemaField#createFields} on the specified value for the specified field name, and asserts 
   * that the results match the SchemaField propeties, with an additional check that the <code>pointType</code> 
   * is included if and only if the SchemaField is "indexed" 
   */
  private List<IndexableField> callAndCheckCreateFields(final String fieldName, final Class<?> pointType, final Object value) throws Exception {
    final SchemaField sf = h.getCore().getLatestSchema().getField(fieldName);
    final List<IndexableField> results = sf.createFields(value);
    final Set<IndexableField> resultSet = new LinkedHashSet<>(results);
    assertEquals("duplicates found in results? " + results.toString(),
                 results.size(), resultSet.size());

    final Set<Class<?>> resultClasses = new HashSet<>();
    for (IndexableField f : results) {
      resultClasses.add(f.getClass());
      
      if (!sf.hasDocValues() ) {
        assertFalse(f.toString(),
                    (f instanceof NumericDocValuesField) ||
                    (f instanceof SortedNumericDocValuesField));
      }
    }
    assertEquals(fieldName + " stored? Result Fields: " + Arrays.toString(results.toArray()),
                 sf.stored(), resultClasses.contains(StoredField.class));
    assertEquals(fieldName + " indexed? Result Fields: " + Arrays.toString(results.toArray()),
                 sf.indexed(), resultClasses.contains(pointType));
    if (sf.multiValued()) {
      assertEquals(fieldName + " docvalues? Result Fields: " + Arrays.toString(results.toArray()),
                   sf.hasDocValues(), resultClasses.contains(SortedNumericDocValuesField.class));
    } else {
      assertEquals(fieldName + " docvalues? Result Fields: " + Arrays.toString(results.toArray()),
                   sf.hasDocValues(), resultClasses.contains(NumericDocValuesField.class));
    }

    return results;
  }


}
