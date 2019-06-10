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

package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.DoublePointMultiRangeBuilder;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.FloatPointMultiRangeBuilder;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.IntPointMultiRangeBuilder;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.LongPointMultiRangeBuilder;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestMultiRangeQueries extends LuceneTestCase {

  public void testDoubleRandomMultiRangeQuery() throws IOException {
    final int numDims = TestUtil.nextInt(random(), 1, 3);
    final int numVals = TestUtil.nextInt(random(), 3, 8);
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    double[] value = new double[numDims];
    for (int i = 0; i < numDims; ++i) {
      value[i] = TestUtil.nextInt(random(), 1, 10);
    }
    doc.add(new DoublePoint("point", value));
    w.addDocument(doc);
    IndexReader reader = w.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    DoublePointMultiRangeBuilder builder = new DoublePointMultiRangeBuilder("point", numDims);
    for (int j = 0;j < numVals; j++) {
      double[] lowerBound = new double[numDims];
      double[] upperBound = new double[numDims];
      for (int i = 0; i < numDims; ++i) {
        lowerBound[i] = value[i] - random().nextInt(1);
        upperBound[i] = value[i] + random().nextInt(1);
      }
      builder.add(lowerBound, upperBound);
    }

    Query query = builder.build();
    searcher.search(query, Integer.MAX_VALUE);

    reader.close();
    w.close();
    dir.close();
  }

  public void testDoublePointMultiRangeQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    double[] firstPoint = {112.4, 296.2, 512.7};
    double[] secondPoint = {219.3, 514.7, 624.2};

    Document doc = new Document();
    doc.add(new DoublePoint("point", firstPoint));
    iw.addDocument(doc);
    iw.commit();

    doc = new Document();
    doc.add(new DoublePoint("point", secondPoint));
    iw.addDocument(doc);
    iw.commit();

    // One range matches
    double[] firstLowerRange= {111.3, 294.2, 502.8};
    double[] firstUpperRange = {117.3, 301.4, 514.5};

    double[] secondLowerRange = {15.3, 4.5, 415.7};
    double[] secondUpperRange = {200.2, 402.4, 583.6};

    DoublePointMultiRangeBuilder builder = new DoublePointMultiRangeBuilder("point", 3);

    builder.add(firstLowerRange, firstUpperRange);
    builder.add(secondLowerRange, secondUpperRange);

    Query query = builder.build();

    IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();

    assertEquals(searcher.count(query), 1);

    // Both ranges match
    double[] firstMatchingLowerRange= {111.3, 294.2, 502.4};
    double[] firstMatchingUpperRange = {117.6, 301.8, 514.2};

    double[] secondMatchingLowerRange = {212.4, 512.3, 415.7};
    double[] secondMatchingUpperRange = {228.3, 538.7, 647.1};

    DoublePointMultiRangeBuilder builder2 = new DoublePointMultiRangeBuilder("point", 3);

    builder2.add(firstMatchingLowerRange, firstMatchingUpperRange);
    builder2.add(secondMatchingLowerRange, secondMatchingUpperRange);

    query = builder2.build();

    assertEquals(searcher.count(query), 2);

    // None match
    double[] nonMatchingFirstRangeLower = {1.3, 3.5, 2.7};
    double[] nonMatchingFirstRangeUpper = {5.2, 8.3, 7.8};

    double[] nonMatchingSecondRangeLower = {11246.3, 19388.7, 21248.4};
    double[] nonMatchingSecondRangeUpper = {13242.9, 20214.2, 23236.5};
    DoublePointMultiRangeBuilder builder3 = new DoublePointMultiRangeBuilder("point", 3);

    builder3.add(nonMatchingFirstRangeLower, nonMatchingFirstRangeUpper);
    builder3.add(nonMatchingSecondRangeLower, nonMatchingSecondRangeUpper);

    query = builder3.build();

    assertEquals(searcher.count(query), 0);

    // Lower point is equal to a point
    double[] firstEqualLowerRange= {112.4, 296.2, 512.7};
    double[] firstEqualUpperRange = {117.6, 301.8, 514.2};

    double[] secondEqualLowerRange = {219.3, 514.7, 624.2};
    double[] secondEqualUpperRange = {228.3, 538.7, 647.1};

    DoublePointMultiRangeBuilder builder4 = new DoublePointMultiRangeBuilder("point", 3);

    builder4.add(firstEqualLowerRange, firstEqualUpperRange);
    builder4.add(secondEqualLowerRange, secondEqualUpperRange);

    query = builder4.build();

    assertEquals(searcher.count(query), 2);

    reader.close();
    dir.close();
  }

  public void testLongRandomMultiRangeQuery() throws IOException {
    final int numDims = TestUtil.nextInt(random(), 1, 3);
    final int numVals = TestUtil.nextInt(random(), 3, 8);
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    long[] value = new long[numDims];
    for (int i = 0; i < numDims; ++i) {
      value[i] = TestUtil.nextLong(random(), 1, 10);
    }
    doc.add(new LongPoint("point", value));
    w.addDocument(doc);
    IndexReader reader = w.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    LongPointMultiRangeBuilder builder = new LongPointMultiRangeBuilder("point", numDims);
    for (int j = 0;j < numVals; j++) {
      long[] lowerBound = new long[numDims];
      long[] upperBound = new long[numDims];
      for (int i = 0; i < numDims; ++i) {
        lowerBound[i] = value[i] - random().nextInt(1);
        upperBound[i] = value[i] + random().nextInt(1);
      }
      builder.add(lowerBound, upperBound);
    }

    Query query = builder.build();
    searcher.search(query, Integer.MAX_VALUE);

    reader.close();
    w.close();
    dir.close();
  }

  public void testLongPointMultiRangeQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    long[] firstPoint = {112, 296, 512};
    long[] secondPoint = {219, 514, 624};

    Document doc = new Document();
    doc.add(new LongPoint("point", firstPoint));
    iw.addDocument(doc);
    iw.commit();

    doc = new Document();
    doc.add(new LongPoint("point", secondPoint));
    iw.addDocument(doc);
    iw.commit();

    // One range matches
    long[] firstLowerRange= {111, 294, 502};
    long[] firstUpperRange = {117, 301, 514};

    long[] secondLowerRange = {15, 4, 415};
    long[] secondUpperRange = {200, 402, 583};

    LongPointMultiRangeBuilder builder = new LongPointMultiRangeBuilder("point", 3);

    builder.add(firstLowerRange, firstUpperRange);
    builder.add(secondLowerRange, secondUpperRange);

    Query query = builder.build();

    IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();

    assertEquals(searcher.count(query), 1);

    // Both ranges match
    long[] firstMatchingLowerRange= {111, 294, 502};
    long[] firstMatchingUpperRange = {117, 301, 514};

    long[] secondMatchingLowerRange = {212, 512, 415};
    long[] secondMatchingUpperRange = {228, 538, 647};


    LongPointMultiRangeBuilder builder2 = new LongPointMultiRangeBuilder("point", 3);

    builder2.add(firstMatchingLowerRange, firstMatchingUpperRange);
    builder2.add(secondMatchingLowerRange, secondMatchingUpperRange);

    query = builder2.build();

    assertEquals(searcher.count(query), 2);

    // None match
    long[] nonMatchingFirstRangeLower = {1, 3, 2};
    long[] nonMatchingFirstRangeUpper = {5, 8, 7};

    long[] nonMatchingSecondRangeLower = {11246, 19388, 21248};
    long[] nonMatchingSecondRangeUpper = {13242, 20214, 23236};
    LongPointMultiRangeBuilder builder3 = new LongPointMultiRangeBuilder("point", 3);

    builder3.add(nonMatchingFirstRangeLower, nonMatchingFirstRangeUpper);
    builder3.add(nonMatchingSecondRangeLower, nonMatchingSecondRangeUpper);

    query = builder3.build();

    assertEquals(searcher.count(query), 0);

    // Lower point is equal to a point
    long[] firstEqualsLowerPoint= {112, 296, 512};
    long[] firstEqualsUpperPoint = {219, 514, 624};

    long[] secondEqualsLowerPoint = {11246, 19388, 21248};
    long[] secondEqualsUpperPoint = {13242, 20214, 23236};

    LongPointMultiRangeBuilder builder4 = new LongPointMultiRangeBuilder("point", 3);

    builder4.add(firstEqualsLowerPoint, firstEqualsUpperPoint);
    builder4.add(secondEqualsLowerPoint, secondEqualsUpperPoint);

    query = builder4.build();

    assertEquals(searcher.count(query), 2);

    reader.close();
    dir.close();
  }

  public void testFloatRandomMultiRangeQuery() throws IOException {
    final int numDims = TestUtil.nextInt(random(), 1, 3);
    final int numVals = TestUtil.nextInt(random(), 3, 8);
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    float[] value = new float[numDims];
    for (int i = 0; i < numDims; ++i) {
      value[i] = TestUtil.nextInt(random(), 1, 10);
    }
    doc.add(new FloatPoint("point", value));
    w.addDocument(doc);
    IndexReader reader = w.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    FloatPointMultiRangeBuilder builder = new FloatPointMultiRangeBuilder("point", numDims);
    for (int j = 0;j < numVals; j++) {
      float[] lowerBound = new float[numDims];
      float[] upperBound = new float[numDims];
      for (int i = 0; i < numDims; ++i) {
        lowerBound[i] = value[i] - random().nextInt(1);
        upperBound[i] = value[i] + random().nextInt(1);
      }
      builder.add(lowerBound, upperBound);
    }

    Query query = builder.build();
    searcher.search(query, Integer.MAX_VALUE);

    reader.close();
    w.close();
    dir.close();
  }

  public void testFloatPointMultiRangeQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    float[] firstPoint = {112.4f, 296.3f, 512.1f};
    float[] secondPoint = {219.7f, 514.2f, 624.6f};

    Document doc = new Document();
    doc.add(new FloatPoint("point", firstPoint));
    iw.addDocument(doc);
    iw.commit();

    doc = new Document();
    doc.add(new FloatPoint("point", secondPoint));
    iw.addDocument(doc);
    iw.commit();

    // One range matches
    float[] firstLowerRange= {111.3f, 294.7f, 502.1f};
    float[] firstUpperRange = {117.2f, 301.6f, 514.3f};

    float[] secondLowerRange = {15.2f, 4.3f, 415.2f};
    float[] secondUpperRange = {200.6f, 402.3f, 583.8f};

    FloatPointMultiRangeBuilder builder = new FloatPointMultiRangeBuilder("point", 3);

    builder.add(firstLowerRange, firstUpperRange);
    builder.add(secondLowerRange, secondUpperRange);

    Query query = builder.build();

    IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();

    assertEquals(searcher.count(query), 1);

    // Both ranges match
    float[] firstMatchingLowerRange= {111f, 294f, 502f};
    float[] firstMatchingUpperRange = {117f, 301f, 514f};

    float[] secondMatchingLowerRange = {212f, 512f, 415f};
    float[] secondMatchingUpperRange = {228f, 538f, 647f};

    FloatPointMultiRangeBuilder builder2 = new FloatPointMultiRangeBuilder("point", 3);

    builder2.add(firstMatchingLowerRange, firstMatchingUpperRange);
    builder2.add(secondMatchingLowerRange, secondMatchingUpperRange);

    query = builder2.build();

    assertEquals(searcher.count(query), 2);

    // None Match
    float[] nonMatchingFirstRangeLower = {1.4f, 3.3f, 2.7f};
    float[] nonMatchingFirstRangeUpper = {5.4f, 8.2f, 7.3f};

    float[] nonMatchingSecondRangeLower = {11246.2f, 19388.6f, 21248.3f};
    float[] nonMatchingSecondRangeUpper = {13242.4f, 20214.7f, 23236.3f};
    FloatPointMultiRangeBuilder builder3 = new FloatPointMultiRangeBuilder("point", 3);

    builder3.add(nonMatchingFirstRangeLower, nonMatchingFirstRangeUpper);
    builder3.add(nonMatchingSecondRangeLower, nonMatchingSecondRangeUpper);

    query = builder3.build();

    assertEquals(searcher.count(query), 0);

    // Lower point is equal to a point
    float[] firstEqualsLowerPoint= {112.4f, 296.3f, 512.1f};
    float[] firstEqualsUpperPoint = {117.3f, 299.4f, 519.3f};

    float[] secondEqualsLowerPoint = {219.7f, 514.2f, 624.6f};
    float[] secondEqualsUpperPoint = {13242.4f, 20214.7f, 23236.3f};

    FloatPointMultiRangeBuilder builder4 = new FloatPointMultiRangeBuilder("point", 3);

    builder4.add(firstEqualsLowerPoint, firstEqualsUpperPoint);
    builder4.add(secondEqualsLowerPoint, secondEqualsUpperPoint);

    query = builder4.build();

    assertEquals(searcher.count(query), 2);

    reader.close();
    dir.close();
  }

  public void testIntRandomMultiRangeQuery() throws IOException {
    final int numDims = TestUtil.nextInt(random(), 1, 3);
    final int numVals = TestUtil.nextInt(random(), 3, 8);
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    int[] value = new int[numDims];
    for (int i = 0; i < numDims; ++i) {
      value[i] = TestUtil.nextInt(random(), 1, 10);
    }
    doc.add(new IntPoint("point", value));
    w.addDocument(doc);
    IndexReader reader = w.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    IntPointMultiRangeBuilder builder = new IntPointMultiRangeBuilder("point", numDims);
    for (int j = 0;j < numVals; j++) {
      int[] lowerBound = new int[numDims];
      int[] upperBound = new int[numDims];
      for (int i = 0; i < numDims; ++i) {
        lowerBound[i] = value[i] - random().nextInt(1);
        upperBound[i] = value[i] + random().nextInt(1);
      }
      builder.add(lowerBound, upperBound);
    }

    Query query = builder.build();
    searcher.search(query, Integer.MAX_VALUE);

    reader.close();
    w.close();
    dir.close();
  }

  public void testIntPointMultiRangeQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int[] firstPoint = {112, 296, 512};
    int[] secondPoint = {219, 514, 624};

    Document doc = new Document();
    doc.add(new IntPoint("point", firstPoint));
    iw.addDocument(doc);
    iw.commit();

    doc = new Document();
    doc.add(new IntPoint("point", secondPoint));
    iw.addDocument(doc);
    iw.commit();

    // One range matches
    int[] firstLowerRange= {111, 294, 502};
    int[] firstUpperRange = {117, 301, 514};

    int[] secondLowerRange = {15, 4, 415};
    int[] secondUpperRange = {200, 402, 583};

    IntPointMultiRangeBuilder builder = new IntPointMultiRangeBuilder("point", 3);

    builder.add(firstLowerRange, firstUpperRange);
    builder.add(secondLowerRange, secondUpperRange);

    Query query = builder.build();

    IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();

    assertEquals(searcher.count(query), 1);

    // Both ranges match
    int[] firstMatchingLowerRange= {111, 294, 502};
    int[] firstMatchingUpperRange = {117, 301, 514};

    int[] secondMatchingLowerRange = {212, 512, 415};
    int[] secondMatchingUpperRange = {228, 538, 647};


    IntPointMultiRangeBuilder builder2 = new IntPointMultiRangeBuilder("point", 3);

    builder2.add(firstMatchingLowerRange, firstMatchingUpperRange);
    builder2.add(secondMatchingLowerRange, secondMatchingUpperRange);

    query = builder2.build();

    assertEquals(searcher.count(query), 2);

    // None match
    int[] nonMatchingFirstRangeLower = {1, 3, 2};
    int[] nonMatchingFirstRangeUpper = {5, 8, 7};

    int[] nonMatchingSecondRangeLower = {11246, 19388, 21248};
    int[] nonMatchingSecondRangeUpper = {13242, 20214, 23236};
    IntPointMultiRangeBuilder builder3 = new IntPointMultiRangeBuilder("point", 3);

    builder3.add(nonMatchingFirstRangeLower, nonMatchingFirstRangeUpper);
    builder3.add(nonMatchingSecondRangeLower, nonMatchingSecondRangeUpper);

    query = builder3.build();

    assertEquals(searcher.count(query), 0);

    // None match
    int[] firstEqualsPointLower= {112, 296, 512};
    int[] firstEqualsPointUpper = {117, 299, 517};

    int[] secondEqualsPointLower = {219, 514, 624};
    int[] secondEqualsPointUpper = {13242, 20214, 23236};

    IntPointMultiRangeBuilder builder4 = new IntPointMultiRangeBuilder("point", 3);

    builder4.add(firstEqualsPointLower, firstEqualsPointUpper);
    builder4.add(secondEqualsPointLower, secondEqualsPointUpper);

    query = builder4.build();

    assertEquals(searcher.count(query), 2);

    reader.close();
    dir.close();
  }

  public void testToString() {
    double[] firstDoubleLowerRange= {111, 294.3, 502.4};
    double[] firstDoubleUpperRange = {117.3, 301.8, 514.3};

    double[] secondDoubleLowerRange = {15.3, 412.8, 415.1};
    double[] secondDoubleUpperRange = {200.4, 567.4, 642.2};

    DoublePointMultiRangeBuilder stringTestbuilder = new DoublePointMultiRangeBuilder("point", 3);

    stringTestbuilder.add(firstDoubleLowerRange, firstDoubleUpperRange);
    stringTestbuilder.add(secondDoubleLowerRange, secondDoubleUpperRange);

    Query query = stringTestbuilder.build();

    assertEquals("point:{[111.0 TO 117.3],[294.3 TO 301.8],[502.4 TO 514.3]},{[15.3 TO 200.4],[412.8 TO 567.4],[415.1 TO 642.2]}",
        query.toString());

    long[] firstLongLowerRange= {111, 294, 502};
    long[] firstLongUpperRange = {117, 301, 514};

    long[] secondLongLowerRange = {15, 412, 415};
    long[] secondLongUpperRange = {200, 567, 642};

    LongPointMultiRangeBuilder stringLongTestbuilder = new LongPointMultiRangeBuilder("point", 3);

    stringLongTestbuilder.add(firstLongLowerRange, firstLongUpperRange);
    stringLongTestbuilder.add(secondLongLowerRange, secondLongUpperRange);

    query = stringLongTestbuilder.build();

    assertEquals("point:{[111 TO 117],[294 TO 301],[502 TO 514]},{[15 TO 200],[412 TO 567],[415 TO 642]}",
        query.toString());

    float[] firstFloatLowerRange= {111.3f, 294.4f, 502.2f};
    float[] firstFloatUpperRange = {117.7f, 301.2f, 514.4f};

    float[] secondFloatLowerRange = {15.3f, 412.2f, 415.9f};
    float[] secondFloatUpperRange = {200.2f, 567.4f, 642.3f};

    FloatPointMultiRangeBuilder stringFloatTestbuilder = new FloatPointMultiRangeBuilder("point", 3);

    stringFloatTestbuilder.add(firstFloatLowerRange, firstFloatUpperRange);
    stringFloatTestbuilder.add(secondFloatLowerRange, secondFloatUpperRange);

    query = stringFloatTestbuilder.build();

    assertEquals("point:{[111.3 TO 117.7],[294.4 TO 301.2],[502.2 TO 514.4]},{[15.3 TO 200.2],[412.2 TO 567.4],[415.9 TO 642.3]}",
        query.toString());

    int[] firstIntLowerRange= {111, 294, 502};
    int[] firstIntUpperRange = {117, 301, 514};

    int[] secondIntLowerRange = {15, 412, 415};
    int[] secondIntUpperRange = {200, 567, 642};

    IntPointMultiRangeBuilder stringIntTestbuilder = new IntPointMultiRangeBuilder("point", 3);

    stringIntTestbuilder.add(firstIntLowerRange, firstIntUpperRange);
    stringIntTestbuilder.add(secondIntLowerRange, secondIntUpperRange);

    query = stringIntTestbuilder.build();

    assertEquals("point:{[111 TO 117],[294 TO 301],[502 TO 514]},{[15 TO 200],[412 TO 567],[415 TO 642]}",
        query.toString());
  }
}
