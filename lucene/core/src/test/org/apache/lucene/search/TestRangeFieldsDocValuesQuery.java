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
import org.apache.lucene.document.DoubleRangeDocValuesField;
import org.apache.lucene.document.FloatRangeDocValuesField;
import org.apache.lucene.document.IntRangeDocValuesField;
import org.apache.lucene.document.LongRangeDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestRangeFieldsDocValuesQuery extends LuceneTestCase {
  public void testDoubleRangeDocValuesIntersectsQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int iters = atLeast(10);
    double[] min = {112.7, 296.0, 512.4};
    double[] max = {119.3, 314.8, 524.3};
    for (int i = 0; i < iters; ++i) {
      Document doc = new Document();
      doc.add(new DoubleRangeDocValuesField("dv", min, max));
      iw.addDocument(doc);
    }
    iw.commit();

    double[] nonMatchingMin = {256.7, 296.0, 532.4};
    double[] nonMatchingMax = {259.3, 364.8, 534.3};

    Document doc = new Document();
    doc.add(new DoubleRangeDocValuesField("dv", nonMatchingMin, nonMatchingMax));
    iw.addDocument(doc);
    iw.commit();

    IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();

    final double[] lowRange = {111.3, 294.4, 517.4};
    final double[] highRange = {116.7, 319.4, 533.0};

    Query query = DoubleRangeDocValuesField.newSlowIntersectsQuery("dv", lowRange, highRange);
    assertEquals(searcher.count(query), iters);

    double[] lowRange2 = {116.3, 299.3, 517.0};
    double[] highRange2 = {121.0, 317.1, 531.2};

    query = DoubleRangeDocValuesField.newSlowIntersectsQuery( "dv", lowRange2, highRange2);

    assertEquals(searcher.count(query), iters);

    reader.close();
    dir.close();
  }

  public void testIntRangeDocValuesIntersectsQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int iters = atLeast(10);
    int[] min = {3, 11, 17};
    int[] max = {27, 35, 49};
    for (int i = 0; i < iters; ++i) {
      Document doc = new Document();
      doc.add(new IntRangeDocValuesField("dv", min, max));
      iw.addDocument(doc);
    }

    int[] min2 = {11, 19, 27};
    int[] max2 = {29, 38, 56};

    Document doc = new Document();
    doc.add(new IntRangeDocValuesField("dv", min2, max2));

    iw.commit();

    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();

    final int[] lowRange = {6, 16, 19};
    final int[] highRange = {29, 41, 42};

    Query query = IntRangeDocValuesField.newSlowIntersectsQuery("dv", lowRange, highRange);

    assertEquals(searcher.count(query), iters);

    int[] lowRange2 = {2, 9, 18};
    int[] highRange2 = {25, 34, 41};

    query = IntRangeDocValuesField.newSlowIntersectsQuery("dv", lowRange2, highRange2);

    assertEquals(searcher.count(query), iters);

    int[] lowRange3 = {101, 121, 153};
    int[] highRange3 = {156, 127, 176};

    query = IntRangeDocValuesField.newSlowIntersectsQuery("dv", lowRange3, highRange3);

    assertEquals(searcher.count(query), 0);

    reader.close();
    dir.close();
  }

  public void testLongRangeDocValuesIntersectQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int iters = atLeast(10);
    long[] min = {31, 15, 2};
    long[] max = {95, 27, 4};
    for (int i = 0; i < iters; ++i) {
      Document doc = new Document();
      doc.add(new LongRangeDocValuesField("dv", min, max));
      iw.addDocument(doc);
    }

    long[] min2 = {101, 124, 137};
    long[] max2 = {138, 145, 156};
    Document doc = new Document();
    doc.add(new LongRangeDocValuesField("dv", min2, max2));

    iw.commit();

    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();

    final long[] lowRange = {6, 12, 1};
    final long[] highRange = {34, 24, 3};

    Query query = LongRangeDocValuesField.newSlowIntersectsQuery("dv", lowRange, highRange);

    assertEquals(searcher.count(query), iters);

    final long[] lowRange2 = {32, 18, 3};
    final long[] highRange2 = {96, 29, 5};

    query = LongRangeDocValuesField.newSlowIntersectsQuery("dv", lowRange2, highRange2);

    assertEquals(searcher.count(query), iters);

    reader.close();
    dir.close();
  }

  public void testFloatRangeDocValuesIntersectQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int iters = atLeast(10);
    float[] min = {3.7f, 11.0f, 33.4f};
    float[] max = {8.3f, 21.6f, 59.8f};
    for (int i = 0; i < iters; ++i) {
      Document doc = new Document();
      doc.add(new FloatRangeDocValuesField("dv", min, max));
      iw.addDocument(doc);
    }


    float[] nonMatchingMin = {11.4f, 29.7f, 102.4f};
    float[] nonMatchingMax = {17.6f, 37.2f, 160.2f};
    Document doc = new Document();
    doc.add(new FloatRangeDocValuesField("dv", nonMatchingMin, nonMatchingMax));
    iw.addDocument(doc);

    iw.commit();

    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();

    final float[] lowRange = {1.2f, 8.3f, 21.4f};
    final float[] highRange = {6.0f, 17.6f, 47.1f};

    Query query = FloatRangeDocValuesField.newSlowIntersectsQuery("dv", lowRange, highRange);

    assertEquals(searcher.count(query), iters);

    final float[] lowRange2 = {6.1f, 17.0f, 31.3f};
    final float[] highRange2 = {14.2f, 23.4f, 61.1f};

    query = FloatRangeDocValuesField.newSlowIntersectsQuery("dv", lowRange2, highRange2);

    assertEquals(searcher.count(query), iters);

    reader.close();
    dir.close();
  }

  public void testToString() {
    double[] doubleMin = {112.7, 296.0, 512.4f};
    double[] doubleMax = {119.3, 314.8, 524.3f};
    Query q1 = DoubleRangeDocValuesField.newSlowIntersectsQuery("foo", doubleMin, doubleMax);
    assertEquals("foo:[[112.7, 296.0, 512.4000244140625] TO [119.3, 314.8, 524.2999877929688]]", q1.toString());

    int[] intMin = {3, 11, 17};
    int[] intMax = {27, 35, 49};
    Query q2 = IntRangeDocValuesField.newSlowIntersectsQuery("foo", intMin, intMax);
    assertEquals("foo:[[3, 11, 17] TO [27, 35, 49]]", q2.toString());

    float[] floatMin = {3.7f, 11.0f, 33.4f};
    float[] floatMax = {8.3f, 21.6f, 59.8f};
    Query q3 = FloatRangeDocValuesField.newSlowIntersectsQuery("foo", floatMin, floatMax);
    assertEquals("foo:[[3.7, 11.0, 33.4] TO [8.3, 21.6, 59.8]]", q3.toString());

    long[] longMin = {101, 124, 137};
    long[] longMax = {138, 145, 156};
    Query q4 = LongRangeDocValuesField.newSlowIntersectsQuery("foo", longMin, longMax);
    assertEquals("foo:[[101, 124, 137] TO [138, 145, 156]]", q4.toString());
  }
}
