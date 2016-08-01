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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleRangeField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;

/**
 * Random testing for RangeFieldQueries. Testing rigor inspired by {@code BaseGeoPointTestCase}
 */
public class TestDoubleRangeFieldQueries extends BaseRangeFieldQueryTestCase {
  private static final String FIELD_NAME = "rangeField";

  protected DoubleRangeField newRangeField(double[] min, double[] max) {
    return new DoubleRangeField(FIELD_NAME, min, max);
  }

  protected Query newIntersectsQuery(double[] min, double[] max) {
    return DoubleRangeField.newIntersectsQuery(FIELD_NAME, min, max);
  }

  protected Query newContainsQuery(double[] min, double[] max) {
    return DoubleRangeField.newContainsQuery(FIELD_NAME, min, max);
  }

  protected Query newWithinQuery(double[] min, double[] max) {
    return DoubleRangeField.newWithinQuery(FIELD_NAME, min, max);
  }

  /** Basic test */
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // intersects (within)
    Document document = new Document();
    document.add(new DoubleRangeField(FIELD_NAME, new double[] {-10.0, -10.0}, new double[] {9.1, 10.1}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new DoubleRangeField(FIELD_NAME, new double[] {10.0, -10.0}, new double[] {20.0, 10.0}));
    writer.addDocument(document);

    // intersects (contains)
    document = new Document();
    document.add(new DoubleRangeField(FIELD_NAME, new double[] {-20.0, -20.0}, new double[] {30.0, 30.1}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new DoubleRangeField(FIELD_NAME, new double[] {-11.1, -11.2}, new double[] {1.23, 11.5}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new DoubleRangeField(FIELD_NAME, new double[] {12.33, 1.2}, new double[] {15.1, 29.9}));
    writer.addDocument(document);

    // disjoint
    document = new Document();
    document.add(new DoubleRangeField(FIELD_NAME, new double[] {-122.33, 1.2}, new double[] {-115.1, 29.9}));
    writer.addDocument(document);

    // intersects (crosses)
    document = new Document();
    document.add(new DoubleRangeField(FIELD_NAME, new double[] {Double.NEGATIVE_INFINITY, 1.2}, new double[] {-11.0, 29.9}));
    writer.addDocument(document);

    // equal (within, contains, intersects)
    document = new Document();
    document.add(new DoubleRangeField(FIELD_NAME, new double[] {-11, -15}, new double[] {15, 20}));
    writer.addDocument(document);

    // search
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(7, searcher.count(DoubleRangeField.newIntersectsQuery(FIELD_NAME,
        new double[] {-11.0, -15.0}, new double[] {15.0, 20.0})));
    assertEquals(2, searcher.count(DoubleRangeField.newWithinQuery(FIELD_NAME,
        new double[] {-11.0, -15.0}, new double[] {15.0, 20.0})));
    assertEquals(2, searcher.count(DoubleRangeField.newContainsQuery(FIELD_NAME,
        new double[] {-11.0, -15.0}, new double[] {15.0, 20.0})));

    reader.close();
    writer.close();
    dir.close();
  }
}
