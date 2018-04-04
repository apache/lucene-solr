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
package org.apache.lucene.document;

import org.apache.lucene.util.LuceneTestCase;

/**
 * Random testing for RangeField type.
 **/
public class TestDoubleRangeField extends LuceneTestCase {
  private static final String FIELD_NAME = "rangeField";

  /** test illegal NaN range values */
  public void testIllegalNaNValues() {
    Document doc = new Document();
    IllegalArgumentException expected;

    expected = expectThrows(IllegalArgumentException.class, () ->
        doc.add(new DoubleRange(FIELD_NAME, new double[] {Double.NaN}, new double[] {5})));
    assertTrue(expected.getMessage().contains("invalid min value"));

    expected = expectThrows(IllegalArgumentException.class, () ->
        doc.add(new DoubleRange(FIELD_NAME, new double[] {5}, new double[] {Double.NaN})));
    assertTrue(expected.getMessage().contains("invalid max value"));
  }

  /** min/max array sizes must agree */
  public void testUnevenArrays() {
    Document doc = new Document();
    IllegalArgumentException expected;
    expected = expectThrows(IllegalArgumentException.class, () ->
        doc.add(new DoubleRange(FIELD_NAME, new double[] {5, 6}, new double[] {5})));
    assertTrue(expected.getMessage().contains("min/max ranges must agree"));
  }

  /** dimensions greater than 4 not supported */
  public void testOversizeDimensions() {
    Document doc = new Document();
    IllegalArgumentException expected;
    expected = expectThrows(IllegalArgumentException.class, () ->
        doc.add(new DoubleRange(FIELD_NAME, new double[] {1, 2, 3, 4, 5}, new double[] {5})));
    assertTrue(expected.getMessage().contains("does not support greater than 4 dimensions"));
  }

  /** min cannot be greater than max */
  public void testMinGreaterThanMax() {
    Document doc = new Document();
    IllegalArgumentException expected;
    expected = expectThrows(IllegalArgumentException.class, () ->
      doc.add(new DoubleRange(FIELD_NAME, new double[] {3, 4}, new double[] {1, 2})));
    assertTrue(expected.getMessage().contains("is greater than max value"));
  }
}
