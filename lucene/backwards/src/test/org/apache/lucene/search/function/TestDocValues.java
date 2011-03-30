package org.apache.lucene.search.function;

/**
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

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

/**
 * DocValues TestCase  
 */
public class TestDocValues extends LuceneTestCase {

  @Test
  public void testGetMinValue() {
    float[] innerArray = new float[] { 1.0f, 2.0f, -1.0f, 100.0f };
    DocValuesTestImpl docValues = new DocValuesTestImpl(innerArray);
    assertEquals("-1.0f is the min value in the source array", -1.0f, docValues
        .getMinValue(), 0);

    // test with without values - NaN
    innerArray = new float[] {};
    docValues = new DocValuesTestImpl(innerArray);
    assertTrue("max is NaN - no values in inner array", Float.isNaN(docValues
        .getMinValue()));
  }
  @Test
  public void testGetMaxValue() {
    float[] innerArray = new float[] { 1.0f, 2.0f, -1.0f, 10.0f };
    DocValuesTestImpl docValues = new DocValuesTestImpl(innerArray);
    assertEquals("10.0f is the max value in the source array", 10.0f, docValues
        .getMaxValue(), 0);

    innerArray = new float[] { -3.0f, -1.0f, -100.0f };
    docValues = new DocValuesTestImpl(innerArray);
    assertEquals("-1.0f is the max value in the source array", -1.0f, docValues
        .getMaxValue(), 0);

    innerArray = new float[] { -3.0f, -1.0f, 100.0f, Float.MAX_VALUE,
        Float.MAX_VALUE - 1 };
    docValues = new DocValuesTestImpl(innerArray);
    assertEquals(Float.MAX_VALUE + " is the max value in the source array",
        Float.MAX_VALUE, docValues.getMaxValue(), 0);

    // test with without values - NaN
    innerArray = new float[] {};
    docValues = new DocValuesTestImpl(innerArray);
    assertTrue("max is NaN - no values in inner array", Float.isNaN(docValues
        .getMaxValue()));
  }

  @Test
  public void testGetAverageValue() {
    float[] innerArray = new float[] { 1.0f, 1.0f, 1.0f, 1.0f };
    DocValuesTestImpl docValues = new DocValuesTestImpl(innerArray);
    assertEquals("the average is 1.0f", 1.0f, docValues.getAverageValue(), 0);

    innerArray = new float[] { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f };
    docValues = new DocValuesTestImpl(innerArray);
    assertEquals("the average is 3.5f", 3.5f, docValues.getAverageValue(), 0);

    // test with negative values
    innerArray = new float[] { -1.0f, 2.0f };
    docValues = new DocValuesTestImpl(innerArray);
    assertEquals("the average is 0.5f", 0.5f, docValues.getAverageValue(), 0);

    // test with without values - NaN
    innerArray = new float[] {};
    docValues = new DocValuesTestImpl(innerArray);
    assertTrue("the average is NaN - no values in inner array", Float
        .isNaN(docValues.getAverageValue()));
  }

  static class DocValuesTestImpl extends DocValues {
    float[] innerArray;

    DocValuesTestImpl(float[] innerArray) {
      this.innerArray = innerArray;
    }

    /**
     * @see org.apache.lucene.search.function.DocValues#floatVal(int)
     */
    @Override
    public float floatVal(int doc) {
      return innerArray[doc];
    }

    /**
     * @see org.apache.lucene.search.function.DocValues#toString(int)
     */
    @Override
    public String toString(int doc) {
      return Integer.toString(doc);
    }

  }

}
