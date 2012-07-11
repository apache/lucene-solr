package org.apache.lucene.facet.search;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.FloatArrayAllocator;
import org.apache.lucene.facet.search.IntArrayAllocator;

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

public class TestFacetArrays extends LuceneTestCase {

  @Test
  public void testSimple() {
    FacetArrays arrays = new FacetArrays(new IntArrayAllocator(1, 1), new FloatArrayAllocator(1, 1));

    int[] intArray = arrays.getIntArray();
    // Set the element, then free
    intArray[0] = 1;
    arrays.free();

    // We should expect a cleared array back
    intArray = arrays.getIntArray();
    assertEquals("Expected a cleared array back, but the array is still filled", 0, intArray[0]);

    float[] floatArray = arrays.getFloatArray();
    // Set the element, then free
    floatArray[0] = 1.0f;
    arrays.free();

    // We should expect a cleared array back
    floatArray = arrays.getFloatArray();
    assertEquals("Expected a cleared array back, but the array is still filled", 0.0f, floatArray[0], 0.0);
  }
  
}
