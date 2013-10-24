package org.apache.lucene.facet.search;

import org.apache.lucene.facet.FacetTestCase;
import org.junit.Test;

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

public class TestFacetArrays extends FacetTestCase {

  @Test
  public void testFacetArrays() {
    for (boolean reusing : new boolean[] { false, true }) {
      final FacetArrays arrays;
      if (reusing) {
        arrays = new ReusingFacetArrays(new ArraysPool(1, 1));
      } else {
        arrays = new FacetArrays(1);
      }
      
      int[] intArray = arrays.getIntArray();
      // Set the element, then free
      intArray[0] = 1;
      arrays.free();
      
      // We should expect a cleared array back
      int[] newIntArray = arrays.getIntArray();
      assertEquals("Expected a cleared array back, but the array is still filled", 0, newIntArray[0]);
      
      float[] floatArray = arrays.getFloatArray();
      // Set the element, then free
      floatArray[0] = 1.0f;
      arrays.free();
      
      // We should expect a cleared array back
      float[] newFloatArray = arrays.getFloatArray();
      assertEquals("Expected a cleared array back, but the array is still filled", 0.0f, newFloatArray[0], 0.0);
      
      if (reusing) {
        // same instance should be returned after free()
        assertSame("ReusingFacetArrays did not reuse the array!", intArray, newIntArray);
        assertSame("ReusingFacetArrays did not reuse the array!", floatArray, newFloatArray);
      }
    }
  }
  
}
