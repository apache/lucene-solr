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
package org.apache.lucene.search.similarities;

import org.apache.lucene.util.LuceneTestCase;

public class TestDefaultSimilarity extends LuceneTestCase {

  // Javadocs give this as an example so we test to make sure it's correct:
  public void testPrecisionLoss() throws Exception {
    DefaultSimilarity sim = new DefaultSimilarity();
    float v = sim.decodeNormValue(sim.encodeNormValue(.89f));
    assertEquals(0.875f, v, 0.0001f);
  }

  public void testSaneNormValues() {
    ClassicSimilarity sim = new ClassicSimilarity();
    for (int i = 0; i < 256; i++) {
      float boost = sim.decodeNormValue((byte) i);
      assertFalse("negative boost: " + boost + ", byte=" + i, boost < 0.0f);
      assertFalse("inf bost: " + boost + ", byte=" + i, Float.isInfinite(boost));
      assertFalse("nan boost for byte=" + i, Float.isNaN(boost));
      if (i > 0) {
        assertTrue("boost is not increasing: " + boost + ",byte=" + i, boost > sim.decodeNormValue((byte)(i-1)));
      }
    }
  }
}
