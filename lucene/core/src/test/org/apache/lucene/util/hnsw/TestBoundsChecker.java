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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.util.LuceneTestCase;

public class TestBoundsChecker extends LuceneTestCase {

  public void testMax() {
    BoundsChecker max = BoundsChecker.create(false);
    float f = random().nextFloat() - 0.5f;
    // any float > -MAX_VALUE is in bounds
    assertFalse(max.check(f));
    // f is now the bound (minus some delta)
    max.update(f);
    assertFalse(max.check(f)); // f is not out of bounds
    assertFalse(max.check(f + 1)); // anything greater than f is in bounds
    assertTrue(max.check(f - 1e-5f)); // delta is zero initially
  }

  public void testMaxDelta() {
    BoundsChecker max = BoundsChecker.create(false);
    max.set(0, 100); // set the bound to 0, with a large delta
    assertFalse(max.check(0)); // 0 is not out of bounds
    assertFalse(max.check(1000)); // anything greater than 0 is in bounds
    assertFalse(max.check(-19)); // delta is 20
    assertTrue(max.check(-20)); // delta is 20
  }

  public void testMin() {
    BoundsChecker min = BoundsChecker.create(true);
    float f = random().nextFloat() - 0.5f;
    // any float < MAX_VALUE is in bounds
    assertFalse(min.check(f));
    // f is now the bound (minus some delta)
    min.update(f);
    assertFalse(min.check(f)); // f is not out of bounds
    assertFalse(min.check(f - 1)); // anything less than f is in bounds
    assertTrue(min.check(f + 1e-5f)); // delta is zero initially
  }

  public void testMinDelta() {
    BoundsChecker min = BoundsChecker.create(true);
    min.set(0, -100); // set the bound to 0, with a large delta
    assertFalse(min.check(0)); // 0 is not out of bounds
    assertFalse(min.check(-1000)); // anything less than 0 is in bounds
    assertFalse(min.check(19)); // delta is 20
    assertTrue(min.check(20)); // delta is 20
  }

}
