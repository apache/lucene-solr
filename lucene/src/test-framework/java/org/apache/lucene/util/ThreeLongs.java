package org.apache.lucene.util;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** helper class for a random seed that is really 3 random seeds:
 *  <ol>
 *   <li>The test class's random seed: this is what the test sees in its beforeClass methods
 *   <li>The test method's random seed: this is what the test method sees starting in its befores
 *   <li>The test runner's random seed (controls the shuffling of test methods)
 *  </ol>
 */
class ThreeLongs {
  public final long l1, l2, l3;
  
  public ThreeLongs(long l1, long l2, long l3) {
    this.l1 = l1;
    this.l2 = l2;
    this.l3 = l3;
  }
  
  @Override
  public String toString() {
    return Long.toString(l1, 16) + ":" + Long.toString(l2, 16) + ":" + Long.toString(l3, 16);
  }
  
  public static ThreeLongs fromString(String s) {
    String parts[] = s.split(":");
    assert parts.length == 3;
    return new ThreeLongs(Long.parseLong(parts[0], 16), Long.parseLong(parts[1], 16), Long.parseLong(parts[2], 16));
  }
}
