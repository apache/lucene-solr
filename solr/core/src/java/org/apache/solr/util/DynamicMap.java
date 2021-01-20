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

package org.apache.solr.util;

/**
 * An efficient map for storing keys as integer in range from 0..n with n can be estimated up-front.
 * By automatically switching from a hashMap (which is memory efficient) to an array (which is faster)
 * on increasing number of keys.
 * So it SHOULD not be used for other cases where key can be any arbitrary integer.
 */
public interface DynamicMap {

  default boolean useArrayBased(int expectedKeyMax) {
    boolean assertsEnabled = false;
    assert assertsEnabled = true; // Intentional side-effect!
    if (assertsEnabled) {
      // avoid using array based up-front on testing
      return false;
    }

    // for small size, prefer using array based
    return expectedKeyMax < (1 << 12);
  }

  /**
   * Compute threshold for switching from hashMap based to array
   */
  default int threshold(int expectedKeyMax) {
    return expectedKeyMax >>> 6;
  }

  /**
   * Compute expected elements for hppc maps, so resizing won't happen if we store less elements than {@code threshold}
   */
  default int mapExpectedElements(int expectedKeyMax) {
    // hppc's expectedElements <= first hppc's resizeAt.
    // +2 let's us not to worry about which comparison operator to choose
    return threshold(expectedKeyMax) + 2;
  }
}
