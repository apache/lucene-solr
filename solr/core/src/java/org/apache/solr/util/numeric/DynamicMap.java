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

package org.apache.solr.util.numeric;

import com.carrotsearch.hppc.HashContainers;

public interface DynamicMap {
  static boolean useArrayBased(int expectedMaxSize) {
    // for small size, prefer using array based
    return expectedMaxSize < (1 << 12);
  }

  static int threshold(int expectedMaxSize) {
    return expectedMaxSize >>> 6;
  }

  static int mapExpectedElements(int expectedMaxSize) {
    return (int) (threshold(expectedMaxSize) / HashContainers.DEFAULT_LOAD_FACTOR);
  }
}
