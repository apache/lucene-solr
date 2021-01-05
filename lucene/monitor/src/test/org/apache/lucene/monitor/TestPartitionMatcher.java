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

package org.apache.lucene.monitor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class TestPartitionMatcher extends ConcurrentMatcherTestBase {

  @Override
  protected <T extends QueryMatch> MatcherFactory<T> matcherFactory(
      ExecutorService executor, MatcherFactory<T> factory, int threads) {
    return PartitionMatcher.factory(executor, factory, threads);
  }

  public void testPartitions() {

    List<String> terms = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");

    List<List<String>> partitions = PartitionMatcher.partition(terms, 2);
    assertTrue(partitions.contains(Arrays.asList("1", "2", "3", "4", "5")));
    assertTrue(partitions.contains(Arrays.asList("6", "7", "8", "9", "10")));

    partitions = PartitionMatcher.partition(terms, 3);
    assertTrue(partitions.contains(Arrays.asList("1", "2", "3")));
    assertTrue(partitions.contains(Arrays.asList("4", "5", "6")));
    assertTrue(partitions.contains(Arrays.asList("7", "8", "9", "10")));

    partitions = PartitionMatcher.partition(terms, 4);
    assertTrue(partitions.contains(Arrays.asList("1", "2")));
    assertTrue(partitions.contains(Arrays.asList("3", "4", "5")));
    assertTrue(partitions.contains(Arrays.asList("6", "7")));
    assertTrue(partitions.contains(Arrays.asList("8", "9", "10")));

    partitions = PartitionMatcher.partition(terms, 6);
    assertTrue(partitions.contains(Collections.singletonList("1")));
    assertTrue(partitions.contains(Arrays.asList("2", "3")));
    assertTrue(partitions.contains(Arrays.asList("4", "5")));
    assertTrue(partitions.contains(Collections.singletonList("6")));
    assertTrue(partitions.contains(Arrays.asList("7", "8")));
    assertTrue(partitions.contains(Arrays.asList("9", "10")));
  }
}
