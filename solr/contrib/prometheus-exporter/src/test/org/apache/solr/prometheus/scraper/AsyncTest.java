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

package org.apache.solr.prometheus.scraper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AsyncTest {

  private CompletableFuture<Integer> failedFuture() {
    CompletableFuture<Integer> result = new CompletableFuture<>();
    result.completeExceptionally(new RuntimeException("Some error"));
    return result;
  }

  @Test
  public void getAllResults() throws Exception {
    List<Integer> expectedValues = Arrays.asList(1, 2, 3, 4, 5, 6, 7);

    CompletableFuture<List<Integer>> results = Async.waitForAllSuccessfulResponses(
        expectedValues.stream()
            .map(CompletableFuture::completedFuture)
            .collect(Collectors.toList()));

    List<Integer> actualValues = results.get();

    Collections.sort(expectedValues);
    Collections.sort(actualValues);

    assertEquals(expectedValues, actualValues);
  }

  @Test
  public void ignoresFailures() throws Exception {
    CompletableFuture<List<Integer>> results = Async.waitForAllSuccessfulResponses(Arrays.asList(
        CompletableFuture.completedFuture(1),
        failedFuture()
    ));

    List<Integer> values = results.get();

    assertEquals(Collections.singletonList(1), values);
  }

  @Test
  public void allFuturesFail() throws Exception {
    CompletableFuture<List<Integer>> results = Async.waitForAllSuccessfulResponses(Collections.singletonList(
        failedFuture()
    ));

    List<Integer> values = results.get();

    assertTrue(values.isEmpty());
  }
}