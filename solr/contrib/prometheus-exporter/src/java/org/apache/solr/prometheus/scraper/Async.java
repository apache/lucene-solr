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

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Async {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static <T> CompletableFuture<List<T>> waitForAllSuccessfulResponses(List<CompletableFuture<T>> futures) {
    CompletableFuture<Void> completed = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    return completed.thenApply(values -> {
        return futures.stream()
          .map(CompletableFuture::join)
          .collect(Collectors.toList());
      }
    ).exceptionally(error -> {
      futures.stream()
          .filter(CompletableFuture::isCompletedExceptionally)
          .forEach(future -> {
            try {
              future.get();
            } catch (Exception exception) {
              log.warn("Error occurred during metrics collection", exception);
            }
          });

      return futures.stream()
          .filter(future -> !(future.isCompletedExceptionally() || future.isCancelled()))
          .map(CompletableFuture::join)
          .collect(Collectors.toList());
      }
    );
  }


}
