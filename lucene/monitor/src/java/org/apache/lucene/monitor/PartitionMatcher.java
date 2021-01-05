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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/**
 * A multi-threaded matcher that collects all possible matches in one pass, and then partitions them
 * amongst a number of worker threads to perform the actual matching.
 *
 * <p>This class delegates the matching to separate CandidateMatcher classes, built from a passed in
 * MatcherFactory.
 *
 * <p>Use this if your query sets contain large numbers of very fast queries, where the
 * synchronization overhead of {@link ParallelMatcher} can outweigh the benefit of multithreading.
 *
 * @param <T> the type of QueryMatch to return
 * @see ParallelMatcher
 */
public class PartitionMatcher<T extends QueryMatch> extends CandidateMatcher<T> {

  private final ExecutorService executor;

  private final MatcherFactory<T> matcherFactory;

  private final int threads;

  private final CandidateMatcher<T> resolvingMatcher;

  private static class MatchTask {

    final String queryId;
    final Query matchQuery;
    final Map<String, String> metadata;

    private MatchTask(String queryId, Query matchQuery, Map<String, String> metadata) {
      this.queryId = queryId;
      this.matchQuery = matchQuery;
      this.metadata = metadata;
    }
  }

  private final List<MatchTask> tasks = new ArrayList<>();

  private PartitionMatcher(
      IndexSearcher searcher,
      ExecutorService executor,
      MatcherFactory<T> matcherFactory,
      int threads) {
    super(searcher);
    this.executor = executor;
    this.matcherFactory = matcherFactory;
    this.threads = threads;
    this.resolvingMatcher = matcherFactory.createMatcher(searcher);
  }

  @Override
  protected void matchQuery(String queryId, Query matchQuery, Map<String, String> metadata) {
    tasks.add(new MatchTask(queryId, matchQuery, metadata));
  }

  @Override
  public T resolve(T match1, T match2) {
    return resolvingMatcher.resolve(match1, match2);
  }

  @Override
  protected void doFinish() {

    List<Callable<MultiMatchingQueries<T>>> workers = new ArrayList<>(threads);
    for (List<MatchTask> taskset : partition(tasks, threads)) {
      CandidateMatcher<T> matcher = matcherFactory.createMatcher(searcher);
      workers.add(new MatcherWorker(taskset, matcher));
    }

    try {
      for (Future<MultiMatchingQueries<T>> future : executor.invokeAll(workers)) {
        MultiMatchingQueries<T> matches = future.get();
        for (int doc = 0; doc < matches.getBatchSize(); doc++) {
          for (T match : matches.getMatches(doc)) {
            addMatch(match, doc);
          }
        }
      }

    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Interrupted during match", e);
    }
  }

  private class MatcherWorker implements Callable<MultiMatchingQueries<T>> {

    final List<MatchTask> tasks;
    final CandidateMatcher<T> matcher;

    private MatcherWorker(List<MatchTask> tasks, CandidateMatcher<T> matcher) {
      this.tasks = tasks;
      this.matcher = matcher;
    }

    @Override
    public MultiMatchingQueries<T> call() {
      for (MatchTask task : tasks) {
        try {
          matcher.matchQuery(task.queryId, task.matchQuery, task.metadata);
        } catch (IOException e) {
          PartitionMatcher.this.reportError(task.queryId, e);
        }
      }
      return matcher.finish(0, 0);
    }
  }

  private static class PartitionMatcherFactory<T extends QueryMatch> implements MatcherFactory<T> {

    private final ExecutorService executor;
    private final MatcherFactory<T> matcherFactory;
    private final int threads;

    PartitionMatcherFactory(
        ExecutorService executor, MatcherFactory<T> matcherFactory, int threads) {
      this.executor = executor;
      this.matcherFactory = matcherFactory;
      this.threads = threads;
    }

    @Override
    public PartitionMatcher<T> createMatcher(IndexSearcher searcher) {
      return new PartitionMatcher<>(searcher, executor, matcherFactory, threads);
    }
  }

  /**
   * Create a new MatcherFactory for a PartitionMatcher
   *
   * @param executor the ExecutorService to use
   * @param matcherFactory the MatcherFactory to use to create submatchers
   * @param threads the number of threads to use
   * @param <T> the type of QueryMatch generated
   */
  public static <T extends QueryMatch> MatcherFactory<T> factory(
      ExecutorService executor, MatcherFactory<T> matcherFactory, int threads) {
    return new PartitionMatcherFactory<>(executor, matcherFactory, threads);
  }

  /**
   * Create a new MatcherFactory for a PartitionMatcher
   *
   * <p>This factory will create a PartitionMatcher that uses as many threads as there are cores
   * available to the JVM (as determined by {@code Runtime.getRuntime().availableProcessors()}).
   *
   * @param executor the ExecutorService to use
   * @param matcherFactory the MatcherFactory to use to create submatchers
   * @param <T> the type of QueryMatch generated
   */
  public static <T extends QueryMatch> MatcherFactory<T> factory(
      ExecutorService executor, MatcherFactory<T> matcherFactory) {
    int threads = Runtime.getRuntime().availableProcessors();
    return new PartitionMatcherFactory<>(executor, matcherFactory, threads);
  }

  static <T> List<List<T>> partition(List<T> items, int slices) {
    double size = items.size() / (double) slices;
    double accum = 0;
    int start = 0;
    List<List<T>> list = new ArrayList<>(slices);
    for (int i = 0; i < slices; i++) {
      int end = (int) Math.floor(accum + size);
      if (i == slices - 1) end = items.size();
      list.add(items.subList(start, end));
      accum += size;
      start = (int) Math.floor(accum);
    }
    return list;
  }
}
