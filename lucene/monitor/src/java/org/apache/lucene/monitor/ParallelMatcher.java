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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/**
 * Matcher class that runs matching queries in parallel.
 *
 * <p>This class delegates the actual matching to separate CandidateMatcher classes, built from a
 * passed in MatcherFactory.
 *
 * <p>Use this when individual queries can take a long time to run, and you want to minimize
 * latency. The matcher distributes queries amongst its worker threads using a BlockingQueue, and
 * synchronization overhead may affect performance if the individual queries are very fast.
 *
 * @param <T> the QueryMatch type returned
 * @see PartitionMatcher
 */
public class ParallelMatcher<T extends QueryMatch> extends CandidateMatcher<T> {

  private final BlockingQueue<MatcherTask> queue = new LinkedBlockingQueue<>(1024);

  private final List<Future<CandidateMatcher<T>>> futures = new ArrayList<>();

  private final CandidateMatcher<T> collectorMatcher;

  /**
   * Create a new ParallelMatcher
   *
   * @param searcher the IndexSearcher to match against
   * @param executor an ExecutorService to use for parallel execution
   * @param matcherFactory MatcherFactory to use to create CandidateMatchers
   * @param threads the number of threads to execute on
   */
  private ParallelMatcher(
      IndexSearcher searcher,
      ExecutorService executor,
      MatcherFactory<T> matcherFactory,
      int threads) {
    super(searcher);
    for (int i = 0; i < threads; i++) {
      MatcherWorker mw = new MatcherWorker(matcherFactory);
      futures.add(executor.submit(mw));
    }
    collectorMatcher = matcherFactory.createMatcher(searcher);
  }

  @Override
  protected void matchQuery(String queryId, Query matchQuery, Map<String, String> metadata)
      throws IOException {
    try {
      queue.put(new MatcherTask(queryId, matchQuery, metadata));
    } catch (InterruptedException e) {
      throw new IOException("Interrupted during match", e);
    }
  }

  @Override
  public T resolve(T match1, T match2) {
    return collectorMatcher.resolve(match1, match2);
  }

  @Override
  protected void doFinish() {
    try {
      for (int i = 0; i < futures.size(); i++) {
        queue.put(END);
      }

      for (Future<CandidateMatcher<T>> future : futures) {
        MultiMatchingQueries<T> matches = future.get().finish(0, 0);
        for (int doc = 0; doc < matches.getBatchSize(); doc++) {
          for (T match : matches.getMatches(doc)) {
            this.addMatch(match, doc);
          }
        }
        for (Map.Entry<String, Exception> error : matches.getErrors().entrySet()) {
          this.reportError(error.getKey(), error.getValue());
        }
      }

    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Interrupted during match", e);
    }
  }

  private class MatcherWorker implements Callable<CandidateMatcher<T>> {

    final CandidateMatcher<T> matcher;

    private MatcherWorker(MatcherFactory<T> matcherFactory) {
      this.matcher = matcherFactory.createMatcher(searcher);
    }

    @Override
    public CandidateMatcher<T> call() {
      MatcherTask task;
      try {
        while ((task = queue.take()) != END) {
          try {
            matcher.matchQuery(task.id, task.matchQuery, task.metadata);
          } catch (IOException e) {
            matcher.reportError(task.id, e);
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted during match", e);
      }
      return matcher;
    }
  }

  private static class MatcherTask {

    final String id;
    final Query matchQuery;
    final Map<String, String> metadata;

    private MatcherTask(String id, Query matchQuery, Map<String, String> metadata) {
      this.id = id;
      this.matchQuery = matchQuery;
      this.metadata = metadata;
    }
  }

  /* Marker object placed on the queue after all matches are done, to indicate to the
  worker threads that they should finish */
  private static final MatcherTask END = new MatcherTask("", null, Collections.emptyMap());

  private static class ParallelMatcherFactory<T extends QueryMatch> implements MatcherFactory<T> {

    private final ExecutorService executor;
    private final MatcherFactory<T> matcherFactory;
    private final int threads;

    ParallelMatcherFactory(
        ExecutorService executor, MatcherFactory<T> matcherFactory, int threads) {
      this.executor = executor;
      this.matcherFactory = matcherFactory;
      this.threads = threads;
    }

    @Override
    public ParallelMatcher<T> createMatcher(IndexSearcher searcher) {
      return new ParallelMatcher<>(searcher, executor, matcherFactory, threads);
    }
  }

  /**
   * Create a new MatcherFactory for a ParallelMatcher
   *
   * @param executor the ExecutorService to use
   * @param matcherFactory the MatcherFactory to use to create submatchers
   * @param threads the number of threads to use
   * @param <T> the type of QueryMatch generated
   */
  public static <T extends QueryMatch> MatcherFactory<T> factory(
      ExecutorService executor, MatcherFactory<T> matcherFactory, int threads) {
    return new ParallelMatcherFactory<>(executor, matcherFactory, threads);
  }

  /**
   * Create a new MatcherFactory for a ParallelMatcher
   *
   * <p>This factory will create a ParallelMatcher that uses as many threads as there are cores
   * available to the JVM (as determined by {@code Runtime.getRuntime().availableProcessors()}).
   *
   * @param executor the ExecutorService to use
   * @param matcherFactory the MatcherFactory to use to create submatchers
   * @param <T> the type of QueryMatch generated
   */
  public static <T extends QueryMatch> MatcherFactory<T> factory(
      ExecutorService executor, MatcherFactory<T> matcherFactory) {
    int threads = Runtime.getRuntime().availableProcessors();
    return new ParallelMatcherFactory<>(executor, matcherFactory, threads);
  }
}
