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

package org.apache.lucene.luwak;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.luwak.presearcher.MatchAllPresearcher;
import org.apache.lucene.luwak.queryparsers.LuceneQueryParser;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;

import static org.hamcrest.core.Is.is;

public class TestCachePurging extends LuceneTestCase {

  public void testQueryCacheCanBePurged() throws IOException, UpdateException {

    final AtomicInteger purgeCount = new AtomicInteger();
    QueryIndexUpdateListener listener = new QueryIndexUpdateListener() {
      @Override
      public void onPurge() {
        purgeCount.incrementAndGet();
      }
    };

    try (Monitor monitor = new Monitor(new LuceneQueryParser("field"), MatchAllPresearcher.INSTANCE)) {
      MonitorQuery[] queries = new MonitorQuery[]{
          new MonitorQuery("1", "test1 test4"),
          new MonitorQuery("2", "test2"),
          new MonitorQuery("3", "test3")
      };
      monitor.addQueryIndexUpdateListener(listener);
      monitor.update(queries);
      assertThat(monitor.getQueryCount(), is(3));
      assertThat(monitor.getDisjunctCount(), is(4));
      assertThat(monitor.getQueryCacheStats().cachedQueries, is(4));

      InputDocument doc = InputDocument.builder("doc1")
          .addField("field", "test1 test2 test3", new StandardAnalyzer()).build();
      assertThat(monitor.match(doc, SimpleMatcher.FACTORY).getMatchCount("doc1"), is(3));

      monitor.deleteById("1");
      assertThat(monitor.getQueryCount(), is(2));
      assertThat(monitor.getQueryCacheStats().cachedQueries, is(4));
      assertThat(monitor.match(doc, SimpleMatcher.FACTORY).getMatchCount("doc1"), is(2));

      monitor.purgeCache();
      assertThat(monitor.getQueryCacheStats().cachedQueries, is(2));

      Matches<QueryMatch> result = monitor.match(doc, SimpleMatcher.FACTORY);
      assertThat(result.getMatchCount("doc1"), is(2));
      assertTrue(purgeCount.get() > 0);
    }
  }

  public void testConcurrentPurges() throws Exception {
    int iters = Integer.getInteger("purgeIters", 2);
    for (int i = 0; i < iters; i++) {
      doConcurrentPurgesAndUpdatesTest();
    }
  }

  private static void doConcurrentPurgesAndUpdatesTest() throws Exception {

    final CountDownLatch startUpdating = new CountDownLatch(1);
    final CountDownLatch finishUpdating = new CountDownLatch(1);

    try (final Monitor monitor = new Monitor(new LuceneQueryParser("field"), MatchAllPresearcher.INSTANCE)) {
      Runnable updaterThread = () -> {
        try {
          startUpdating.await();
          for (int i = 200; i < 400; i++) {
            monitor.update(newMonitorQuery(i));
          }
          finishUpdating.countDown();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      ExecutorService executor = Executors.newFixedThreadPool(1, new NamedThreadFactory("updaters"));
      try {
        executor.submit(updaterThread);

        for (int i = 0; i < 200; i++) {
          monitor.update(newMonitorQuery(i));
        }
        for (int i = 20; i < 80; i++) {
          monitor.deleteById(Integer.toString(i));
        }

        assertEquals(200, monitor.getQueryCacheStats().cachedQueries);

        startUpdating.countDown();
        monitor.purgeCache();
        finishUpdating.await();

        assertEquals(340, monitor.getQueryCacheStats().cachedQueries);
        InputDocument doc = InputDocument.builder("doc1").addField("field", "test", new StandardAnalyzer()).build();
        Matches<QueryMatch> matcher = monitor.match(doc, SimpleMatcher.FACTORY);
        assertEquals(0, matcher.getErrors().size());
        assertEquals(340, matcher.getMatchCount("doc1"));
      } finally {
        executor.shutdownNow();
      }
    }
  }

  private static MonitorQuery newMonitorQuery(int id) {
    return new MonitorQuery(Integer.toString(id), "+test " + Integer.toString(id));
  }

  public void testBackgroundPurges() throws IOException, InterruptedException, UpdateException {

    QueryIndexConfiguration config = new QueryIndexConfiguration().setPurgeFrequency(1, TimeUnit.SECONDS);
    try (Monitor monitor = new Monitor(new LuceneQueryParser("field"), MatchAllPresearcher.INSTANCE, config)) {

      assertEquals(-1, monitor.getQueryCacheStats().lastPurged);

      for (int i = 0; i < 100; i++) {
        monitor.update(newMonitorQuery(i));
      }
      monitor.deleteById("5");
      assertEquals(99, monitor.getQueryCacheStats().queries);
      assertEquals(100, monitor.getQueryCacheStats().cachedQueries);

      TimeUnit.SECONDS.sleep(2);
      assertEquals(99, monitor.getQueryCacheStats().queries);
      assertEquals(99, monitor.getQueryCacheStats().cachedQueries);
      assertTrue(monitor.getQueryCacheStats().lastPurged > 0);
    }
  }
}
