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
package org.apache.lucene.codecs;


import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedRunner;

/* WARNING: This test does *not* extend LuceneTestCase to prevent static class
 * initialization when spawned as subprocess (and please let default codecs alive)! */

@RunWith(RandomizedRunner.class)
public class TestCodecLoadingDeadlock extends Assert {
  private static int MAX_TIME_SECONDS = 30;

  @Test
  public void testDeadlock() throws Exception {
    LuceneTestCase.assumeFalse("This test fails on UNIX with Turkish default locale (https://issues.apache.org/jira/browse/LUCENE-6036)",
      new Locale("tr").getLanguage().equals(Locale.getDefault().getLanguage()));

    // pick random codec names for stress test in separate process:
    final Random rnd = RandomizedContext.current().getRandom();
    Set<String> avail;
    final String codecName = new ArrayList<>(avail = Codec.availableCodecs())
        .get(rnd.nextInt(avail.size()));
    final String pfName = new ArrayList<>(avail = PostingsFormat.availablePostingsFormats())
        .get(rnd.nextInt(avail.size()));
    final String dvfName = new ArrayList<>(avail = DocValuesFormat.availableDocValuesFormats())
        .get(rnd.nextInt(avail.size()));

    System.out.println(String.format(Locale.ROOT,
        "codec: %s, pf: %s, dvf: %s", codecName, pfName, dvfName));

    // Fork a separate JVM to reinitialize classes.
    final Process p = new ProcessBuilder(
      Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
      "-cp",
      System.getProperty("java.class.path"),
      getClass().getName(),
      codecName,
      pfName,
      dvfName
    ).inheritIO().start();
    if (p.waitFor(MAX_TIME_SECONDS * 2, TimeUnit.SECONDS)) {
      assertEquals("Process died abnormally?", 0, p.waitFor());
    } else {
      p.destroyForcibly().waitFor();
      fail("Process did not exit after 60 secs?");
    }
  }

  // This method is called in a spawned process.
  public static void main(final String... args) throws Exception {
    final String codecName = args[0];
    final String pfName = args[1];
    final String dvfName = args[2];

    final int numThreads = 14; // two times the modulo in switch statement below
    final CopyOnWriteArrayList<Thread> allThreads = new CopyOnWriteArrayList<>();
    final ExecutorService pool = Executors.newFixedThreadPool(numThreads, new NamedThreadFactory("deadlockchecker") {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = super.newThread(r);
        allThreads.add(t);
        return t;
      }
    });

    final CyclicBarrier barrier = new CyclicBarrier(numThreads);
    IntStream.range(0, numThreads).forEach(taskNo -> pool.execute(() -> {
      try {
        // Await a common barrier point for all threads and then
        // run racy code. This is intentional.
        barrier.await();
        switch (taskNo % 7) {
          case 0:
            Codec.getDefault();
            break;
          case 1:
            Codec.forName(codecName);
            break;
          case 2:
            PostingsFormat.forName(pfName);
            break;
          case 3:
            DocValuesFormat.forName(dvfName);
            break;
          case 4:
            Codec.availableCodecs();
            break;
          case 5:
            PostingsFormat.availablePostingsFormats();
            break;
          case 6:
            DocValuesFormat.availableDocValuesFormats();
            break;
          default:
            throw new AssertionError();
        }
      } catch (Throwable t) {
        synchronized(args) {
          System.err.println(Thread.currentThread().getName() + " failed to lookup codec service:");
          t.printStackTrace(System.err);
        }
        Runtime.getRuntime().halt(1); // signal failure to caller
      }
    }));

    pool.shutdown();

    if (!pool.awaitTermination(MAX_TIME_SECONDS, TimeUnit.SECONDS)) {
      // Try to collect stacks so that we can better diagnose the failure.
      System.err.println("Pool didn't return after " + MAX_TIME_SECONDS +
          " seconds, classloader deadlock? Dumping stack traces.");

      for (Thread t : allThreads) {
        System.err.println(
            "# Thread: " + t + ", " +
            "state: " + t.getState() + ", " +
            "stack:\n\t" + Arrays.stream(t.getStackTrace()).map(Object::toString)
                .collect(Collectors.joining("\t")) + "\n");
      }
      Runtime.getRuntime().halt(1); // signal failure to caller
    }
  }
}
