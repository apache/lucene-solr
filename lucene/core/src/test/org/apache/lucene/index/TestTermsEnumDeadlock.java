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
package org.apache.lucene.index;

import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class TestTermsEnumDeadlock extends Assert {
  private static final int MAX_TIME_SECONDS = 15;

  @Test
  public void testDeadlock() throws Exception {
    for (int i = 0; i < 20; i++) {
      // Fork a separate JVM to reinitialize classes.
      final Process p =
              new ProcessBuilder(
                      Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                      "-cp",
                      System.getProperty("java.class.path"),
                      getClass().getName())
                      .inheritIO()
                      .start();
      long waitingTime = MAX_TIME_SECONDS * 2L;
      if (p.waitFor(waitingTime, TimeUnit.SECONDS)) {
        assertEquals("Process died abnormally?", 0, p.waitFor());
      } else {
        p.destroyForcibly().waitFor();
        fail("Process did not exit after " + waitingTime + " secs?");
      }
    }
  }

  // This method is called in a spawned process.
  public static void main(final String... args) throws Exception {
    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    if (runtimeMXBean == null) return;
    String processInfo = runtimeMXBean.getName();
    if (processInfo == null) return;
    String pid = processInfo.split("@")[0];
    if (pid.isEmpty()) return;
    System.out.println("PID : " + pid);
    int delayMillis = ThreadLocalRandom.current().nextInt(100, 200);
    System.out.println("delayMillis : " + delayMillis);

    new Thread(() -> {
      System.out.println(Thread.currentThread() + " Sleeping before TermsEnum init : " + delayMillis + " ms");
      try {
        Thread.sleep(delayMillis);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.out.println(Thread.currentThread() + " TermsEnum Created : " + TermsEnum.EMPTY);
    }).start();

    try {
      Thread.sleep(delayMillis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println(Thread.currentThread() + " DummyTermsEnum Creating");
    System.out.println(Thread.currentThread() + " DummyTermsEnum Created: " + new DummyTermsEnum());
    System.out.println();
  }

  private static class DummyTermsEnum extends BaseTermsEnum {

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      return null;
    }

    @Override
    public void seekExact(long ord) throws IOException {
    }

    @Override
    public BytesRef term() throws IOException {
      return null;
    }

    @Override
    public long ord() throws IOException {
      return 0;
    }

    @Override
    public int docFreq() throws IOException {
      return 0;
    }

    @Override
    public long totalTermFreq() throws IOException {
      return 0;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      return null;
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      return null;
    }

    @Override
    public BytesRef next() throws IOException {
      return null;
    }
  }
}
