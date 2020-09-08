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
package org.apache.lucene.store;

import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.TestRuleLimitSysouts;

@TestRuleLimitSysouts.Limit(bytes = 512_000, hardLimit = TestRuleLimitSysouts.DEFAULT_HARD_LIMIT)
public class TestLockFactoriesMultiJVM extends LuceneTestCase {
  
  private static final ProcessBuilder applyRedirection(ProcessBuilder pb, int client, Path dir) {
    if (LuceneTestCase.VERBOSE) {
      return pb.inheritIO();
    } else {
      return pb
        .redirectError(dir.resolve("err-" + client + ".txt").toFile())
        .redirectOutput(dir.resolve("out-" + client + ".txt").toFile())
        .redirectInput(Redirect.INHERIT);
    }
  }
  
  private void runImpl(Class<? extends LockFactory> impl) throws Exception {
    // make sure we are in clean state:
    LockVerifyServer.PORT.set(-1);
    
    final int clients = 2;
    final String host = "127.0.0.1";
    final int delay = 1;
    final int rounds = (LuceneTestCase.TEST_NIGHTLY ? 30000 : 500) * LuceneTestCase.RANDOM_MULTIPLIER;
    
    final Path dir = LuceneTestCase.createTempDir(impl.getSimpleName());
    
    // create the LockVerifyServer in a separate thread
    final ExecutorService pool = Executors.newSingleThreadExecutor(new NamedThreadFactory("lockfactory-tester-"));
    try {
      pool.submit(() -> {
        LockVerifyServer.main(host, Integer.toString(clients));
        return (Void) null;
      });
      
      // wait for it to boot up
      int port;
      while ((port = LockVerifyServer.PORT.get()) == -1) {
        Thread.sleep(100L);
      }
      
      // spawn clients as separate Java processes
      final List<Process> processes = new ArrayList<>();
      for (int i = 0; i < clients; i++) {
        processes.add(applyRedirection(new ProcessBuilder(
            Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp",
            System.getProperty("java.class.path"),
            LockStressTest.class.getName(),
            Integer.toString(i),
            host,
            Integer.toString(port),
            impl.getName(),
            dir.toString(),
            Integer.toString(delay),
            Integer.toString(rounds)
          ), i, dir).start());
      }
      
      // wait for all processes to exit
      int exited = 0;
      while (exited < clients) {
        for (Process p : processes) {
          if (p.waitFor(1, TimeUnit.SECONDS)) {
            exited++;
            assertEquals("Process died abnormally?", 0, p.waitFor());
          }
        }
      }
    } finally {
      // shutdown threadpool
      pool.shutdown();
      assertTrue("LockVerifyServer did not exit after 20s.",
          pool.awaitTermination(20, TimeUnit.SECONDS));
    }
  }
  
  public void testNativeFSLockFactory() throws Exception {
    runImpl(NativeFSLockFactory.class);
  }

  public void testSimpleFSLockFactory() throws Exception {
    runImpl(SimpleFSLockFactory.class);
  }

}
