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

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SuppressForbidden;

@LuceneTestCase.SuppressFileSystems("*")
public class TestStressLockFactories extends LuceneTestCase {
  
  @SuppressForbidden(reason = "ProcessBuilder only allows to redirect to java.io.File")
  private static final ProcessBuilder applyRedirection(ProcessBuilder pb, int client, Path dir) {
    if (VERBOSE) {
      return pb.inheritIO();
    } else {
      return pb
        .redirectError(dir.resolve("err-" + client + ".txt").toFile())
        .redirectOutput(dir.resolve("out-" + client + ".txt").toFile())
        .redirectInput(Redirect.INHERIT);
    }
  }
  
  private void runImpl(Class<? extends LockFactory> impl) throws Exception {
    final int clients = TEST_NIGHTLY ? 5 : 2;
    final String host = "127.0.0.1";
    final int delay = 1;
    final int rounds = (TEST_NIGHTLY ? 30000 : 500) * RANDOM_MULTIPLIER;
    
    final Path dir = createTempDir(impl.getSimpleName());
    
    final List<Process> processes = new ArrayList<>(clients);

    LockVerifyServer.run(host, clients, addr -> {
      // spawn clients as separate Java processes
      for (int i = 0; i < clients; i++) {
        try {
          processes.add(applyRedirection(new ProcessBuilder(
              Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
              "-Xmx32M",
              "-cp",
              System.getProperty("java.class.path"),
              LockStressTest.class.getName(),
              Integer.toString(i),
              addr.getHostString(),
              Integer.toString(addr.getPort()),
              impl.getName(),
              dir.toString(),
              Integer.toString(delay),
              Integer.toString(rounds)
            ), i, dir).start());
        } catch (IOException ioe) {
          throw new AssertionError("Failed to start child process.", ioe);
        }
      }
    });
     
    // wait for all processes to exit...
    try {
      for (Process p : processes) {
        if (p.waitFor(15, TimeUnit.SECONDS)) {
          assertEquals("Process died abnormally?", 0, p.waitFor());
        }
      }
    } finally {
      // kill all processes, which are still alive.
      for (Process p : processes) {
        if (p.isAlive()) {
          p.destroyForcibly().waitFor();
        }
      }
    }
  }
  
  public void testNativeFSLockFactory() throws Exception {
    runImpl(NativeFSLockFactory.class);
  }

  public void testSimpleFSLockFactory() throws Exception {
    runImpl(SimpleFSLockFactory.class);
  }

}
