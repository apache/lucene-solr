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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SuppressForbidden;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@LuceneTestCase.SuppressFileSystems("*")
public class TestStressLockFactories extends LuceneTestCase {

  private interface LockClient {
    void await();
    void cleanup();
  }

  private static class ForkedProcessClient implements LockClient {
    private final Process process;

    public ForkedProcessClient(Process process) {
      this.process = process;
    }

    @Override
    public void await() {
      try {
        if (process.waitFor(10, TimeUnit.SECONDS)) {
          int exitValue = process.exitValue();
          assertEquals("Process " + process.pid() + " exit status != 0: " + exitValue, 0, exitValue);
        } else {
          assertFalse("Timeout reached waiting for process " + process.pid() + " to terminate.", process.isAlive());
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void cleanup() {
      try {
        if (process.isAlive()) {
          process.destroyForcibly().waitFor();
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class LocalThreadClient implements LockClient {
    private final Thread t;

    public LocalThreadClient(Thread t) {
      this.t = t;
    }

    @Override
    public void await() {
      try {
        t.join(TimeUnit.SECONDS.toMillis(10));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void cleanup() {
      // best-effort to interrupt it.
      t.interrupt();
    }
  }

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

  private static LockClient forkedProcess(Class<? extends LockFactory> impl,
                                            int delay, int rounds, Path dir,
                                            InetSocketAddress addr, int id) {
    try {
      ProcessBuilder pb = new ProcessBuilder(
          Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
          "-Xmx32M",
          "-cp",
          System.getProperty("java.class.path"),
          LockStressTest.class.getName(),
          Integer.toString(id),
          addr.getHostString(),
          Integer.toString(addr.getPort()),
          impl.getName(),
          dir.toString(),
          Integer.toString(delay),
          Integer.toString(rounds)
      );
      applyRedirection(pb, id, dir);
      return new ForkedProcessClient(pb.start());
    } catch (IOException e) {
      throw new AssertionError("Failed to start a child process.", e);
    }
  }

  private static LockClient localThread(Class<? extends LockFactory> impl,
                                 int delay, int rounds, Path dir, InetSocketAddress addr, Integer id) {
    Thread t = new Thread(() -> {
      try {
        int exitCode = LockStressTest.run(
            id,
            addr.getHostString(),
            addr.getPort(),
            impl.getName(),
            dir,
            delay,
            rounds);
        assertEquals(0, exitCode);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }, "LockStressTester-" + id);
    t.start();

    return new LocalThreadClient(t);
  }
  
  private static LockClient newLockClient(Class<? extends LockFactory> impl,
      int delay, int rounds, Path dir,
      InetSocketAddress addr, int id) {
    return (id % 2 == 0)
        ? localThread(impl, delay, rounds, dir, addr, id)
        : forkedProcess(impl, delay, rounds, dir, addr, id);
  }

  private static void runImpl(Class<? extends LockFactory> impl) throws Exception {
    final InetAddress host = Inet4Address.getLoopbackAddress();
    final int delay = 1;
    final int rounds = (TEST_NIGHTLY ? 30000 : 500) * RANDOM_MULTIPLIER;

    final Path dir = createTempDir(impl.getSimpleName());

    final int clients = TEST_NIGHTLY ? 12 : 4;
    
    final List<LockClient> processes = new ArrayList<>(clients);
    try {
      LockVerifyServer.run(host, clients, addr -> {
        for (int id = 0; id < clients; id++) {
          processes.add(newLockClient(impl, delay, rounds, dir, addr, id));
        }
      });

      // Wait for all processes to exit...
      processes.forEach(LockClient::await);
    } finally {
      processes.forEach(LockClient::cleanup);
    }
  }

  
  public void testNativeFSLockFactory() throws Exception {
    runImpl(NativeFSLockFactory.class);
  }

  public void testSimpleFSLockFactory() throws Exception {
    runImpl(SimpleFSLockFactory.class);
  }

}
