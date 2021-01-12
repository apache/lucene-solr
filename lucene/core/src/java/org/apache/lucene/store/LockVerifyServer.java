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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SuppressForbidden;

/**
 * Simple standalone server that must be running when you use {@link VerifyingLockFactory}. This
 * server simply verifies at most one process holds the lock at a time. Run without any args to see
 * usage.
 *
 * @see VerifyingLockFactory
 * @see LockStressTest
 */
@SuppressForbidden(reason = "System.out required: command line tool")
public class LockVerifyServer {
  public static final int START_GUN_SIGNAL = 43;

  // method pkg-private for tests
  static void run(String hostname, int maxClients, Consumer<InetSocketAddress> startClients)
      throws Exception {
    try (final ServerSocket s = new ServerSocket()) {
      s.setReuseAddress(true);
      s.setSoTimeout(30000); // initially 30 secs to give clients enough time to startup
      s.bind(new InetSocketAddress(hostname, 0));
      final InetSocketAddress localAddr = (InetSocketAddress) s.getLocalSocketAddress();
      System.out.println("Listening on " + localAddr + "...");

      // callback only for the test to start the clients:
      startClients.accept(localAddr);

      final Object localLock = new Object();
      final int[] lockedID = new int[1];
      lockedID[0] = -1;
      final CountDownLatch startingGun = new CountDownLatch(1);
      final Thread[] threads = new Thread[maxClients];

      for (int count = 0; count < maxClients; count++) {
        final Socket cs = s.accept();
        threads[count] =
            new Thread() {
              @Override
              public void run() {
                try (InputStream in = cs.getInputStream();
                    OutputStream os = cs.getOutputStream()) {
                  final int id = in.read();
                  if (id < 0) {
                    throw new IOException("Client closed connection before communication started.");
                  }

                  startingGun.await();
                  os.write(START_GUN_SIGNAL);
                  os.flush();

                  while (true) {
                    final int command = in.read();
                    if (command < 0) {
                      return; // closed
                    }

                    synchronized (localLock) {
                      final int currentLock = lockedID[0];
                      if (currentLock == -2) {
                        return; // another thread got error, so we exit, too!
                      }
                      switch (command) {
                        case VerifyingLockFactory.MSG_LOCK_ACQUIRED:
                          // Locked
                          if (currentLock != -1) {
                            lockedID[0] = -2;
                            throw new IllegalStateException(
                                "id "
                                    + id
                                    + " got lock, but "
                                    + currentLock
                                    + " already holds the lock");
                          }
                          lockedID[0] = id;
                          break;
                        case VerifyingLockFactory.MSG_LOCK_RELEASED:
                          // Unlocked
                          if (currentLock != id) {
                            lockedID[0] = -2;
                            throw new IllegalStateException(
                                "id "
                                    + id
                                    + " released the lock, but "
                                    + currentLock
                                    + " is the one holding the lock");
                          }
                          lockedID[0] = -1;
                          break;
                        default:
                          throw new RuntimeException("Unrecognized command: " + command);
                      }
                      os.write(command);
                      os.flush();
                    }
                  }
                } catch (RuntimeException | Error e) {
                  throw e;
                } catch (Exception ioe) {
                  throw new RuntimeException(ioe);
                } finally {
                  IOUtils.closeWhileHandlingException(cs);
                }
              }
            };
        threads[count].start();
      }

      // start
      System.out.println("All clients started, fire gun...");
      startingGun.countDown();

      // wait for all threads to finish
      for (Thread t : threads) {
        t.join();
      }

      System.out.println("Server terminated.");
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: java org.apache.lucene.store.LockVerifyServer bindToIp clients\n");
      System.exit(1);
    }

    run(args[0], Integer.parseInt(args[1]), addr -> {});
  }
}
