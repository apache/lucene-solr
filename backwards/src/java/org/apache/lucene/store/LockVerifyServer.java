package org.apache.lucene.store;

/**
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

import java.net.ServerSocket;
import java.net.Socket;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;

/**
 * Simple standalone server that must be running when you
 * use {@link VerifyingLockFactory}.  This server simply
 * verifies at most one process holds the lock at a time.
 * Run without any args to see usage.
 *
 * @see VerifyingLockFactory
 * @see LockStressTest
 */

public class LockVerifyServer {

  private static String getTime(long startTime) {
    return "[" + ((System.currentTimeMillis()-startTime)/1000) + "s] ";
  }

  public static void main(String[] args) throws IOException {

    if (args.length != 1) {
      System.out.println("\nUsage: java org.apache.lucene.store.LockVerifyServer port\n");
      System.exit(1);
    }

    final int port = Integer.parseInt(args[0]);

    ServerSocket s = new ServerSocket(port);
    s.setReuseAddress(true);
    System.out.println("\nReady on port " + port + "...");

    int lockedID = 0;
    long startTime = System.currentTimeMillis();

    while(true) {
      Socket cs = s.accept();
      OutputStream out = cs.getOutputStream();
      InputStream in = cs.getInputStream();

      int id = in.read();
      int command = in.read();

      boolean err = false;

      if (command == 1) {
        // Locked
        if (lockedID != 0) {
          err = true;
          System.out.println(getTime(startTime) + " ERROR: id " + id + " got lock, but " + lockedID + " already holds the lock");
        }
        lockedID = id;
      } else if (command == 0) {
        if (lockedID != id) {
          err = true;
          System.out.println(getTime(startTime) + " ERROR: id " + id + " released the lock, but " + lockedID + " is the one holding the lock");
        }
        lockedID = 0;
      } else
        throw new RuntimeException("unrecognized command " + command);

      System.out.print(".");

      if (err)
        out.write(1);
      else
        out.write(0);

      out.close();
      in.close();
      cs.close();
    }
  }
}
