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

package org.apache.lucene.replicator.nrt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/** A pipe thread. It'd be nice to reuse guava's implementation for this... */
class ThreadPumper {
  public static Thread start(final Runnable onExit, final BufferedReader from, final PrintStream to, final Writer toFile, final AtomicBoolean nodeClosing) {
    Thread t = new Thread() {
        @Override
        public void run() {
          try {
            long startTimeNS = System.nanoTime();
            Pattern logTimeStart = Pattern.compile("^[0-9\\.]+s .*");
            String line;
            while ((line = from.readLine()) != null) {
              if (toFile != null) {
                toFile.write(line);
                toFile.write("\n");
                toFile.flush();
              } else if (logTimeStart.matcher(line).matches()) {
                // Already a well-formed log output:
                System.out.println(line);
              } else {
                TestStressNRTReplication.message(line, startTimeNS);
              }
              if (line.contains("now force close server socket after")) {
                nodeClosing.set(true);
              }
            }
            // Sub-process finished
          } catch (IOException e) {
            System.err.println("ignore IOExc reading from forked process pipe: " + e);
          } finally {
            onExit.run();
          }
        }
      };
    t.start();
    return t;
  }
}
