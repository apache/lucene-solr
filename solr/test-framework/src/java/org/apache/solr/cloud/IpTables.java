package org.apache.solr.cloud;

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


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To use, tests must be able to run iptables, eg sudo chmod u+s iptables
 */
public class IpTables {
  static final Logger log = LoggerFactory
      .getLogger(IpTables.class);
  
  private static boolean ENABLED = Boolean.getBoolean("solr.tests.use.iptables");
  static class ThreadPumper {

    public ThreadPumper() {}
    
    public static Thread start(final InputStream from, final OutputStream to, final boolean verbose) {
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            byte [] buffer = new byte [1024];
            int len;
            while ((len = from.read(buffer)) != -1) {
              if (verbose) {
                to.write(buffer, 0, len);
              }
            }
          } catch (IOException e) {
            System.err.println("Couldn't pipe from the forked process: " + e.toString());
          }
        }
      };
      t.start();
      return t;
    }
  }
  
  private static Set<Integer> BLOCK_PORTS = Collections.synchronizedSet(new HashSet<Integer>());
  
  public static void blockPort(int port) throws IOException,
      InterruptedException {
    if (ENABLED) {
      log.info("Block port with iptables: " + port);
      BLOCK_PORTS.add(port);
      runCmd(("iptables -A INPUT -p tcp --dport " + port + " -j DROP")
          .split("\\s"));
      runCmd(("iptables -A OUTPUT -p tcp --dport " + port + " -j DROP")
          .split("\\s"));
    }
  }
  
  public static void unblockPort(int port) throws IOException,
      InterruptedException {
    if (ENABLED) {
      log.info("Unblock port with iptables: " + port);
      runCmd(("iptables -D INPUT -p tcp --dport " + port + " -j DROP")
          .split("\\s"));
      runCmd(("iptables -D OUTPUT -p tcp --dport " + port + " -j DROP")
          .split("\\s"));
    }
  }
  
  public static void unblockAllPorts() throws IOException, InterruptedException {
    if (ENABLED) {
      log.info("Unblocking any ports previously blocked with iptables...");
      for (Integer port : BLOCK_PORTS) {
        IpTables.unblockPort(port);
      }
    }
  }
  
  private static void runCmd(String[] cmd) throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(cmd);

    pb.redirectErrorStream(true);
    Process p = pb.start();

    // We pump everything to stderr.
    PrintStream childOut = System.err; 
    Thread stdoutPumper = ThreadPumper.start(p.getInputStream(), childOut, true);
    Thread stderrPumper = ThreadPumper.start(p.getErrorStream(), childOut, true);
    if (true) childOut.println(">>> Begin subprocess output");
    p.waitFor();
    stdoutPumper.join();
    stderrPumper.join();
    if (true) childOut.println("<<< End subprocess output");
  }
}
