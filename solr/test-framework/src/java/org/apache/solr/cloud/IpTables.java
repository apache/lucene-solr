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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To use, tests must be able to run iptables, eg sudo chmod u+s iptables
 */
public class IpTables {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final boolean ENABLED = Boolean.getBoolean("solr.tests.use.iptables");
  
  private static final Set<Integer> BLOCK_PORTS = Collections.synchronizedSet(new HashSet<Integer>());
  
  public static void blockPort(int port) throws IOException, InterruptedException {
    if (ENABLED) {
      log.info("Block port with iptables: {}", port);
      BLOCK_PORTS.add(port);
      runCmd(("iptables -A INPUT -p tcp --dport " + port + " -j DROP")
          .split("\\s"));
      runCmd(("iptables -A OUTPUT -p tcp --dport " + port + " -j DROP")
          .split("\\s"));
    }
  }
  
  public static void unblockPort(int port) throws IOException, InterruptedException {
    if (ENABLED && BLOCK_PORTS.contains(port)) {
      log.info("Unblock port with iptables: {}", port);
      runCmd(("iptables -D INPUT -p tcp --dport " + port + " -j DROP")
          .split("\\s"));
      runCmd(("iptables -D OUTPUT -p tcp --dport " + port + " -j DROP")
          .split("\\s"));
      BLOCK_PORTS.remove(port);
    }
  }
  
  public static void unblockAllPorts() throws IOException, InterruptedException {
    if (ENABLED) {
      log.info("Unblocking any ports previously blocked with iptables...");
      final Integer[] ports = BLOCK_PORTS.toArray(new Integer[BLOCK_PORTS.size()]);
      for (Integer port : ports) {
        IpTables.unblockPort(port);
      }
    }
  }
  
  private static void runCmd(String... cmd) throws IOException, InterruptedException {
    final int exitCode = new ProcessBuilder(cmd).inheritIO().start().waitFor();
    if (exitCode != 0) {
      throw new IOException("iptables process did not exit successfully, exit code was: " + exitCode);
    }
  }
}
