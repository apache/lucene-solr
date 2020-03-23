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

package org.apache.solr.bootstrap;

import org.eclipse.jetty.start.StartLog;

/**
 * Main class that will delegate to Jetty's Main class after doing some bootstrap actions.
 * Everything that needs to be done before the Jetty application starts can go here.
 */
public class SolrBootstrap {
  static {
    System.setProperty("jna.tmpdir", System.getProperty("solr.solr.home"));
  }

  public SolrBootstrap() {
    StartLog.info("Starting Solr...");
  }

  public static void main(String[] args) {
    SolrBootstrap solrBootstrap = new SolrBootstrap();
    solrBootstrap.memLockMaybe();
    org.eclipse.jetty.start.Main.main(args);
  }

  private void memLockMaybe() {
    if (Boolean.getBoolean("solr.memory.lock")) {
      if (NativeLibrary.isAvailable()) {
        StartLog.info("Attempting to lock Solr's memory to prevent swapping...");
        NativeLibrary.tryMlockall();
      } else {
        StartLog.warn("JNA not available, cannot lock memory");
      }
    }
  }
}