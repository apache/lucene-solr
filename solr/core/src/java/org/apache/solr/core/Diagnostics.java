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
package org.apache.solr.core;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;

public class Diagnostics {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public interface Callable {
    public void call(Object... data);  // data depends on the context
  }

  public static void call(Callable callable, Object... data) {
    try {
      callable.call(data);
    } catch (Exception e) {
      log.error("TEST HOOK EXCEPTION", e);
    }
  }

  public static void logThreadDumps(String message) {
    StringBuilder sb = new StringBuilder(32768);
    if (message == null) message = "============ THREAD DUMP REQUESTED ============";
    sb.append(message);
    sb.append("\n");
    ThreadInfo[] threads = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
    for (ThreadInfo info : threads) {
      sb.append(info);
      // sb.append("\n");
    }
    log.error("{}", sb);
  }


}
