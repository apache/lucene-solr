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

package org.apache.solr.servlet;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrRequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeOutPatrol implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final long TIMEOUT = 2 * 60 * 1000; // 2 minutes
  private final boolean enabled;

  private ScheduledExecutorService executorService;

  public TimeOutPatrol(CoreContainer cc) {
    if (Boolean.getBoolean("timeout.patrol")) {
      enabled=true;
      executorService = Executors.newSingleThreadScheduledExecutor();
      executorService.scheduleAtFixedRate(
          printLongRunningTasks(cc), 0, 60, TimeUnit.SECONDS);

    } else {
      enabled= false;
      executorService = null;
    }
  }

  private Runnable printLongRunningTasks(CoreContainer cc) {
    return () -> {
      
      if (cc.isShutDown()) return;
      try {
        Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();

        for (Thread t: threads.keySet()) {
          if (t.getName().startsWith("qtp") || t.getName().startsWith("httpShardExecutor") || t.getName().startsWith("searcherExecutor")) {
            SolrRequestInfo info = getSolrRequestInfo(t);
            if (info != null) {
              long timeElapsed = new Date().getTime() - info.getNOW().getTime();
              
              long timeout = info.getReq().getParams().getLong("timeAllowed", TIMEOUT);
              if (timeElapsed > timeout) {
                log.info("Long running query: thread={}, state={}, start={}, timeElapsed={}, request={}", 
                    t.getName(), t.getState().name(), info.getNOW(), timeElapsed, info.getReq());
                log.error("Stack trace for " + t.getName() + ": {}", Arrays.deepToString(threads.get(t)));
              }
            }
          }
        }

      } catch (Exception e) {
        log.error("Error reading threads ", e);
      }
    };
  }

  @Override
  public void close() throws IOException {
    if (executorService != null) {
      executorService.shutdownNow();
      executorService = null;
    }
  }

  // Reference: https://stackoverflow.com/a/32231177
  private SolrRequestInfo getSolrRequestInfo(Thread thread) {
    SolrRequestInfo info = null;
    try {
      Field field;
      field = Thread.class.getDeclaredField("threadLocals");
      field.setAccessible(true);
      Object map = field.get(thread);

      if (map != null) {
        Field table = Class.forName("java.lang.ThreadLocal$ThreadLocalMap").getDeclaredField("table");
        table.setAccessible(true);
        Object tbl = table.get(map);
        int length = Array.getLength(tbl);

        for (int i = 0; i < length; i++) {
          Object entry = Array.get(tbl, i);
          Object value = null;
          if (entry != null) {
            Field valueField = Class.forName("java.lang.ThreadLocal$ThreadLocalMap$Entry").getDeclaredField("value");
            valueField.setAccessible(true);
            value = valueField.get(entry);

            if (value != null && log.isDebugEnabled()) log.debug("\t[" + i + "] type[" + value.getClass() + "] " + value);

            if (value != null && value instanceof LinkedList && ((LinkedList) value).size() != 0
                && ((LinkedList) value).get(0) instanceof SolrRequestInfo) {
              info = (SolrRequestInfo) ((LinkedList) value).get(0);
              break;
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Couldn't get threadlocal for thread " + thread + ": ", e);
    }
    return info;
  }

}
