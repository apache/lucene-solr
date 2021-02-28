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
package org.apache.solr.common;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.logging.MDCLoggingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SkyHook {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getName());
  public static final int STACK = 10;
  public static SkyHook skyHookDoc = null;//new SkyHook();
  private static AtomicInteger cnt = new AtomicInteger();

  public final static Pattern FIELD_ID = Pattern.compile("\\<id\\:(\\d+)\\>");

  public void register(SolrDocumentBase document) {
    String sId;
    try {
      Object id = document.getFieldValue("id");

      if (id instanceof SolrInputField) {
        Object val = ((SolrInputField) id).getValue();
        sId = String.valueOf(val);
      } else {
        sId = String.valueOf(id);
      }

      if (sId.contains("=") || sId.contains(",") || sId.contains(":")) {
        throw new IllegalArgumentException(sId);
      }

    } catch (Throwable t) {
      log.error("SkyHook Exception (BAD!)", t);
      return;
    }

    register(sId, "");
  }

  private void reg(String sId, String line) {

    if (Integer.parseInt(sId) > 400) {
      log.info("found high one {}", sId);
    }

    log.info(cnt.incrementAndGet() +  " docid=" + sId + " " + line);

   // log.info("SkyHook add Send id={} map={}", sId, map.size());
  }

  public void register(String sId) {
    register(sId, "");
  }

  public void register(String sId, String additional) {
    try {
      Matcher m = FIELD_ID.matcher(sId);

      if (m.find()) {
        sId = m.group(1);
      }

      StringBuilder sb = ObjectReleaseTracker.getThreadLocalStringBuilder();
      sb.setLength(0);
      StringBuilderWriter sw = new StringBuilderWriter(sb);
      PrintWriter pw = new PrintWriter(sw);
      printStackTrace(new SolrException(SolrException.ErrorCode.UNKNOWN, ""), pw, STACK);
      String stack = sw.toString();
      String line = System.currentTimeMillis() + " registered on node=" + MDCLoggingContext.getNodeName() + (additional != null && additional.length() > 0 ?
          " " + additional :
          "") + " originStack=" + stack;

      reg(sId, line);
    } catch (Throwable t) {
      log.error("SkyHook Exception (BAD!?) " + additional, t);
    }
  }

//  public void logAll() {
//    try {
//      log.info("SkyHookOutput");
//      synchronized (map) {
//        map.forEach((id, deque) -> {
//
//          log.info("⤞⤞⤞⤞⤞⤞⤠⤠⤠⤠⤠⤠⤠⤗⤗⤗⤗⤗");
//          log.info("⤞⤞⤞⤞⤞⤞⤠⤠⤠⤠⤠⤠⤠⤗⤗⤗⤗⤗ docid={}", id);
//
//          deque.forEach(line -> {
//            log.info("docid={} {}", id, line);
//          });
//
//        });
//      }
//      log.info("SkyHookOutput done");
//    } catch (Throwable t) {
//      log.error("SkyHook exception", t);
//      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, t);
//    }
//  }

  private static void printStackTrace(Throwable t, PrintWriter p, int stack) {
    // Print our stack trace
    StackTraceElement[] trace = t.getStackTrace();
    for (int i = 2; i < Math.min(stack, trace.length); i++) {
      StackTraceElement traceElement = trace[i];
      p.println(" " + traceElement);
    }
  }

}
