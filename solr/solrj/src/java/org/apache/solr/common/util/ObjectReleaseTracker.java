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
package org.apache.solr.common.util;

import org.apache.commons.io.output.StringBuilderWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class ObjectReleaseTracker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static Map<Object,ObjectTrackerException> OBJECTS = new ConcurrentHashMap<>(64, 0.75f, 1);


  protected final static ThreadLocal<StringBuilder> THREAD_LOCAL_SB = new ThreadLocal<>();

  public static StringBuilder getThreadLocalStringBuilder() {
    StringBuilder sw = THREAD_LOCAL_SB.get();
    if (sw == null) {
      sw = new StringBuilder(1024);
      THREAD_LOCAL_SB.set(sw);
    }
    return sw;
  }

  public static boolean track(Object object) {
    ObjectTrackerException ote = new ObjectTrackerException(object.getClass().getName());
    OBJECTS.put(object, ote);
    return true;
  }
  
  public static boolean release(Object object) {
    OBJECTS.remove(object);
    return true;
  }
  
  public static void clear() {
    OBJECTS.clear();
  }

  /**
   * @return null if ok else error message
   */
  public static String checkEmpty() {
    return checkEmpty(null);
  }

  /**
   * @return null if ok else error message
   * @param object tmp feature allowing to ignore and close an object
   */
  public static String checkEmpty(String object) {
   // if (true) return null; // MRM TODO:
    StringBuilder error = new StringBuilder();
    HashMap<Object,ObjectTrackerException> entries = new HashMap<>(OBJECTS);

    if (entries.size() > 0) {
      List<String> objects = new ArrayList<>(entries.size());
      for (Entry<Object,ObjectTrackerException> entry : entries.entrySet()) {
        if (object != null && entry.getKey().getClass().getSimpleName().equals(object)) {
          entries.remove(entry.getKey());
          if (entry.getKey() instanceof Closeable) {
            try {
              ((Closeable) entry.getKey()).close();
            } catch (IOException e) {
              log.warn("Exception trying to close", e);
            }
          }
          continue;
        }
        objects.add(entry.getKey().getClass().getSimpleName());
      }
      if (objects.isEmpty()) {
        return null;
      }
      error.append("ObjectTracker found " + objects.size() + " object(s) that were not released!!! " + objects + "\n");
      for (Entry<Object,ObjectTrackerException> entry : entries.entrySet()) {
        StringBuilder sb = getThreadLocalStringBuilder();
        sb.setLength(0);
        StringBuilderWriter sw = new StringBuilderWriter(sb);
        PrintWriter pw = new PrintWriter(sw);
        ObjectTrackerException ote = entry.getValue();
        printStackTrace(ote, pw, 8);
        String stack = object + "\n" + sw.toString().replaceAll("org\\.apache\\.solr", "o.a.s");
        error.append(entry.getKey() + "\n" + "StackTrace:\n" + stack + "\n");
      }
    }
    if (error.length() == 0) {
      return null;
    }
    return error.toString();
  }

  private static void printStackTrace(Throwable t, PrintWriter p, int stack) {
    // Print our stack trace
    p.println(t);
    StackTraceElement[] trace = t.getStackTrace();
    for (int i = 0; i < stack; i++) {
      StackTraceElement traceElement = trace[i];
      p.println("at " + traceElement);
    }
  }

  public static class ObjectTrackerException extends RuntimeException {
    public ObjectTrackerException(String msg) {
      super(msg);
    }

    public ObjectTrackerException(Throwable t) {
      super(t);
    }
  }

}
