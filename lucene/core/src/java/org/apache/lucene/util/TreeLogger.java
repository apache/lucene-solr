package org.apache.lucene.util;

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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TreeLogger {
  
  private final List<String> stack = new ArrayList<String>();
  private String indent = "";
  private final String label;

  private static Map<Thread,TreeLogger> loggers = new ConcurrentHashMap<Thread,TreeLogger>();

  public static void setLogger(TreeLogger instance) {
    loggers.put(Thread.currentThread(), instance);
  }

  public static TreeLogger getLogger() {
    return loggers.get(Thread.currentThread());
  }

  public static void start(String label) {
    loggers.get(Thread.currentThread()).start0(label);
  }

  public static void end(String label) {
    loggers.get(Thread.currentThread()).end0(label);
  }

  public static void log(String message) {
    loggers.get(Thread.currentThread()).log0(message);
  }

  public static void log(String message, Throwable t) {
    loggers.get(Thread.currentThread()).log0(message, t);
  }

  public TreeLogger(String label) {
    this.label = label;
  }

  public void start0(String label) {
    stack.add(label);
    indent += "  ";
  }

  public void end0(String label) {
    if (stack.isEmpty()) {
      throw new IllegalStateException(Thread.currentThread().getName() + ": cannot end: label=" + label + " wasn't pushed");
    }
    String last = stack.get(stack.size()-1);
    if (last.equals(label) == false) {
      throw new IllegalStateException(Thread.currentThread().getName() + ": cannot end: label=" + label + " doesn't match current label=" + last);
    }
    stack.remove(stack.size()-1);
    indent = indent.substring(0, indent.length()-2);
  }

  public void log0(String message, Throwable t) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    t.printStackTrace(pw);
    log(message + ":\n" + sw.toString());
  }

  public void log0(String message) {
    StringBuilder b = new StringBuilder();
    while (message.startsWith("\n")) {
      b.append('\n');
      message = message.substring(1);
    }
    b.append('[');
    b.append(label);
    b.append("] ");
    b.append(indent);
    b.append("- ");
    b.append(message.replace("\n", "\n" + indent + "  "));
    System.out.println(b.toString());
  }
}
