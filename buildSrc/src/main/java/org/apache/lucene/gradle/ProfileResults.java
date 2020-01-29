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

package org.apache.lucene.gradle;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import jdk.jfr.consumer.RecordedClass;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;

/**
 * Processes an array of recording files (from tests), and prints a simple histogram.
 * Inspired by the JFR example code.
 * Whole stacks are deduplicated (with the default stacksize being 1): you can drill deeper
 * by adjusting the parameters.
 */
public class ProfileResults {
  /**
   * Formats a frame to a formatted line. This is deduplicated on!
   */
  static String frameToString(RecordedFrame frame, boolean lineNumbers) {
    StringBuilder builder = new StringBuilder();
    RecordedMethod method = frame.getMethod();
    RecordedClass clazz = method.getType();
    if (clazz == null) {
      builder.append("<<");
      builder.append(frame.getType());
      builder.append(">>");
    } else {
      builder.append(clazz.getName());
    }
    builder.append("#");
    builder.append(method.getName());
    builder.append("()");
    if (lineNumbers) {
      builder.append(":");
      if (frame.getLineNumber() == -1) {
        builder.append("(" + frame.getType() + " code)");
      } else {
        builder.append(frame.getLineNumber());
      }
    }
    return builder.toString();
  }

  /*
   * Making the java code wordy to work around gradle's broken property design, ugh!
   */
  public static final String STACKSIZE_KEY = "tests.profile.stacksize";
  public static final String STACKSIZE_DEFAULT = "1";
  public static final String COUNT_KEY = "tests.profile.count";
  public static final String COUNT_DEFAULT = "10";
  public static final String LINENUMBERS_KEY = "tests.profile.linenumbers";
  public static final String LINENUMBERS_DEFAULT = "false";

  /**
   * Driver method, for testing standalone.
   * <pre>
   * java -Dtests.profile.count=5 buildSrc/src/main/java/org/apache/lucene/gradle/ProfileResults.java \
   *   ./lucene/core/build/tmp/tests-cwd/somefile.jfr ...
   * </pre>
   */
  public static void main(String[] args) throws IOException {
    printReport(Arrays.asList(args),
                Integer.parseInt(System.getProperty(STACKSIZE_KEY, STACKSIZE_DEFAULT)),
                Integer.parseInt(System.getProperty(COUNT_KEY, COUNT_DEFAULT)),
                Boolean.parseBoolean(System.getProperty(LINENUMBERS_KEY, LINENUMBERS_DEFAULT)));
  }

  /**
   * Process all the JFR files passed in args and print a merged summary.
   */
  public static void printReport(List<String> files, int stacksize, int count, boolean lineNumbers) throws IOException {
    if (stacksize < 1) {
      throw new IllegalArgumentException("tests.profile.stacksize must be positive");
    }
    if (count < 1) {
      throw new IllegalArgumentException("tests.profile.count must be positive");
    }
    Map<String, SimpleEntry<String, Integer>> histogram = new HashMap<>();
    int total = 0;
    for (String file : files) {
      try (RecordingFile recording = new RecordingFile(Paths.get(file))) {
        while (recording.hasMoreEvents()) {
          RecordedEvent event = recording.readEvent();
          RecordedThread thread = event.getThread("sampledThread");
          // ignore gradle's epoll loop in the worker thread
          if (thread != null && thread.getJavaName().startsWith("/127.0.0.1")) {
            continue;
          }
          // process java and native method samples
          if (event.getEventType().getName().equals("jdk.ExecutionSample") ||
              event.getEventType().getName().equals("jdk.NativeMethodSample")) {
            RecordedStackTrace trace = event.getStackTrace();
            if (trace != null) {
              StringBuilder stack = new StringBuilder();
              for (int i = 0; i < Math.min(stacksize, trace.getFrames().size()); i++) {
                if (stack.length() > 0) {
                  stack.append("\n\t\t\t  at ");
                }
                stack.append(frameToString(trace.getFrames().get(i), lineNumbers));
              }
              String line = stack.toString();
              SimpleEntry<String,Integer> entry = histogram.computeIfAbsent(line, u -> new SimpleEntry<String, Integer>(line, 0));
              entry.setValue(entry.getValue() + 1);
              total++;
            }
          }
        }
      }
    }
    // print summary from histogram
    System.out.printf(Locale.ROOT, "PROFILE SUMMARY from %d samples\n", total);
    System.out.printf(Locale.ROOT, "  tests.profile.count=%d\n", count);
    System.out.printf(Locale.ROOT, "  tests.profile.stacksize=%d\n",  stacksize);
    System.out.printf(Locale.ROOT, "  tests.profile.linenumbers=%b\n",  lineNumbers);
    System.out.printf(Locale.ROOT, "PERCENT\tSAMPLES\tSTACK\n", total);
    List<SimpleEntry<String, Integer>> entries = new ArrayList<>(histogram.values());
    entries.sort((u, v) -> v.getValue().compareTo(u.getValue()));
    int seen = 0;
    for (SimpleEntry<String, Integer> c : entries) {
      if (seen++ == count) {
        break;
      }
      System.out.printf(Locale.ROOT, "%2.2f%%\t%d\t%s\n", 100 * (float) c.getValue() / total, c.getValue(), c.getKey());
    }
  }
}
