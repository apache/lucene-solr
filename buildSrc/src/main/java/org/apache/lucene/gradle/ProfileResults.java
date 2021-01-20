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
  public static final String MODE_KEY = "tests.profile.mode";
  public static final String MODE_DEFAULT = "cpu";
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
                System.getProperty(MODE_KEY, MODE_DEFAULT),
                Integer.parseInt(System.getProperty(STACKSIZE_KEY, STACKSIZE_DEFAULT)),
                Integer.parseInt(System.getProperty(COUNT_KEY, COUNT_DEFAULT)),
                Boolean.parseBoolean(System.getProperty(LINENUMBERS_KEY, LINENUMBERS_DEFAULT)));
  }

  /** true if we care about this event */
  static boolean isInteresting(String mode, RecordedEvent event) {
    String name = event.getEventType().getName();
    switch(mode) {
      case "cpu":
        return (name.equals("jdk.ExecutionSample") || name.equals("jdk.NativeMethodSample")) &&
            !isGradlePollThread(event.getThread("sampledThread"));
      case "heap":
        return (name.equals("jdk.ObjectAllocationInNewTLAB") || name.equals("jdk.ObjectAllocationOutsideTLAB")) &&
            !isGradlePollThread(event.getThread("eventThread"));
      default:
        throw new UnsupportedOperationException(event.toString());
    }
  }

  /** true if the thread is gradle epoll thread (which we don't care about) */
  static boolean isGradlePollThread(RecordedThread thread) {
    return (thread != null && thread.getJavaName().startsWith("/127.0.0.1"));
  }

  /** value we accumulate for this event */
  static long getValue(RecordedEvent event) {
    switch(event.getEventType().getName()) {
      case "jdk.ObjectAllocationInNewTLAB":
        return event.getLong("tlabSize");
      case "jdk.ObjectAllocationOutsideTLAB":
        return event.getLong("allocationSize");
      case "jdk.ExecutionSample":
        return 1L;
      case "jdk.NativeMethodSample":
        return 1L;
      default:
        throw new UnsupportedOperationException(event.toString());
    }
  }

  /** format a value, if its huge, we show millions */
  static String formatValue(long value) {
    if (value > 1_000_000) {
      return String.format("%dM", value / 1_000_000);
    } else {
      return Long.toString(value);
    }
  }

  /** fixed width used for printing the different columns */
  private static final int COLUMN_SIZE = 14;
  private static final String COLUMN_PAD = "%-" + COLUMN_SIZE + "s";
  private static String pad(String input) {
    return String.format(Locale.ROOT, COLUMN_PAD, input);
  }

  /**
   * Process all the JFR files passed in args and print a merged summary.
   */
  public static void printReport(List<String> files, String mode, int stacksize, int count, boolean lineNumbers) throws IOException {
    if (!"cpu".equals(mode) && !"heap".equals(mode)) {
      throw new IllegalArgumentException("tests.profile.mode must be one of (cpu,heap)");
    }
    if (stacksize < 1) {
      throw new IllegalArgumentException("tests.profile.stacksize must be positive");
    }
    if (count < 1) {
      throw new IllegalArgumentException("tests.profile.count must be positive");
    }
    Map<String, SimpleEntry<String, Long>> histogram = new HashMap<>();
    int totalEvents = 0;
    long sumValues = 0;
    String framePadding = " ".repeat(COLUMN_SIZE * 2);
    for (String file : files) {
      try (RecordingFile recording = new RecordingFile(Paths.get(file))) {
        while (recording.hasMoreEvents()) {
          RecordedEvent event = recording.readEvent();
          if (!isInteresting(mode, event)) {
            continue;
          }
          RecordedStackTrace trace = event.getStackTrace();
          if (trace != null) {
            StringBuilder stack = new StringBuilder();
            for (int i = 0; i < Math.min(stacksize, trace.getFrames().size()); i++) {
              if (stack.length() > 0) {
                stack.append("\n")
                     .append(framePadding)
                     .append("  at ");
              }
              stack.append(frameToString(trace.getFrames().get(i), lineNumbers));
            }
            String line = stack.toString();
            SimpleEntry<String,Long> entry = histogram.computeIfAbsent(line, u -> new SimpleEntry<String, Long>(line, 0L));
            long value = getValue(event);
            entry.setValue(entry.getValue() + value);
            totalEvents++;
            sumValues += value;
          }
        }
      }
    }
    // print summary from histogram
    System.out.printf(Locale.ROOT, "PROFILE SUMMARY from %d events (total: %s)\n", totalEvents, formatValue(sumValues));
    System.out.printf(Locale.ROOT, "  tests.profile.mode=%s\n", mode);
    System.out.printf(Locale.ROOT, "  tests.profile.count=%d\n", count);
    System.out.printf(Locale.ROOT, "  tests.profile.stacksize=%d\n", stacksize);
    System.out.printf(Locale.ROOT, "  tests.profile.linenumbers=%b\n", lineNumbers);
    System.out.printf(Locale.ROOT, "%s%sSTACK\n", pad("PERCENT"), pad(mode.toUpperCase(Locale.ROOT) + " SAMPLES"));
    List<SimpleEntry<String, Long>> entries = new ArrayList<>(histogram.values());
    entries.sort((u, v) -> v.getValue().compareTo(u.getValue()));
    int seen = 0;
    for (SimpleEntry<String, Long> c : entries) {
      if (seen++ == count) {
        break;
      }
      String percent = String.format("%2.2f%%", 100 * (c.getValue() / (float) sumValues));
      System.out.printf(Locale.ROOT, "%s%s%s\n", pad(percent), pad(formatValue(c.getValue())), c.getKey());
    }
  }
}
