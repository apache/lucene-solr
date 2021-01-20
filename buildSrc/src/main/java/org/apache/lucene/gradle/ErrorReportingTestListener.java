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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.gradle.api.internal.tasks.testing.logging.FullExceptionFormatter;
import org.gradle.api.internal.tasks.testing.logging.TestExceptionFormatter;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestOutputEvent;
import org.gradle.api.tasks.testing.TestOutputListener;
import org.gradle.api.tasks.testing.TestResult;
import org.gradle.api.tasks.testing.logging.TestLogging;

/**
 * An error reporting listener that queues test output streams and displays them
 * on failure.
 * <p>
 * Heavily inspired by Elasticsearch's ErrorReportingTestListener (ASL 2.0 licensed).
 */
public class ErrorReportingTestListener implements TestOutputListener, TestListener {
   private static final Logger LOGGER = Logging.getLogger(ErrorReportingTestListener.class);

   private final TestExceptionFormatter formatter;
   private final Map<TestKey, OutputHandler> outputHandlers = new ConcurrentHashMap<>();
   private final Path spillDir;
   private final Path outputsDir;
   private final boolean verboseMode;

   public ErrorReportingTestListener(TestLogging testLogging, Path spillDir, Path outputsDir, boolean verboseMode) {
      this.formatter = new FullExceptionFormatter(testLogging);
      this.spillDir = spillDir;
      this.outputsDir = outputsDir;
      this.verboseMode = verboseMode;
   }

   @Override
   public void onOutput(TestDescriptor testDescriptor, TestOutputEvent outputEvent) {
      handlerFor(testDescriptor).write(outputEvent);
   }

   @Override
   public void beforeSuite(TestDescriptor suite) {
      // noop.
   }

   @Override
   public void beforeTest(TestDescriptor testDescriptor) {
      // Noop.
   }

   @Override
   public void afterSuite(final TestDescriptor suite, TestResult result) {
      if (suite.getParent() == null || suite.getName().startsWith("Gradle")) {
         return;
      }

      TestKey key = TestKey.of(suite);
      try {
         OutputHandler outputHandler = outputHandlers.get(key);
         if (outputHandler != null) {
            long length = outputHandler.length();
            if (length > 1024 * 1024 * 10) {
               LOGGER.warn(String.format(Locale.ROOT, "WARNING: Test %s wrote %,d bytes of output.",
                   suite.getName(),
                   length));
            }
         }

         boolean echoOutput = Objects.equals(result.getResultType(), TestResult.ResultType.FAILURE);
         boolean dumpOutput = echoOutput;

         // If the test suite failed, report output.
         if (dumpOutput || echoOutput) {
            Files.createDirectories(outputsDir);
            Path outputLog = outputsDir.resolve(getOutputLogName(suite));

            // Save the output of a failing test to disk.
            try (Writer w = Files.newBufferedWriter(outputLog, StandardCharsets.UTF_8)) {
               if (outputHandler != null) {
                  outputHandler.copyTo(w);
               }
            }

            if (echoOutput && !verboseMode) {
               synchronized (this) {
                  System.out.println("");
                  System.out.println(suite.getClassName() + " > test suite's output saved to " + outputLog + ", copied below:");
                  try (BufferedReader reader = Files.newBufferedReader(outputLog, StandardCharsets.UTF_8)) {
                     char[] buf = new char[1024];
                     int len;
                     while ((len = reader.read(buf)) >= 0) {
                        System.out.print(new String(buf, 0, len));
                     }
                     System.out.println();
                  }
               }
            }
         }
      } catch (IOException e) {
         throw new UncheckedIOException(e);
      } finally {
         OutputHandler handler = outputHandlers.remove(key);
         if (handler != null) {
            try {
               handler.close();
            } catch (IOException e) {
               LOGGER.error("Failed to close output handler for: " + key, e);
            }
         }
      }
   }

   private static Pattern SANITIZE = Pattern.compile("[^a-zA-Z .\\-_0-9]+");

   public static String getOutputLogName(TestDescriptor suite) {
      return SANITIZE.matcher("OUTPUT-" + suite.getName() + ".txt").replaceAll("_");
   }

   @Override
   public void afterTest(TestDescriptor testDescriptor, TestResult result) {
      // Include test failure exception stacktrace(s) in test output log.
      if (result.getResultType() == TestResult.ResultType.FAILURE) {
         if (result.getExceptions().size() > 0) {
            String message = formatter.format(testDescriptor, result.getExceptions());
            handlerFor(testDescriptor).write(message);
         }
      }
   }

   private OutputHandler handlerFor(TestDescriptor descriptor) {
      // Attach output of leaves (individual tests) to their parent.
      if (!descriptor.isComposite()) {
         descriptor = descriptor.getParent();
      }
      return outputHandlers.computeIfAbsent(TestKey.of(descriptor), (key) -> new OutputHandler());
   }

   public static class TestKey {
      private final String key;

      private TestKey(String key) {
         this.key = key;
      }

      public static TestKey of(TestDescriptor d) {
         StringBuilder key = new StringBuilder();
         key.append(d.getClassName());
         key.append("::");
         key.append(d.getName());
         key.append("::");
         key.append(d.getParent() == null ? "-" : d.getParent().toString());
         return new TestKey(key.toString());
      }

      @Override
      public boolean equals(Object o) {
         return o != null &&
             o.getClass() == this.getClass() &&
             Objects.equals(((TestKey) o).key, key);
      }

      @Override
      public int hashCode() {
         return key.hashCode();
      }

       @Override
       public String toString() {
           return key;
       }
   }

   private class OutputHandler implements Closeable {
      // Max single-line buffer before automatic wrap occurs.
      private static final int MAX_LINE_WIDTH = 1024 * 4;

      private final SpillWriter buffer;

      // internal stream.
      private final PrefixedWriter sint;
      // stdout
      private final PrefixedWriter sout;
      // stderr
      private final PrefixedWriter serr;

      // last used stream (so that we can flush it properly and prefixes are not screwed up).
      private PrefixedWriter last;

      public OutputHandler() {
         buffer = new SpillWriter(() -> {
            try {
               return Files.createTempFile(spillDir, "spill-", ".tmp");
            } catch (IOException e) {
               throw new UncheckedIOException(e);
            }
         });

         Writer sink = buffer;
         if (verboseMode) {
            sink = new StdOutTeeWriter(buffer);
         }

         sint = new PrefixedWriter("   > ", sink, MAX_LINE_WIDTH);
         sout = new PrefixedWriter("  1> ", sink, MAX_LINE_WIDTH);
         serr = new PrefixedWriter("  2> ", sink, MAX_LINE_WIDTH);
         last = sint;
      }

      public void write(TestOutputEvent event) {
         write((event.getDestination() == TestOutputEvent.Destination.StdOut ? sout : serr), event.getMessage());
      }

      public void write(String message) {
         write(sint, message);
      }

      public long length() throws IOException {
         return buffer.length();
      }

      private void write(PrefixedWriter out, String message) {
         try {
            if (out != last) {
               last.completeLine();
               last = out;
            }
            out.write(message);
         } catch (IOException e) {
            throw new UncheckedIOException("Unable to write to test output.", e);
         }
      }

      public void copyTo(Writer out) throws IOException {
         flush();
         buffer.copyTo(out);
      }

      public void flush() throws IOException {
         sout.completeLine();
         serr.completeLine();
         buffer.flush();
      }

      @Override
      public void close() throws IOException {
         buffer.close();
      }
   }
}
