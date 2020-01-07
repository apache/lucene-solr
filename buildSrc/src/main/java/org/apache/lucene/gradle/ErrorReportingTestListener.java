package org.apache.lucene.gradle;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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

   public ErrorReportingTestListener(TestLogging testLogging, Path spillDir) {
      this.formatter = new FullExceptionFormatter(testLogging);
      this.spillDir = spillDir;
   }

   @Override
   public void onOutput(TestDescriptor testDescriptor, TestOutputEvent outputEvent) {
      TestDescriptor suite = testDescriptor.getParent();

      // Check if this is output from the test suite itself (e.g. afterTest or beforeTest)
      if (testDescriptor.isComposite()) {
         suite = testDescriptor;
      }

      handlerFor(suite).write(outputEvent);
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
      TestKey key = TestKey.of(suite);
      try {
         // if the test suite failed, report all captured output
         if (Objects.equals(result.getResultType(), TestResult.ResultType.FAILURE)) {
            reportFailure(suite, outputHandlers.get(key));
         }
      } catch (IOException e) {
         throw new UncheckedIOException("Error reading test suite output", e);
      } finally {
         OutputHandler writer = outputHandlers.remove(key);
         if (writer != null) {
            try {
               writer.close();
            } catch (IOException e) {
               LOGGER.error("Failed to close test suite's event writer for: " + key, e);
            }
         }
      }
   }

   private void reportFailure(TestDescriptor suite, OutputHandler outputHandler) throws IOException {
      if (outputHandler != null) {
         synchronized (this) {
            System.out.println("");
            System.out.println(suite.getClassName() + " > test suite's output copied below:");
            outputHandler.copyTo(System.out);
         }
      }
   }

   @Override
   public void afterTest(TestDescriptor testDescriptor, TestResult result) {
      // Include test failure exception stacktrace(s) in test output log.
      if (result.getResultType() == TestResult.ResultType.FAILURE) {
         if (testDescriptor.getParent() != null) {
            if (result.getExceptions().size() > 0) {
               String message = formatter.format(testDescriptor, result.getExceptions());
               handlerFor(testDescriptor.getParent()).write(message);
            }
         }
      }
   }

   private OutputHandler handlerFor(TestDescriptor suite) {
      return outputHandlers.computeIfAbsent(TestKey.of(suite), (key) -> new OutputHandler());
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

         sint = new PrefixedWriter("   > ", buffer, MAX_LINE_WIDTH);
         sout = new PrefixedWriter("  1> ", buffer, MAX_LINE_WIDTH);
         serr = new PrefixedWriter("  2> ", buffer, MAX_LINE_WIDTH);
         last = sint;
      }

      public void write(TestOutputEvent event) {
         write((event.getDestination() == TestOutputEvent.Destination.StdOut ? sout : serr), event.getMessage());
      }

      public void write(String message) {
         write(sint, message);
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

      public void copyTo(PrintStream out) throws IOException {
         flush();
         buffer.copyTo(out);
         out.println();
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
