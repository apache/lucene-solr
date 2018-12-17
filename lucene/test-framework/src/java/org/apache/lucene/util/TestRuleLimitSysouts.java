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
package org.apache.lucene.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;
import org.apache.lucene.util.LuceneTestCase.Monster;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;


/**
 * This test rule serves two purposes:
 *  <ul>
 *    <li>it fails the test if it prints too much to stdout and stderr (tests that chatter too much
 *    are discouraged)</li>
 *    <li>the rule ensures an absolute hard limit of stuff written to stdout and stderr to prevent
 *    accidental infinite loops from filling all available disk space with persisted output.</li>
 *  </ul>
 *
 * The rule is not enforced for certain test types (see {@link #isEnforced()}).
 */
public class TestRuleLimitSysouts extends TestRuleAdapter {
  private static final long KB = 1024;
  private static final long MB = KB * 1024;
  private static final long GB = MB * 1024;

  /**
   * Max limit of bytes printed to either {@link System#out} or {@link System#err}. 
   * This limit is enforced per-class (suite).
   */
  public final static long DEFAULT_LIMIT = 8 * KB;

  /**
   * Max hard limit of sysout bytes.
   */
  public final static long DEFAULT_HARD_LIMIT = 2 * GB;

  /**
   * Maximum limit allowed for {@link Limit#bytes()} before sysout check suppression
   * is suggested.
   */
  public final static int MAX_LIMIT = 1 * 1024 * 1024;

  /**
   * An annotation specifying the limit of bytes per class.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface Limit {
    /**
     * The maximum number of bytes written to stdout or stderr. If exceeded, a suite failure will be
     * triggered.
     */
    long bytes();

    /**
     * Maximum number of bytes passed to actual stdout or stderr. Any writes beyond this limit will be
     * ignored (will actually cause an IOException on the underlying output, but this is silently ignored
     * by PrintStreams).
     */
    long hardLimit() default DEFAULT_HARD_LIMIT;
  }

  private final static AtomicLong bytesWritten = new AtomicLong();

  private final static PrintStream capturedSystemOut;
  private final static PrintStream capturedSystemErr;

  private final static AtomicLong hardLimit;

  /**
   * We capture system output and error streams as early as possible because
   * certain components (like the Java logging system) steal these references and
   * never refresh them.
   * 
   * Also, for this exact reason, we cannot change delegate streams for every suite.
   * This isn't as elegant as it should be, but there's no workaround for this.
   */
  static {
    PrintStream sout = System.out;
    PrintStream serr = System.err;

    sout.flush();
    serr.flush();

    hardLimit = new AtomicLong(Integer.MAX_VALUE);
    LimitPredicate limitCheck = (before, after) -> {
      long limit = hardLimit.get();
      if (after > limit) {
        if (before < limit) {
          // Crossing the boundary. Write directly to stderr.
          serr.println("\nNOTE: Hard limit on sysout exceeded, further output truncated.\n");
          serr.flush();
        }
        throw new IOException("Hard limit on sysout exceeded.");
      }
    };

    final String csn = Charset.defaultCharset().name();
    try {
      capturedSystemOut = new PrintStream(new DelegateStream(sout, bytesWritten, limitCheck), true, csn);
      capturedSystemErr = new PrintStream(new DelegateStream(serr, bytesWritten, limitCheck), true, csn);
    } catch (UnsupportedEncodingException e) {
      throw new UncheckedIOException(e);
    }

    System.setOut(capturedSystemOut);
    System.setErr(capturedSystemErr);
  }

  /**
   * Test failures from any tests or rules before.
   */
  private final TestRuleMarkFailure failureMarker;

  static interface LimitPredicate {
    void check(long before, long after) throws IOException;
  }

  /**
   * Tracks the number of bytes written to an underlying stream by
   * incrementing an {@link AtomicInteger}.
   */
  final static class DelegateStream extends OutputStream {
    private final OutputStream delegate;
    private final LimitPredicate limitPredicate;
    private final AtomicLong bytesCounter;

    public DelegateStream(OutputStream delegate, AtomicLong bytesCounter, LimitPredicate limitPredicate) {
      this.delegate = delegate;
      this.bytesCounter = bytesCounter;
      this.limitPredicate = limitPredicate;
    }

    @Override
    public void write(byte[] b) throws IOException {
      this.write(b, 0, b.length);
    }
    
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (len > 0) {
        checkLimit(len);
      }
      delegate.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
      checkLimit(1);
      delegate.write(b);
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    private void checkLimit(int bytes) throws IOException {
      long after = bytesCounter.addAndGet(bytes);
      long before = after - bytes;
      limitPredicate.check(before, after);
    }
  }

  public TestRuleLimitSysouts(TestRuleMarkFailure failureMarker) {
    this.failureMarker = failureMarker;
  }

  /** */
  @Override
  protected void before() throws Throwable {
    if (isEnforced()) {
      checkCaptureStreams();
    }
    resetCaptureState();
    applyClassAnnotations();
  }

  private void applyClassAnnotations() {
    Class<?> target = RandomizedTest.getContext().getTargetClass();
    if (target.isAnnotationPresent(Limit.class)) {
      Limit limitAnn = target.getAnnotation(Limit.class);
      long bytes = limitAnn.bytes();
      if (bytes < 0 || bytes > MAX_LIMIT) {
        throw new AssertionError("This sysout limit is very high: " + bytes + ". Did you want to use "
            + "@" + LuceneTestCase.SuppressSysoutChecks.class.getName() + " annotation to "
            + "avoid sysout checks entirely (this is discouraged)?");
      }

      hardLimit.set(limitAnn.hardLimit());
    }
  }

  /**
   * Ensures {@link System#out} and {@link System#err} point to delegate streams.
   */
  public static void checkCaptureStreams() {
    // Make sure we still hold the right references to wrapper streams.
    if (System.out != capturedSystemOut) {
      throw new AssertionError("Something has changed System.out to: " + System.out.getClass().getName());
    }
    if (System.err != capturedSystemErr) {
      throw new AssertionError("Something has changed System.err to: " + System.err.getClass().getName());
    }
  }

  protected boolean isEnforced() {
    Class<?> target = RandomizedTest.getContext().getTargetClass();

    if (LuceneTestCase.VERBOSE || 
        LuceneTestCase.INFOSTREAM ||
        target.isAnnotationPresent(Monster.class) ||
        target.isAnnotationPresent(SuppressSysoutChecks.class)) {
      return false;
    }
    
    if (!target.isAnnotationPresent(Limit.class)) {
      return false;
    }

    return true;
  }

  /**
   * We're only interested in failing the suite if it was successful (otherwise
   * just propagate the original problem and don't bother doing anything else).
   */
  @Override
  protected void afterIfSuccessful() throws Throwable {
    if (isEnforced()) {
      checkCaptureStreams();
  
      // Flush any buffers.
      capturedSystemOut.flush();
      capturedSystemErr.flush();
  
      // Check for offenders, but only if everything was successful so far.
      Limit ann = RandomizedTest.getContext().getTargetClass().getAnnotation(Limit.class);
      long limit = ann.bytes();
      long hardLimit = ann.hardLimit();
      long written = bytesWritten.get();
      if (written >= limit && failureMarker.wasSuccessful()) {
        throw new AssertionError(String.format(Locale.ENGLISH, 
            "The test or suite printed %d bytes to stdout and stderr," +
            " even though the limit was set to %d bytes.%s Increase the limit with @%s, ignore it completely" +
            " with @%s or run with -Dtests.verbose=true",
            written,
            limit,
            written <= hardLimit ? "" : "Hard limit was enforced so output is truncated.",
            Limit.class.getSimpleName(),
            SuppressSysoutChecks.class.getSimpleName()));
      }
    }
  }

  @Override
  protected void afterAlways(List<Throwable> errors) throws Throwable {
    resetCaptureState();
  }

  private void resetCaptureState() {
    capturedSystemOut.flush();
    capturedSystemErr.flush();
    bytesWritten.set(0);
    hardLimit.set(Integer.MAX_VALUE);
  }
}

