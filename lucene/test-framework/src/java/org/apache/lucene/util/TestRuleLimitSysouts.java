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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
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

import org.apache.lucene.util.LuceneTestCase.Monster;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;


/**
 * Fails the suite if it prints over the given limit of bytes to either
 * {@link System#out} or {@link System#err},
 * unless the condition is not enforced (see {@link #isEnforced()}).
 */
public class TestRuleLimitSysouts extends TestRuleAdapter {
  /**
   * Max limit of bytes printed to either {@link System#out} or {@link System#err}. 
   * This limit is enforced per-class (suite).
   */
  public final static int DEFAULT_SYSOUT_BYTES_THRESHOLD = 8 * 1024;

  /**
   * An annotation specifying the limit of bytes per class.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public static @interface Limit {
    public int bytes();
  }

  private final static AtomicInteger bytesWritten = new AtomicInteger();

  private final static DelegateStream capturedSystemOut;
  private final static DelegateStream capturedSystemErr;
  
  /**
   * We capture system output and error streams as early as possible because
   * certain components (like the Java logging system) steal these references and
   * never refresh them.
   * 
   * Also, for this exact reason, we cannot change delegate streams for every suite.
   * This isn't as elegant as it should be, but there's no workaround for this.
   */
  static {
    System.out.flush();
    System.err.flush();

    final String csn = Charset.defaultCharset().name();
    capturedSystemOut = new DelegateStream(System.out, csn, bytesWritten);
    capturedSystemErr = new DelegateStream(System.err, csn, bytesWritten);

    System.setOut(capturedSystemOut.printStream);
    System.setErr(capturedSystemErr.printStream);
  }

  /**
   * Test failures from any tests or rules before.
   */
  private final TestRuleMarkFailure failureMarker;

  /**
   * Tracks the number of bytes written to an underlying stream by
   * incrementing an {@link AtomicInteger}.
   */
  static class DelegateStream extends FilterOutputStream {
    final PrintStream printStream;
    final AtomicInteger bytesCounter;

    public DelegateStream(OutputStream delegate, String charset, AtomicInteger bytesCounter) {
      super(delegate);
      try {
        this.printStream = new PrintStream(this, true, charset);
        this.bytesCounter = bytesCounter;
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    // Do override all three write() methods to make sure nothing slips through.

    @Override
    public void write(byte[] b) throws IOException {
      if (b.length > 0) {
        bytesCounter.addAndGet(b.length);
      }
      super.write(b);
    }
    
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (len > 0) {
        bytesCounter.addAndGet(len);
      }
      super.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
      bytesCounter.incrementAndGet();
      super.write(b);
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
    validateClassAnnotations();
  }

  private void validateClassAnnotations() {
    Class<?> target = RandomizedTest.getContext().getTargetClass();
    if (target.isAnnotationPresent(Limit.class)) {
      int bytes = target.getAnnotation(Limit.class).bytes();
      if (bytes < 0 || bytes > 1 * 1024 * 1024) {
        throw new AssertionError("The sysout limit is insane. Did you want to use "
            + "@" + LuceneTestCase.SuppressSysoutChecks.class.getName() + " annotation to "
            + "avoid sysout checks entirely?");
      }
    }
  }

  /**
   * Ensures {@link System#out} and {@link System#err} point to delegate streams.
   */
  public static void checkCaptureStreams() {
    // Make sure we still hold the right references to wrapper streams.
    if (System.out != capturedSystemOut.printStream) {
      throw new AssertionError("Something has changed System.out to: " + System.out.getClass().getName());
    }
    if (System.err != capturedSystemErr.printStream) {
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
      capturedSystemOut.printStream.flush();
      capturedSystemErr.printStream.flush();
  
      // Check for offenders, but only if everything was successful so far.
      int limit = RandomizedTest.getContext().getTargetClass().getAnnotation(Limit.class).bytes();
      if (bytesWritten.get() >= limit && failureMarker.wasSuccessful()) {
        throw new AssertionError(String.format(Locale.ENGLISH, 
            "The test or suite printed %d bytes to stdout and stderr," +
            " even though the limit was set to %d bytes. Increase the limit with @%s, ignore it completely" +
            " with @%s or run with -Dtests.verbose=true",
            bytesWritten.get(),
            limit,
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
    capturedSystemOut.printStream.flush();
    capturedSystemErr.printStream.flush();
    bytesWritten.set(0);
  }
}

