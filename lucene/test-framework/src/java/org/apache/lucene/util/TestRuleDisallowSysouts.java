package org.apache.lucene.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;


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

/**
 * Fails the suite if it prints anything to {@link System#out} or {@link System#err},
 * unless the condition is not enforced (see {@link #isEnforced()}).
 */
public class TestRuleDisallowSysouts extends TestRuleAdapter {
  /** 
   * Stack trace of any thread that wrote something to sysout or syserr. 
   */
  private final static AtomicReference<StackTraceElement[]> firstWriteStack = new AtomicReference<StackTraceElement[]>();

  private final static DelegateStream capturedSystemOut;
  private final static DelegateStream capturedSystemErr;
  
  static {
    System.out.flush();
    System.err.flush();

    final String csn = Charset.defaultCharset().name();
    capturedSystemOut = new DelegateStream(System.out, csn, firstWriteStack);
    capturedSystemErr = new DelegateStream(System.err, csn, firstWriteStack);

    System.setOut(capturedSystemOut.printStream);
    System.setErr(capturedSystemErr.printStream);
  }

  /**
   * Test failures from any tests or rules before.
   */
  private final TestRuleMarkFailure failureMarker;

  /**
   * Sets {@link #firstWriteStack} to the current stack trace upon the first actual write
   * to an underlying stream.
   */
  static class DelegateStream extends FilterOutputStream {
    private final AtomicReference<StackTraceElement[]> firstWriteStack;
    final PrintStream printStream;

    public DelegateStream(OutputStream delegate, String charset, AtomicReference<StackTraceElement[]> firstWriteStack) {
      super(delegate);
      try {
        this.firstWriteStack = firstWriteStack;
        this.printStream = new PrintStream(this, true, charset);
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    // Do override all three write() methods to make sure nothing slips through.

    @Override
    public void write(byte[] b) throws IOException {
      if (b.length > 0) {
        bytesWritten();
      }
      super.write(b);
    }
    
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (len > 0) {
        bytesWritten();
      }
      super.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
      bytesWritten();
      super.write(b);
    }
    
    private void bytesWritten() {
      // This check isn't really needed, but getting the stack is expensive and may involve
      // jit deopts, so we'll do it anyway.
      if (firstWriteStack.get() == null) {
        firstWriteStack.compareAndSet(null, Thread.currentThread().getStackTrace());
      }
    }
  }
  
  public TestRuleDisallowSysouts(TestRuleMarkFailure failureMarker) {
    this.failureMarker = failureMarker;
  }

  
  /** */
  @Override
  protected void before() throws Throwable {
    if (isEnforced()) {
      checkCaptureStreams();
    }
    resetCaptureState();
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
    if (LuceneTestCase.VERBOSE || 
        LuceneTestCase.INFOSTREAM ||
        RandomizedTest.getContext().getTargetClass().isAnnotationPresent(SuppressSysoutChecks.class)) {
      return false;
    }
    
    return !RandomizedTest.systemPropertyAsBoolean(LuceneTestCase.SYSPROP_SYSOUTS, true);
  }

  /**
   * We're only interested in failing the suite if it was successful. Otherwise
   * just propagate the original problem and don't bother.
   */
  @Override
  protected void afterIfSuccessful() throws Throwable {
    if (isEnforced()) {
      checkCaptureStreams();
  
      // Flush any buffers.
      capturedSystemOut.printStream.flush();
      capturedSystemErr.printStream.flush();
  
      // And check for offenders, but only if everything was successful so far.
      StackTraceElement[] offenderStack = firstWriteStack.get();
      if (offenderStack != null && failureMarker.wasSuccessful()) {
        AssertionError e = new AssertionError("The test or suite printed information to stdout or stderr," +
            " even though verbose mode is turned off and it's not annotated with @" + 
            SuppressSysoutChecks.class.getSimpleName() + ". This exception contains the stack" +
                " trace of the first offending call.");
        e.setStackTrace(offenderStack);
        throw e;
      }
    }
  }

  /**
   * Restore original streams.
   */
  @Override
  protected void afterAlways(List<Throwable> errors) throws Throwable {
    resetCaptureState();
  }

  private void resetCaptureState() {
    capturedSystemOut.printStream.flush();
    capturedSystemErr.printStream.flush();
    firstWriteStack.set(null);
  }  
}

