/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.lucene.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.logging.LogManager;

import junit.framework.AssertionFailedError;
import junit.framework.Test;

import org.apache.lucene.store.LockReleaseFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitResultFormatter;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitTestRunner;
import org.apache.tools.ant.util.FileUtils;
import org.apache.tools.ant.util.StringUtils;
import org.junit.Ignore;

/**
 * Just like BriefJUnitResultFormatter "brief" bundled with ant,
 * except all formatted text is buffered until the test suite is finished.
 * At this point, the output is written at once in synchronized fashion.
 * This way tests can run in parallel without interleaving output.
 */
public class LuceneJUnitResultFormatter implements JUnitResultFormatter {
  private static final double ONE_SECOND = 1000.0;
  
  private static final NativeFSLockFactory lockFactory;
  
  /** Where to write the log to. */
  private OutputStream out;
  
  /** Formatter for timings. */
  private NumberFormat numberFormat = NumberFormat.getInstance();
  
  /** Output suite has written to System.out */
  private String systemOutput = null;
  
  /** Output suite has written to System.err */
  private String systemError = null;
  
  /** Buffer output until the end of the test */
  private ByteArrayOutputStream sb; // use a BOS for our mostly ascii-output

  private static final org.apache.lucene.store.Lock lock;

  static {
    File lockDir = new File(System.getProperty("java.io.tmpdir"),
        "lucene_junit_lock");
    lockDir.mkdirs();
    if (!lockDir.exists()) {
      throw new RuntimeException("Could not make Lock directory:" + lockDir);
    }
    try {
      lockFactory = new NativeFSLockFactory(lockDir);
      lock = lockFactory.makeLock("junit_lock");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Constructor for LuceneJUnitResultFormatter. */
  public LuceneJUnitResultFormatter() {
  }
  
  /**
   * Sets the stream the formatter is supposed to write its results to.
   * @param out the output stream to write to
   */
  public void setOutput(OutputStream out) {
    this.out = out;
  }
  
  /**
   * @see JUnitResultFormatter#setSystemOutput(String)
   */
  /** {@inheritDoc}. */
  public void setSystemOutput(String out) {
    systemOutput = out;
  }
  
  /**
   * @see JUnitResultFormatter#setSystemError(String)
   */
  /** {@inheritDoc}. */
  public void setSystemError(String err) {
    systemError = err;
  }
  
  
  /**
   * The whole testsuite started.
   * @param suite the test suite
   */
  public synchronized void startTestSuite(JUnitTest suite) {
    if (out == null) {
      return; // Quick return - no output do nothing.
    }
    sb = new ByteArrayOutputStream(); // don't reuse, so its gc'ed
    try {
      LogManager.getLogManager().readConfiguration();
    } catch (Exception e) {}
    append("Testsuite: ");
    append(suite.getName());
    append(StringUtils.LINE_SEP);
  }
  
  /**
   * The whole testsuite ended.
   * @param suite the test suite
   */
  public synchronized void endTestSuite(JUnitTest suite) {
    append("Tests run: ");
    append(suite.runCount());
    append(", Failures: ");
    append(suite.failureCount());
    append(", Errors: ");
    append(suite.errorCount());
    append(", Time elapsed: ");
    append(numberFormat.format(suite.getRunTime() / ONE_SECOND));
    append(" sec");
    append(StringUtils.LINE_SEP);
    append(StringUtils.LINE_SEP);
    
    // append the err and output streams to the log
    if (systemOutput != null && systemOutput.length() > 0) {
      append("------------- Standard Output ---------------")
      .append(StringUtils.LINE_SEP)
      .append(systemOutput)
      .append("------------- ---------------- ---------------")
      .append(StringUtils.LINE_SEP);
    }
    
    // HACK: junit gives us no way to do this in LuceneTestCase
    try {
      Class<?> clazz = Class.forName(suite.getName());
      Ignore ignore = clazz.getAnnotation(Ignore.class);
      if (ignore != null) {
        if (systemError == null) systemError = "";
        systemError += "NOTE: Ignoring test class '" + clazz.getSimpleName() + "': " 
                    + ignore.value() + StringUtils.LINE_SEP;
      }
    } catch (ClassNotFoundException e) { /* no problem */ }
    // END HACK
    
    if (systemError != null && systemError.length() > 0) {
      append("------------- Standard Error -----------------")
      .append(StringUtils.LINE_SEP)
      .append(systemError)
      .append("------------- ---------------- ---------------")
      .append(StringUtils.LINE_SEP);
    }
    
    if (out != null) {
      try {
        lock.obtain(5000);
        try {
          sb.writeTo(out);
          out.flush();
        } finally {
          try {
            lock.release();
          } catch(LockReleaseFailedException e) {
            // well lets pretend its released anyway
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("unable to write results", e);
      } finally {
        if (out != System.out && out != System.err) {
          FileUtils.close(out);
        }
      }
    }
  }
  
  /**
   * A test started.
   * @param test a test
   */
  public void startTest(Test test) {
  }
  
  /**
   * A test ended.
   * @param test a test
   */
  public void endTest(Test test) {
  }
  
  /**
   * Interface TestListener for JUnit &lt;= 3.4.
   *
   * <p>A Test failed.
   * @param test a test
   * @param t    the exception thrown by the test
   */
  public void addFailure(Test test, Throwable t) {
    formatError("\tFAILED", test, t);
  }
  
  /**
   * Interface TestListener for JUnit &gt; 3.4.
   *
   * <p>A Test failed.
   * @param test a test
   * @param t    the assertion failed by the test
   */
  public void addFailure(Test test, AssertionFailedError t) {
    addFailure(test, (Throwable) t);
  }
  
  /**
   * A test caused an error.
   * @param test  a test
   * @param error the error thrown by the test
   */
  public void addError(Test test, Throwable error) {
    formatError("\tCaused an ERROR", test, error);
  }
  
  /**
   * Format the test for printing..
   * @param test a test
   * @return the formatted testname
   */
  protected String formatTest(Test test) {
    if (test == null) {
      return "Null Test: ";
    } else {
      return "Testcase: " + test.toString() + ":";
    }
  }
  
  /**
   * Format an error and print it.
   * @param type the type of error
   * @param test the test that failed
   * @param error the exception that the test threw
   */
  protected synchronized void formatError(String type, Test test,
      Throwable error) {
    if (test != null) {
      endTest(test);
    }
    
    append(formatTest(test) + type);
    append(StringUtils.LINE_SEP);
    append(error.getMessage());
    append(StringUtils.LINE_SEP);
    String strace = JUnitTestRunner.getFilteredTrace(error);
    append(strace);
    append(StringUtils.LINE_SEP);
    append(StringUtils.LINE_SEP);
  }

  public LuceneJUnitResultFormatter append(String s) {
    if (s == null)
      s = "(null)";
    try {
      sb.write(s.getBytes()); // intentionally use default charset, its a console.
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }
  
  public LuceneJUnitResultFormatter append(long l) {
    return append(Long.toString(l));
  }
}

