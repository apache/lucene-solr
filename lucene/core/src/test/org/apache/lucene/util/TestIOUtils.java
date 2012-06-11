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

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

public class TestIOUtils extends LuceneTestCase {

  static final class BrokenCloseable implements Closeable {
    final int i;
    
    public BrokenCloseable(int i) {
      this.i = i;
    }
  
    @Override
    public void close() throws IOException {
      throw new IOException("TEST-IO-EXCEPTION-" + i);
    }
  }

  static final class TestException extends Exception {
    public TestException() {
      super("BASE-EXCEPTION");
    }
  }

  public void testSuppressedExceptions() {
    if (!Constants.JRE_IS_MINIMUM_JAVA7) {
      System.err.println("WARNING: TestIOUtils.testSuppressedExceptions: Full test coverage only with Java 7, as suppressed exception recording is not supported before.");
    }
    
    // test with prior exception
    try {
      final TestException t = new TestException();
      IOUtils.closeWhileHandlingException(t, new BrokenCloseable(1), new BrokenCloseable(2));
    } catch (TestException e1) {
      assertEquals("BASE-EXCEPTION", e1.getMessage());
      final StringWriter sw = new StringWriter();
      final PrintWriter pw = new PrintWriter(sw);
      e1.printStackTrace(pw);
      pw.flush();
      final String trace = sw.toString();
      if (VERBOSE) {
        System.out.println("TestIOUtils.testSuppressedExceptions: Thrown Exception stack trace:");
        System.out.println(trace);
      }
      if (Constants.JRE_IS_MINIMUM_JAVA7) {
        assertTrue("Stack trace does not contain first suppressed Exception: " + trace,
          trace.contains("java.io.IOException: TEST-IO-EXCEPTION-1"));
        assertTrue("Stack trace does not contain second suppressed Exception: " + trace,
          trace.contains("java.io.IOException: TEST-IO-EXCEPTION-2"));
      }
    } catch (IOException e2) {
      fail("IOException should not be thrown here");
    }
    
    // test without prior exception
    try {
      IOUtils.closeWhileHandlingException((TestException) null, new BrokenCloseable(1), new BrokenCloseable(2));
    } catch (TestException e1) {
      fail("TestException should not be thrown here");
    } catch (IOException e2) {
      assertEquals("TEST-IO-EXCEPTION-1", e2.getMessage());
      final StringWriter sw = new StringWriter();
      final PrintWriter pw = new PrintWriter(sw);
      e2.printStackTrace(pw);
      pw.flush();
      final String trace = sw.toString();
      if (VERBOSE) {
        System.out.println("TestIOUtils.testSuppressedExceptions: Thrown Exception stack trace:");
        System.out.println(trace);
      }
      if (Constants.JRE_IS_MINIMUM_JAVA7) {
        assertTrue("Stack trace does not contain suppressed Exception: " + trace,
          trace.contains("java.io.IOException: TEST-IO-EXCEPTION-2"));
      }
    }
  }
  
}
