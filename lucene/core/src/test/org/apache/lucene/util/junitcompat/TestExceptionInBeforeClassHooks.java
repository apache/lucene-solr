package org.apache.lucene.util.junitcompat;

/**
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

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class TestExceptionInBeforeClassHooks extends WithNestedTests {
  public TestExceptionInBeforeClassHooks() {
    super(true);
  }

  public static class Nested1 extends WithNestedTests.AbstractNestedTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
      Thread t = new Thread() {
        public void run() {
          throw new RuntimeException("foobar");
        }
      };
      t.start();
      t.join();
    }

    public void test() {}
  }

  public static class Nested2 extends WithNestedTests.AbstractNestedTest {
    public void test1() throws Exception {
      Thread t = new Thread() {
        public void run() {
          throw new RuntimeException("foobar1");
        }
      };
      t.start();
      t.join();
    }

    public void test2() throws Exception {
      Thread t = new Thread() {
        public void run() {
          throw new RuntimeException("foobar2");
        }
      };
      t.start();
      t.join();
    }
    
    public void test3() throws Exception {
      Thread t = new Thread() {
        public void run() {
          throw new RuntimeException("foobar3");
        }
      };
      t.start();
      t.join();
    }    
  }

  public static class Nested3 extends WithNestedTests.AbstractNestedTest {
    @Before
    public void runBeforeTest() throws Exception {
      Thread t = new Thread() {
        public void run() {
          throw new RuntimeException("foobar");
        }
      };
      t.start();
      t.join();
    }

    public void test1() throws Exception {
    }
  }

  @Test
  public void testExceptionInBeforeClassFailsTheTest() {
    Result runClasses = JUnitCore.runClasses(Nested1.class);
    Assert.assertEquals(1, runClasses.getFailureCount());
    Assert.assertEquals(1, runClasses.getRunCount());
    Assert.assertTrue(runClasses.getFailures().get(0).getTrace().contains("foobar"));
  }

  @Test
  public void testExceptionWithinTestFailsTheTest() {
    Result runClasses = JUnitCore.runClasses(Nested2.class);
    Assert.assertEquals(3, runClasses.getFailureCount());
    Assert.assertEquals(3, runClasses.getRunCount());
    
    ArrayList<String> foobars = new ArrayList<String>();
    for (Failure f : runClasses.getFailures()) {
      Matcher m = Pattern.compile("foobar[0-9]+").matcher(f.getTrace());
      while (m.find()) {
        foobars.add(m.group());
      }
    }

    Collections.sort(foobars);
    Assert.assertEquals("[foobar1, foobar2, foobar3]", 
        Arrays.toString(foobars.toArray()));
  }
  
  @Test
  public void testExceptionWithinBefore() {
    Result runClasses = JUnitCore.runClasses(Nested3.class);
    Assert.assertEquals(1, runClasses.getFailureCount());
    Assert.assertEquals(1, runClasses.getRunCount());
    Assert.assertTrue(runClasses.getFailures().get(0).getTrace().contains("foobar"));
  }  
  
}
