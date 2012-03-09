package org.apache.lucene.util.junitcompat;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

public class TestExceptionInBeforeClassHooks extends WithNestedTests {
  public TestExceptionInBeforeClassHooks() {
    super(true);
  }

  public static class Nested1 extends WithNestedTests.AbstractNestedTest {
    @BeforeClass
    public static void beforeClass() {
      new Thread() {
        public void run() {
          throw new RuntimeException("foobar");
        }
      }.start();
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
    Assert.assertEquals(2, runClasses.getFailureCount());
    Assert.assertEquals(2, runClasses.getRunCount());
    
    String m1 = runClasses.getFailures().get(0).getTrace();
    String m2 = runClasses.getFailures().get(1).getTrace();
    Assert.assertTrue(
        (m1.contains("foobar1") && m2.contains("foobar2")) ||
        (m1.contains("foobar2") && m2.contains("foobar1")));
  }
  
  @Test
  public void testExceptionWithinBefore() {
    Result runClasses = JUnitCore.runClasses(Nested3.class);
    Assert.assertEquals(1, runClasses.getFailureCount());
    Assert.assertEquals(1, runClasses.getRunCount());
    Assert.assertTrue(runClasses.getFailures().get(0).getTrace().contains("foobar"));
  }  
  
}
