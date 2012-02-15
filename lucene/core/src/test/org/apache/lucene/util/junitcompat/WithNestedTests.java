package org.apache.lucene.util.junitcompat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;

/**
 * An abstract test class that prepares nested test classes to run.
 * A nested test class will assume it's executed under control of this
 * class and be ignored otherwise. 
 * 
 * <p>The purpose of this is so that nested test suites don't run from
 * IDEs like Eclipse (where they are automatically detected).
 * 
 * <p>This class cannot extend {@link LuceneTestCase} because in case
 * there's a nested {@link LuceneTestCase} afterclass hooks run twice and
 * cause havoc (static fields).
 */
public abstract class WithNestedTests {
  public static ThreadLocal<Boolean> runsAsNested = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };

  public static abstract class AbstractNestedTest extends LuceneTestCase {
    @Before
    public void before() {
      Assume.assumeTrue(isRunningNested());
    }

    protected static boolean isRunningNested() {
      return runsAsNested.get() != null && runsAsNested.get();
    }
  }

  private boolean suppressOutputStreams;

  protected WithNestedTests(boolean suppressOutputStreams) {
    this.suppressOutputStreams = suppressOutputStreams;
  }
  
  protected PrintStream prevSysErr;
  protected PrintStream prevSysOut;
  private ByteArrayOutputStream sysout;
  private ByteArrayOutputStream syserr;

  @Before
  public final void before() {
    if (suppressOutputStreams) {
      prevSysOut = System.out;
      prevSysErr = System.err;

      try {
        sysout = new ByteArrayOutputStream();
        System.setOut(new PrintStream(sysout, true, "UTF-8"));
        syserr = new ByteArrayOutputStream();
        System.setErr(new PrintStream(syserr, true, "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    runsAsNested.set(true);
  }

  @After
  public final void after() {
    runsAsNested.set(false);
    
    if (suppressOutputStreams) {
      System.out.flush();
      System.err.flush();

      System.setOut(prevSysOut);
      System.setErr(prevSysErr);
    }
  }

  protected String getSysOut() {
    Assert.assertTrue(suppressOutputStreams);
    System.out.flush();
    try {
      return new String(sysout.toByteArray(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  protected String getSysErr() {
    Assert.assertTrue(suppressOutputStreams);
    System.err.flush();
    try {
      return new String(syserr.toByteArray(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }  
}
