package org.apache.lucene.util.junitcompat;

import java.util.Arrays;
import java.util.Stack;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runners.model.Statement;

/**
 * This verifies that JUnit {@link Rule}s are invoked before 
 * {@link Before} and {@link  After} hooks. This should be the
 * case from JUnit 4.10 on.
 */
public class TestJUnitRuleOrder extends WithNestedTests {
  static Stack<String> stack;

  public TestJUnitRuleOrder() {
    super(true);
  }
  
  public static class Nested extends WithNestedTests.AbstractNestedTest {
    @Before
    public void before() {
      stack.push("@Before");
    }
    
    @After
    public void after() {
      stack.push("@After");
    }

    @Rule
    public TestRule testRule = new TestRule() {
      @Override
      public Statement apply(final Statement base, Description description) {
        return new Statement() {
          public void evaluate() throws Throwable {
            stack.push("@Rule before");
            base.evaluate();
            stack.push("@Rule after");
          }
        };
      }
    };

    @Test
    public void test() {/* empty */}

    @BeforeClass
    public static void beforeClassCleanup() {
      stack = new Stack<String>();
    }

    @AfterClass
    public static void afterClassCheck() {
      stack.push("@AfterClass");
    }    
  }

  @Test
  public void testRuleOrder() {
    JUnitCore.runClasses(Nested.class);
    Assert.assertEquals(
        Arrays.toString(stack.toArray()), "[@Rule before, @Before, @After, @Rule after, @AfterClass]");
  }
}
