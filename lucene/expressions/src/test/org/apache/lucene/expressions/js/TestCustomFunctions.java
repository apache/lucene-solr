package org.apache.lucene.expressions.js;

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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.util.LuceneTestCase;

/** Tests customing the function map */
public class TestCustomFunctions extends LuceneTestCase {
  private static double DELTA = 0.0000001;
  
  /** empty list of methods */
  public void testEmpty() throws Exception {
    Map<String,Method> functions = Collections.emptyMap();
    try {
      JavascriptCompiler.compile("sqrt(20)", functions, getClass().getClassLoader());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unrecognized method"));
    }
  }
  
  /** using the default map explicitly */
  public void testDefaultList() throws Exception {
    Map<String,Method> functions = JavascriptCompiler.DEFAULT_FUNCTIONS;
    Expression expr = JavascriptCompiler.compile("sqrt(20)", functions, getClass().getClassLoader());
    assertEquals(Math.sqrt(20), expr.evaluate(0, null), DELTA);
  }
  
  public static double zeroArgMethod() { return 5; }
  
  /** tests a method with no arguments */
  public void testNoArgMethod() throws Exception {
    Map<String,Method> functions = new HashMap<String,Method>();
    functions.put("foo", getClass().getMethod("zeroArgMethod"));
    Expression expr = JavascriptCompiler.compile("foo()", functions, getClass().getClassLoader());
    assertEquals(5, expr.evaluate(0, null), DELTA);
  }
  
  public static double oneArgMethod(double arg1) { return 3 + arg1; }
  
  /** tests a method with one arguments */
  public void testOneArgMethod() throws Exception {
    Map<String,Method> functions = new HashMap<String,Method>();
    functions.put("foo", getClass().getMethod("oneArgMethod", double.class));
    Expression expr = JavascriptCompiler.compile("foo(3)", functions, getClass().getClassLoader());
    assertEquals(6, expr.evaluate(0, null), DELTA);
  }
  
  public static double threeArgMethod(double arg1, double arg2, double arg3) { return arg1 + arg2 + arg3; }
  
  /** tests a method with three arguments */
  public void testThreeArgMethod() throws Exception {
    Map<String,Method> functions = new HashMap<String,Method>();
    functions.put("foo", getClass().getMethod("threeArgMethod", double.class, double.class, double.class));
    Expression expr = JavascriptCompiler.compile("foo(3, 4, 5)", functions, getClass().getClassLoader());
    assertEquals(12, expr.evaluate(0, null), DELTA);
  }
  
  /** tests a map with 2 functions */
  public void testTwoMethods() throws Exception {
    Map<String,Method> functions = new HashMap<String,Method>();
    functions.put("foo", getClass().getMethod("zeroArgMethod"));
    functions.put("bar", getClass().getMethod("oneArgMethod", double.class));
    Expression expr = JavascriptCompiler.compile("foo() + bar(3)", functions, getClass().getClassLoader());
    assertEquals(11, expr.evaluate(0, null), DELTA);
  }
  
  public static String bogusReturnType() { return "bogus!"; }
  
  /** wrong return type: must be double */
  public void testWrongReturnType() throws Exception {
    Map<String,Method> functions = new HashMap<String,Method>();
    functions.put("foo", getClass().getMethod("bogusReturnType"));
    try {
      JavascriptCompiler.compile("foo()", functions, getClass().getClassLoader());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("does not return a double"));
    }
  }
  
  public static double bogusParameterType(String s) { return 0; }
  
  /** wrong param type: must be doubles */
  public void testWrongParameterType() throws Exception {
    Map<String,Method> functions = new HashMap<String,Method>();
    functions.put("foo", getClass().getMethod("bogusParameterType", String.class));
    try {
      JavascriptCompiler.compile("foo(2)", functions, getClass().getClassLoader());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must take only double parameters"));
    }
  }
  
  public double nonStaticMethod() { return 0; }
  
  /** wrong modifiers: must be static */
  public void testWrongNotStatic() throws Exception {
    Map<String,Method> functions = new HashMap<String,Method>();
    functions.put("foo", getClass().getMethod("nonStaticMethod"));
    try {
      JavascriptCompiler.compile("foo()", functions, getClass().getClassLoader());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("is not static"));
    }
  }
  
  static double nonPublicMethod() { return 0; }
  
  /** wrong modifiers: must be public */
  public void testWrongNotPublic() throws Exception {
    Map<String,Method> functions = new HashMap<String,Method>();
    functions.put("foo", getClass().getDeclaredMethod("nonPublicMethod"));
    try {
      JavascriptCompiler.compile("foo()", functions, getClass().getClassLoader());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("is not public"));
    }
  }
  
  static class NestedNotPublic {
    public static double method() { return 0; }
  }
  
  /** wrong class modifiers: class containing method is not public */
  public void testWrongNestedNotPublic() throws Exception {
    Map<String,Method> functions = new HashMap<String,Method>();
    functions.put("foo", NestedNotPublic.class.getMethod("method"));
    try {
      JavascriptCompiler.compile("foo()", functions, getClass().getClassLoader());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("is not public"));
    }
  }
  
  /** hack to load this test a second time in a different classLoader */
  static class Loader extends ClassLoader {
    Loader(ClassLoader parent) {
      super(parent);
    }

    public Class<?> loadFromParentResource(String className) throws Exception {
      final ByteArrayOutputStream byteCode = new ByteArrayOutputStream();
      try (InputStream in = getParent().getResourceAsStream(className.replace('.', '/') + ".class")) {
        final byte[] buf = new byte[1024];
        int read;
        do {
          read = in.read(buf);
          if (read > 0) byteCode.write(buf, 0, read);
        } while (read > 0);
      }
      final byte[] bc = byteCode.toByteArray();
      return defineClass(className, bc, 0, bc.length);
    }
  }
  
  /** uses this test with a different classloader and tries to
   * register it using the default classloader, which should fail */
  public void testClassLoader() throws Exception {
    Loader child = new Loader(this.getClass().getClassLoader());
    Class<?> thisInDifferentLoader = child.loadFromParentResource(getClass().getName());
    Map<String,Method> functions = Collections.singletonMap("zeroArgMethod", thisInDifferentLoader.getMethod("zeroArgMethod"));
    
    // use our classloader, not the foreign one, which should fail!
    try {
      JavascriptCompiler.compile("zeroArgMethod()", functions, getClass().getClassLoader());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("is not declared by a class which is accessible by the given parent ClassLoader"));
    }
    
    // this should pass:
    Expression expr = JavascriptCompiler.compile("zeroArgMethod()", functions, child);
    assertEquals(5, expr.evaluate(0, null), DELTA);
    
    // mix foreign and default functions
    Map<String,Method> mixedFunctions = new HashMap<>(JavascriptCompiler.DEFAULT_FUNCTIONS);
    mixedFunctions.putAll(functions);
    expr = JavascriptCompiler.compile("zeroArgMethod()", mixedFunctions, child);
    assertEquals(5, expr.evaluate(0, null), DELTA);
    expr = JavascriptCompiler.compile("sqrt(20)", mixedFunctions, child);
    assertEquals(Math.sqrt(20), expr.evaluate(0, null), DELTA);
    try {
      JavascriptCompiler.compile("zeroArgMethod()", functions, getClass().getClassLoader());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("is not declared by a class which is accessible by the given parent ClassLoader"));
    }
  }
}
