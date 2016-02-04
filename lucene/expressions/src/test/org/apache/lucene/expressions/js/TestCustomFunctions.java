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
package org.apache.lucene.expressions.js;


import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.util.LuceneTestCase;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

/** Tests customing the function map */
public class TestCustomFunctions extends LuceneTestCase {
  private static double DELTA = 0.0000001;
  
  /** empty list of methods */
  public void testEmpty() throws Exception {
    Map<String,Method> functions = Collections.emptyMap();
    try {
      JavascriptCompiler.compile("sqrt(20)", functions, getClass().getClassLoader());
      fail();
    } catch (ParseException expected) {
      assertEquals("Invalid expression 'sqrt(20)': Unrecognized function call (sqrt).", expected.getMessage());
      assertEquals(expected.getErrorOffset(), 0);
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
    Map<String,Method> functions = new HashMap<>();
    functions.put("foo", getClass().getMethod("zeroArgMethod"));
    Expression expr = JavascriptCompiler.compile("foo()", functions, getClass().getClassLoader());
    assertEquals(5, expr.evaluate(0, null), DELTA);
  }

  public static double oneArgMethod(double arg1) { return 3 + arg1; }
  
  /** tests a method with one arguments */
  public void testOneArgMethod() throws Exception {
    Map<String,Method> functions = new HashMap<>();
    functions.put("foo", getClass().getMethod("oneArgMethod", double.class));
    Expression expr = JavascriptCompiler.compile("foo(3)", functions, getClass().getClassLoader());
    assertEquals(6, expr.evaluate(0, null), DELTA);
  }
  
  public static double threeArgMethod(double arg1, double arg2, double arg3) { return arg1 + arg2 + arg3; }
  
  /** tests a method with three arguments */
  public void testThreeArgMethod() throws Exception {
    Map<String,Method> functions = new HashMap<>();
    functions.put("foo", getClass().getMethod("threeArgMethod", double.class, double.class, double.class));
    Expression expr = JavascriptCompiler.compile("foo(3, 4, 5)", functions, getClass().getClassLoader());
    assertEquals(12, expr.evaluate(0, null), DELTA);
  }
  
  /** tests a map with 2 functions */
  public void testTwoMethods() throws Exception {
    Map<String,Method> functions = new HashMap<>();
    functions.put("foo", getClass().getMethod("zeroArgMethod"));
    functions.put("bar", getClass().getMethod("oneArgMethod", double.class));
    Expression expr = JavascriptCompiler.compile("foo() + bar(3)", functions, getClass().getClassLoader());
    assertEquals(11, expr.evaluate(0, null), DELTA);
  }

  /** tests invalid methods that are not allowed to become variables to be mapped */
  public void testInvalidVariableMethods() {
    try {
      JavascriptCompiler.compile("method()");
      fail();
    } catch (IllegalArgumentException exception) {
      fail();
    } catch (ParseException expected) {
      //expected
      assertEquals("Invalid expression 'method()': Unrecognized function call (method).", expected.getMessage());
      assertEquals(0, expected.getErrorOffset());
    }

    try {
      JavascriptCompiler.compile("method.method(1)");
      fail();
    } catch (IllegalArgumentException exception) {
      fail();
    } catch (ParseException expected) {
      //expected
      assertEquals("Invalid expression 'method.method(1)': Unrecognized function call (method.method).", expected.getMessage());
      assertEquals(0, expected.getErrorOffset());
    }
    
    try {
      JavascriptCompiler.compile("1 + method()");
      fail();
    } catch (IllegalArgumentException exception) {
      fail();
    } catch (ParseException expected) {
      //expected
      assertEquals("Invalid expression '1 + method()': Unrecognized function call (method).", expected.getMessage());
      assertEquals(4, expected.getErrorOffset());
    }
  }

  public static String bogusReturnType() { return "bogus!"; }
  
  /** wrong return type: must be double */
  public void testWrongReturnType() throws Exception {
    Map<String,Method> functions = new HashMap<>();
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
    Map<String,Method> functions = new HashMap<>();
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
    Map<String,Method> functions = new HashMap<>();
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
    Map<String,Method> functions = new HashMap<>();
    functions.put("foo", getClass().getDeclaredMethod("nonPublicMethod"));
    try {
      JavascriptCompiler.compile("foo()", functions, getClass().getClassLoader());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("not public"));
    }
  }

  static class NestedNotPublic {
    public static double method() { return 0; }
  }
  
  /** wrong class modifiers: class containing method is not public */
  public void testWrongNestedNotPublic() throws Exception {
    Map<String,Method> functions = new HashMap<>();
    functions.put("foo", NestedNotPublic.class.getMethod("method"));
    try {
      JavascriptCompiler.compile("foo()", functions, getClass().getClassLoader());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("not public"));
    }
  }
  
  /** Classloader that can be used to create a fake static class that has one method returning a static var */
  static final class Loader extends ClassLoader implements Opcodes {
    Loader(ClassLoader parent) {
      super(parent);
    }

    public Class<?> createFakeClass() {
      String className = TestCustomFunctions.class.getName() + "$Foo";
      ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
      classWriter.visit(Opcodes.V1_5, ACC_PUBLIC | ACC_SUPER | ACC_FINAL | ACC_SYNTHETIC,
          className.replace('.', '/'), null, Type.getInternalName(Object.class), null);
      
      org.objectweb.asm.commons.Method m = org.objectweb.asm.commons.Method.getMethod("void <init>()");
      GeneratorAdapter constructor = new GeneratorAdapter(ACC_PRIVATE | ACC_SYNTHETIC, m, null, null, classWriter);
      constructor.loadThis();
      constructor.loadArgs();
      constructor.invokeConstructor(Type.getType(Object.class), m);
      constructor.returnValue();
      constructor.endMethod();
      
      GeneratorAdapter gen = new GeneratorAdapter(ACC_STATIC | ACC_PUBLIC | ACC_SYNTHETIC,
          org.objectweb.asm.commons.Method.getMethod("double bar()"), null, null, classWriter);
      gen.push(2.0);
      gen.returnValue();
      gen.endMethod();      
      
      byte[] bc = classWriter.toByteArray();
      return defineClass(className, bc, 0, bc.length);
    }
  }
  
  /** uses this test with a different classloader and tries to
   * register it using the default classloader, which should fail */
  public void testClassLoader() throws Exception {
    ClassLoader thisLoader = getClass().getClassLoader();
    Loader childLoader = new Loader(thisLoader);
    Class<?> fooClass = childLoader.createFakeClass();
    
    Method barMethod = fooClass.getMethod("bar");
    Map<String,Method> functions = Collections.singletonMap("bar", barMethod);
    assertNotSame(thisLoader, fooClass.getClassLoader());
    assertNotSame(thisLoader, barMethod.getDeclaringClass().getClassLoader());
    
    // this should pass:
    Expression expr = JavascriptCompiler.compile("bar()", functions, childLoader);
    assertEquals(2.0, expr.evaluate(0, null), DELTA);
    
    // use our classloader, not the foreign one, which should fail!
    try {
      JavascriptCompiler.compile("bar()", functions, thisLoader);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("is not declared by a class which is accessible by the given parent ClassLoader"));
    }
    
    // mix foreign and default functions
    Map<String,Method> mixedFunctions = new HashMap<>(JavascriptCompiler.DEFAULT_FUNCTIONS);
    mixedFunctions.putAll(functions);
    expr = JavascriptCompiler.compile("bar()", mixedFunctions, childLoader);
    assertEquals(2.0, expr.evaluate(0, null), DELTA);
    expr = JavascriptCompiler.compile("sqrt(20)", mixedFunctions, childLoader);
    assertEquals(Math.sqrt(20), expr.evaluate(0, null), DELTA);
    
    // use our classloader, not the foreign one, which should fail!
    try {
      JavascriptCompiler.compile("bar()", mixedFunctions, thisLoader);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("is not declared by a class which is accessible by the given parent ClassLoader"));
    }
  }
  
  static String MESSAGE = "This should not happen but it happens";
  
  public static class StaticThrowingException {
    public static double method() { throw new ArithmeticException(MESSAGE); }
  }
  
  /** the method throws an exception. We should check the stack trace that it contains the source code of the expression as file name. */
  public void testThrowingException() throws Exception {
    Map<String,Method> functions = new HashMap<>();
    functions.put("foo", StaticThrowingException.class.getMethod("method"));
    String source = "3 * foo() / 5";
    Expression expr = JavascriptCompiler.compile(source, functions, getClass().getClassLoader());
    try {
      expr.evaluate(0, null);
      fail();
    } catch (ArithmeticException e) {
      assertEquals(MESSAGE, e.getMessage());
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      pw.flush();
      assertTrue(sw.toString().contains("JavascriptCompiler$CompiledExpression.evaluate(" + source + ")"));
    }
  }

  /** test that namespaces work with custom expressions. */
  public void testNamespaces() throws Exception {
    Map<String, Method> functions = new HashMap<>();
    functions.put("foo.bar", getClass().getMethod("zeroArgMethod"));
    String source = "foo.bar()";
    Expression expr = JavascriptCompiler.compile(source, functions, getClass().getClassLoader());
    assertEquals(5, expr.evaluate(0, null), DELTA);
  }
}
