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

import java.text.ParseException;

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.util.LuceneTestCase;

public class TestJavascriptCompiler extends LuceneTestCase {

  public void testValidCompiles() throws Exception {
    assertNotNull(JavascriptCompiler.compile("100"));
    assertNotNull(JavascriptCompiler.compile("valid0+100"));
    assertNotNull(JavascriptCompiler.compile("valid0+\n100"));
    assertNotNull(JavascriptCompiler.compile("logn(2, 20+10-5.0)"));
  }

  public void testValidVariables() throws Exception {
    doTestValidVariable("object.valid0");
    doTestValidVariable("object0.object1.valid1");
    doTestValidVariable("array0[1]");
    doTestValidVariable("array0[1].x");
    doTestValidVariable("multiarray[0][0]");
    doTestValidVariable("multiarray[0][0].x");
    doTestValidVariable("strindex['hello']");
    doTestValidVariable("strindex[\"hello\"]", "strindex['hello']");
    doTestValidVariable("empty['']");
    doTestValidVariable("empty[\"\"]", "empty['']");
    doTestValidVariable("strindex['\u304A\u65E9\u3046\u3054\u3056\u3044\u307E\u3059']");
    doTestValidVariable("strindex[\"\u304A\u65E9\u3046\u3054\u3056\u3044\u307E\u3059\"]",
                        "strindex['\u304A\u65E9\u3046\u3054\u3056\u3044\u307E\u3059']");
    doTestValidVariable("escapes['\\\\\\'']");
    doTestValidVariable("escapes[\"\\\\\\\"\"]", "escapes['\\\\\"']");
    doTestValidVariable("mixed[23]['key'].sub.sub");
    doTestValidVariable("mixed[23]['key'].sub.sub[1]");
    doTestValidVariable("mixed[23]['key'].sub.sub[1].sub");
    doTestValidVariable("mixed[23]['key'].sub.sub[1].sub['abc']");
    doTestValidVariable("method.method()");
    doTestValidVariable("method.getMethod()");
    doTestValidVariable("method.METHOD()");
    doTestValidVariable("method['key'].method()");
    doTestValidVariable("method['key'].getMethod()");
    doTestValidVariable("method['key'].METHOD()");
    doTestValidVariable("method[23][\"key\"].method()", "method[23]['key'].method()");
    doTestValidVariable("method[23][\"key\"].getMethod()", "method[23]['key'].getMethod()");
    doTestValidVariable("method[23][\"key\"].METHOD()", "method[23]['key'].METHOD()");
  }

  void doTestValidVariable(String variable) throws Exception {
    doTestValidVariable(variable, variable);
  }

  void doTestValidVariable(String variable, String output) throws Exception {
    Expression e = JavascriptCompiler.compile(variable);
    assertNotNull(e);
    assertEquals(1, e.variables.length);
    assertEquals(output, e.variables[0]);
  }

  public void testInvalidVariables() throws Exception {
    doTestInvalidVariable("object.0invalid");
    doTestInvalidVariable("0.invalid");
    doTestInvalidVariable("object..invalid");
    doTestInvalidVariable(".invalid");
    doTestInvalidVariable("negative[-1]");
    doTestInvalidVariable("float[1.0]");
    doTestInvalidVariable("missing_end['abc]");
    doTestInvalidVariable("missing_end[\"abc]");
    doTestInvalidVariable("missing_begin[abc']");
    doTestInvalidVariable("missing_begin[abc\"]");
    doTestInvalidVariable("dot_needed[1]sub");
    doTestInvalidVariable("dot_needed[1]sub");
    doTestInvalidVariable("opposite_escape['\\\"']");
    doTestInvalidVariable("opposite_escape[\"\\'\"]");
  }

  void doTestInvalidVariable(String variable) {
    try {
      JavascriptCompiler.compile(variable);
      fail("\"" + variable + " should have failed to compile");
    }
    catch (ParseException expected) {
      //expected
    }
  }

  public void testInvalidLexer() throws Exception {
    try {
      JavascriptCompiler.compile("\n .");
      fail();
    } catch (ParseException pe) {
      assertTrue(pe.getMessage().contains("unexpected character '.' on line (2) position (1)"));
    }
  }

  public void testInvalidCompiles() throws Exception {
    try {
      JavascriptCompiler.compile("100 100");
      fail();
    } catch (ParseException expected) {
      // expected exception
    }
    
    try {
      JavascriptCompiler.compile("7*/-8");
      fail();
    } catch (ParseException expected) {
      // expected exception
    }
    
    try {
      JavascriptCompiler.compile("0y1234");
      fail();
    } catch (ParseException expected) {
      // expected exception
    }
    
    try {
      JavascriptCompiler.compile("500EE");
      fail();
    } catch (ParseException expected) {
      // expected exception
    }
    
    try {
      JavascriptCompiler.compile("500.5EE");
      fail();
    } catch (ParseException expected) {
      // expected exception
    }
  }
  
  public void testEmpty() {
    try {
      JavascriptCompiler.compile("");
      fail();
    } catch (ParseException expected) {
      // expected exception
    }
    
    try {
      JavascriptCompiler.compile("()");
      fail();
    } catch (ParseException expected) {
      // expected exception
    }
    
    try {
      JavascriptCompiler.compile("   \r\n   \n \t");
      fail();
    } catch (ParseException expected) {
      // expected exception
    }
  }
  
  public void testNull() throws Exception {
    try {
      JavascriptCompiler.compile(null);
      fail();
    } catch (NullPointerException expected) {
      // expected exception
    }
  }
  
  public void testWrongArity() throws Exception {
    try {
      JavascriptCompiler.compile("tan()");
      fail();
    } catch (ParseException expected) {
      assertEquals("Invalid expression 'tan()': Expected (1) arguments for function call (tan), but found (0).", expected.getMessage());
      assertEquals(expected.getErrorOffset(), 0);
    }
    
    try {
      JavascriptCompiler.compile("tan(1, 1)");
      fail();
    } catch (ParseException expected) {
      assertTrue(expected.getMessage().contains("arguments for function call"));
    }
    
    try {
      JavascriptCompiler.compile(" tan()");
      fail();
    } catch (ParseException expected) {
      assertEquals("Invalid expression ' tan()': Expected (1) arguments for function call (tan), but found (0).", expected.getMessage());
      assertEquals(expected.getErrorOffset(), 1);
    }
    
    try {
      JavascriptCompiler.compile("1 + tan()");
      fail();
    } catch (ParseException expected) {
      assertEquals("Invalid expression '1 + tan()': Expected (1) arguments for function call (tan), but found (0).", expected.getMessage());
      assertEquals(expected.getErrorOffset(), 4);
    }
  }

  public void testVariableNormalization() throws Exception {
    // multiple double quotes
    Expression x = JavascriptCompiler.compile("foo[\"a\"][\"b\"]");
    assertEquals("foo['a']['b']", x.variables[0]);

    // single and double in the same var
    x = JavascriptCompiler.compile("foo['a'][\"b\"]");
    assertEquals("foo['a']['b']", x.variables[0]);

    // escapes remain the same in single quoted strings
    x = JavascriptCompiler.compile("foo['\\\\\\'\"']");
    assertEquals("foo['\\\\\\'\"']", x.variables[0]);

    // single quotes are escaped
    x = JavascriptCompiler.compile("foo[\"'\"]");
    assertEquals("foo['\\'']", x.variables[0]);

    // double quotes are unescaped
    x = JavascriptCompiler.compile("foo[\"\\\"\"]");
    assertEquals("foo['\"']", x.variables[0]);

    // backslash escapes are kept the same
    x = JavascriptCompiler.compile("foo['\\\\'][\"\\\\\"]");
    assertEquals("foo['\\\\']['\\\\']", x.variables[0]);
  }
}
