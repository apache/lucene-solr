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

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.util.LuceneTestCase;

public class TestJavascriptFunction extends LuceneTestCase {
  private static double DELTA = 0.0000001;
  
  private void assertEvaluatesTo(String expression, double expected) throws Exception {
    Expression evaluator = JavascriptCompiler.compile(expression);
    double actual = evaluator.evaluate(0, null);
    assertEquals(expected, actual, DELTA);
  }
  
  public void testAbsMethod() throws Exception {
    assertEvaluatesTo("abs(0)", 0);
    assertEvaluatesTo("abs(119)", 119);
    assertEvaluatesTo("abs(119)", 119);
    assertEvaluatesTo("abs(1)", 1);
    assertEvaluatesTo("abs(-1)", 1);
  }
  
  public void testAcosMethod() throws Exception {
    assertEvaluatesTo("acos(-1)", Math.PI);
    assertEvaluatesTo("acos(-0.8660254)", Math.PI*5/6);
    assertEvaluatesTo("acos(-0.7071068)", Math.PI*3/4);
    assertEvaluatesTo("acos(-0.5)", Math.PI*2/3);
    assertEvaluatesTo("acos(0)", Math.PI/2);
    assertEvaluatesTo("acos(0.5)", Math.PI/3);
    assertEvaluatesTo("acos(0.7071068)", Math.PI/4);
    assertEvaluatesTo("acos(0.8660254)", Math.PI/6);
    assertEvaluatesTo("acos(1)", 0);
  }
  
  public void testAcoshMethod() throws Exception {
    assertEvaluatesTo("acosh(1)", 0);
    assertEvaluatesTo("acosh(2.5)", 1.5667992369724109);
    assertEvaluatesTo("acosh(1234567.89)", 14.719378760739708);
  }
  
  public void testAsinMethod() throws Exception {
    assertEvaluatesTo("asin(-1)", -Math.PI/2);
    assertEvaluatesTo("asin(-0.8660254)", -Math.PI/3);
    assertEvaluatesTo("asin(-0.7071068)", -Math.PI/4);
    assertEvaluatesTo("asin(-0.5)", -Math.PI/6);
    assertEvaluatesTo("asin(0)", 0);
    assertEvaluatesTo("asin(0.5)", Math.PI/6);
    assertEvaluatesTo("asin(0.7071068)", Math.PI/4);
    assertEvaluatesTo("asin(0.8660254)", Math.PI/3);
    assertEvaluatesTo("asin(1)", Math.PI/2);
  }
  
  public void testAsinhMethod() throws Exception {
    assertEvaluatesTo("asinh(-1234567.89)", -14.719378760740035);
    assertEvaluatesTo("asinh(-2.5)", -1.6472311463710958);
    assertEvaluatesTo("asinh(-1)", -0.8813735870195429);
    assertEvaluatesTo("asinh(0)", 0);
    assertEvaluatesTo("asinh(1)", 0.8813735870195429);
    assertEvaluatesTo("asinh(2.5)", 1.6472311463710958);
    assertEvaluatesTo("asinh(1234567.89)", 14.719378760740035);
  }
  
  public void testAtanMethod() throws Exception {
    assertEvaluatesTo("atan(-1.732050808)", -Math.PI/3);
    assertEvaluatesTo("atan(-1)", -Math.PI/4);
    assertEvaluatesTo("atan(-0.577350269)", -Math.PI/6);
    assertEvaluatesTo("atan(0)", 0);
    assertEvaluatesTo("atan(0.577350269)", Math.PI/6);
    assertEvaluatesTo("atan(1)", Math.PI/4);
    assertEvaluatesTo("atan(1.732050808)", Math.PI/3);
  }
  
  public void testAtan2Method() throws Exception {
    assertEvaluatesTo("atan2(+0,+0)", +0.0);
    assertEvaluatesTo("atan2(+0,-0)", +Math.PI);
    assertEvaluatesTo("atan2(-0,+0)", -0.0);
    assertEvaluatesTo("atan2(-0,-0)", -Math.PI);
    assertEvaluatesTo("atan2(2,2)", Math.PI/4);
    assertEvaluatesTo("atan2(-2,2)", -Math.PI/4);
    assertEvaluatesTo("atan2(2,-2)", Math.PI*3/4);
    assertEvaluatesTo("atan2(-2,-2)", -Math.PI*3/4);
  }
  
  public void testAtanhMethod() throws Exception {
    assertEvaluatesTo("atanh(-1)", Double.NEGATIVE_INFINITY);
    assertEvaluatesTo("atanh(-0.5)", -0.5493061443340549);
    assertEvaluatesTo("atanh(0)", 0);
    assertEvaluatesTo("atanh(0.5)", 0.5493061443340549);
    assertEvaluatesTo("atanh(1)", Double.POSITIVE_INFINITY);
  }
  
  public void testCeilMethod() throws Exception {
    assertEvaluatesTo("ceil(0)", 0);
    assertEvaluatesTo("ceil(0.1)", 1);
    assertEvaluatesTo("ceil(0.9)", 1);
    assertEvaluatesTo("ceil(25.2)", 26);
    assertEvaluatesTo("ceil(-0.1)", 0);
    assertEvaluatesTo("ceil(-0.9)", 0);
    assertEvaluatesTo("ceil(-1.1)", -1);
  }
  
  public void testCosMethod() throws Exception {
    assertEvaluatesTo("cos(0)", 1);
    assertEvaluatesTo("cos(" + Math.PI/2 + ")", 0);
    assertEvaluatesTo("cos(" + -Math.PI/2 + ")", 0);
    assertEvaluatesTo("cos(" + Math.PI/4 + ")", 0.7071068);
    assertEvaluatesTo("cos(" + -Math.PI/4 + ")", 0.7071068);
    assertEvaluatesTo("cos(" + Math.PI*2/3 + ")",-0.5);
    assertEvaluatesTo("cos(" + -Math.PI*2/3 + ")", -0.5);
    assertEvaluatesTo("cos(" + Math.PI/6 + ")", 0.8660254);
    assertEvaluatesTo("cos(" + -Math.PI/6 + ")", 0.8660254);
  }
  
  public void testCoshMethod() throws Exception {
    assertEvaluatesTo("cosh(0)", 1);
    assertEvaluatesTo("cosh(-1)", 1.5430806348152437);
    assertEvaluatesTo("cosh(1)", 1.5430806348152437);
    assertEvaluatesTo("cosh(-0.5)", 1.1276259652063807);
    assertEvaluatesTo("cosh(0.5)", 1.1276259652063807);
    assertEvaluatesTo("cosh(-12.3456789)", 114982.09728671524);
    assertEvaluatesTo("cosh(12.3456789)", 114982.09728671524);
  }
  
  public void testExpMethod() throws Exception {
    assertEvaluatesTo("exp(0)", 1);
    assertEvaluatesTo("exp(-1)", 0.36787944117);
    assertEvaluatesTo("exp(1)", 2.71828182846);
    assertEvaluatesTo("exp(-0.5)", 0.60653065971);
    assertEvaluatesTo("exp(0.5)", 1.6487212707);
    assertEvaluatesTo("exp(-12.3456789)", 0.0000043485);
    assertEvaluatesTo("exp(12.3456789)", 229964.194569);
  }
  
  public void testFloorMethod() throws Exception {
    assertEvaluatesTo("floor(0)", 0);
    assertEvaluatesTo("floor(0.1)", 0);
    assertEvaluatesTo("floor(0.9)", 0);
    assertEvaluatesTo("floor(25.2)", 25);
    assertEvaluatesTo("floor(-0.1)", -1);
    assertEvaluatesTo("floor(-0.9)", -1);
    assertEvaluatesTo("floor(-1.1)", -2);
  }
  
  public void testHaversinMethod() throws Exception {
    assertEvaluatesTo("haversin(40.7143528,-74.0059731,40.759011,-73.9844722)", 5.284299568309);
  }
  
  public void testLnMethod() throws Exception {
    assertEvaluatesTo("ln(0)", Double.NEGATIVE_INFINITY);
    assertEvaluatesTo("ln(" + Math.E + ")", 1);
    assertEvaluatesTo("ln(-1)", Double.NaN);
    assertEvaluatesTo("ln(1)", 0);
    assertEvaluatesTo("ln(0.5)", -0.69314718056);
    assertEvaluatesTo("ln(12.3456789)", 2.51330611521);
  }
  
  public void testLog10Method() throws Exception {
    assertEvaluatesTo("log10(0)", Double.NEGATIVE_INFINITY);
    assertEvaluatesTo("log10(1)", 0);
    assertEvaluatesTo("log10(-1)", Double.NaN);
    assertEvaluatesTo("log10(0.5)", -0.3010299956639812);
    assertEvaluatesTo("log10(12.3456789)", 1.0915149771692705);
  }
  
  public void testLognMethod() throws Exception {
    assertEvaluatesTo("logn(2, 0)", Double.NEGATIVE_INFINITY);
    assertEvaluatesTo("logn(2, 1)", 0);
    assertEvaluatesTo("logn(2, -1)", Double.NaN);
    assertEvaluatesTo("logn(2, 0.5)", -1);
    assertEvaluatesTo("logn(2, 12.3456789)", 3.6259342686489378);
    assertEvaluatesTo("logn(2.5, 0)", Double.NEGATIVE_INFINITY);
    assertEvaluatesTo("logn(2.5, 1)", 0);
    assertEvaluatesTo("logn(2.5, -1)", Double.NaN);
    assertEvaluatesTo("logn(2.5, 0.5)", -0.75647079736603);
    assertEvaluatesTo("logn(2.5, 12.3456789)", 2.7429133874016745);
  }
  
  public void testMaxMethod() throws Exception {
    assertEvaluatesTo("max(0, 0)", 0);
    assertEvaluatesTo("max(1, 0)", 1);
    assertEvaluatesTo("max(0, -1)", 0);
    assertEvaluatesTo("max(-1, 0)", 0);
    assertEvaluatesTo("max(25, 23)", 25);
  }
  
  public void testMinMethod() throws Exception {
    assertEvaluatesTo("min(0, 0)", 0);
    assertEvaluatesTo("min(1, 0)", 0);
    assertEvaluatesTo("min(0, -1)", -1);
    assertEvaluatesTo("min(-1, 0)", -1);
    assertEvaluatesTo("min(25, 23)", 23);
  }
  
  public void testPowMethod() throws Exception {
    assertEvaluatesTo("pow(0, 0)", 1);
    assertEvaluatesTo("pow(0.1, 2)", 0.01);
    assertEvaluatesTo("pow(0.9, -1)", 1.1111111111111112);
    assertEvaluatesTo("pow(2.2, -2.5)", 0.13929749224447147);
    assertEvaluatesTo("pow(5, 3)", 125);
    assertEvaluatesTo("pow(-0.9, 5)", -0.59049);
    assertEvaluatesTo("pow(-1.1, 2)", 1.21);
  }
  
  public void testSinMethod() throws Exception {
    assertEvaluatesTo("sin(0)", 0);
    assertEvaluatesTo("sin(" + Math.PI/2 + ")", 1);
    assertEvaluatesTo("sin(" + -Math.PI/2 + ")", -1);
    assertEvaluatesTo("sin(" + Math.PI/4 + ")", 0.7071068);
    assertEvaluatesTo("sin(" + -Math.PI/4 + ")", -0.7071068);
    assertEvaluatesTo("sin(" + Math.PI*2/3 + ")", 0.8660254);
    assertEvaluatesTo("sin(" + -Math.PI*2/3 + ")", -0.8660254);
    assertEvaluatesTo("sin(" + Math.PI/6 + ")", 0.5);
    assertEvaluatesTo("sin(" + -Math.PI/6 + ")", -0.5);
  }
  
  public void testSinhMethod() throws Exception {
    assertEvaluatesTo("sinh(0)", 0);
    assertEvaluatesTo("sinh(-1)", -1.1752011936438014);
    assertEvaluatesTo("sinh(1)", 1.1752011936438014);
    assertEvaluatesTo("sinh(-0.5)", -0.52109530549);
    assertEvaluatesTo("sinh(0.5)", 0.52109530549);
    assertEvaluatesTo("sinh(-12.3456789)", -114982.09728236674);
    assertEvaluatesTo("sinh(12.3456789)", 114982.09728236674);
  }
  
  public void testSqrtMethod() throws Exception {
    assertEvaluatesTo("sqrt(0)", 0);
    assertEvaluatesTo("sqrt(-1)", Double.NaN);
    assertEvaluatesTo("sqrt(0.49)", 0.7);
    assertEvaluatesTo("sqrt(49)", 7);
  }
  
  public void testTanMethod() throws Exception {
    assertEvaluatesTo("tan(0)", 0);
    assertEvaluatesTo("tan(-1)", -1.55740772465);
    assertEvaluatesTo("tan(1)", 1.55740772465);
    assertEvaluatesTo("tan(-0.5)", -0.54630248984);
    assertEvaluatesTo("tan(0.5)", 0.54630248984);
    assertEvaluatesTo("tan(-1.3)", -3.60210244797);
    assertEvaluatesTo("tan(1.3)", 3.60210244797);
  }
  
  public void testTanhMethod() throws Exception {
    assertEvaluatesTo("tanh(0)", 0);
    assertEvaluatesTo("tanh(-1)", -0.76159415595);
    assertEvaluatesTo("tanh(1)", 0.76159415595);
    assertEvaluatesTo("tanh(-0.5)", -0.46211715726);
    assertEvaluatesTo("tanh(0.5)", 0.46211715726);
    assertEvaluatesTo("tanh(-12.3456789)", -0.99999999996);
    assertEvaluatesTo("tanh(12.3456789)", 0.99999999996);
  }
}
