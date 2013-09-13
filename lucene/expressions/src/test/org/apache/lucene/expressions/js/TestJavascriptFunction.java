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

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.util.LuceneTestCase;

public class TestJavascriptFunction extends LuceneTestCase {
  private static double DELTA = 0.0000001;
  
  private void testComputedExpressionEvaluation(String name, String expression, double expected) throws Exception {
    Expression evaluator = JavascriptCompiler.compile(expression);
    double actual = evaluator.evaluate(0, null);
    assertEquals(name, expected, actual, DELTA);
  }
  
  public void testAbsMethod() throws Exception {
    testComputedExpressionEvaluation("abs0", "abs(0)", 0);
    testComputedExpressionEvaluation("abs1", "abs(119)", 119);
    testComputedExpressionEvaluation("abs2", "abs(119)", 119);
    testComputedExpressionEvaluation("abs3", "abs(1)", 1);
    testComputedExpressionEvaluation("abs4", "abs(-1)", 1);
  }
  
  public void testAcosMethod() throws Exception {
    testComputedExpressionEvaluation("acos0", "acos(-1)", Math.PI);
    testComputedExpressionEvaluation("acos1", "acos(-0.8660254)", Math.PI*5/6);
    testComputedExpressionEvaluation("acos3", "acos(-0.7071068)", Math.PI*3/4);
    testComputedExpressionEvaluation("acos4", "acos(-0.5)", Math.PI*2/3);
    testComputedExpressionEvaluation("acos5", "acos(0)", Math.PI/2);
    testComputedExpressionEvaluation("acos6", "acos(0.5)", Math.PI/3);
    testComputedExpressionEvaluation("acos7", "acos(0.7071068)", Math.PI/4);
    testComputedExpressionEvaluation("acos8", "acos(0.8660254)", Math.PI/6);
    testComputedExpressionEvaluation("acos9", "acos(1)", 0);
  }
  
  public void testAcoshMethod() throws Exception {
    testComputedExpressionEvaluation("acosh0", "acosh(1)", 0);
    testComputedExpressionEvaluation("acosh1", "acosh(2.5)", 1.5667992369724109);
    testComputedExpressionEvaluation("acosh2", "acosh(1234567.89)", 14.719378760739708);
  }
  
  public void testAsinMethod() throws Exception {
    testComputedExpressionEvaluation("asin0", "asin(-1)", -Math.PI/2);
    testComputedExpressionEvaluation("asin1", "asin(-0.8660254)", -Math.PI/3);
    testComputedExpressionEvaluation("asin3", "asin(-0.7071068)", -Math.PI/4);
    testComputedExpressionEvaluation("asin4", "asin(-0.5)", -Math.PI/6);
    testComputedExpressionEvaluation("asin5", "asin(0)", 0);
    testComputedExpressionEvaluation("asin6", "asin(0.5)", Math.PI/6);
    testComputedExpressionEvaluation("asin7", "asin(0.7071068)", Math.PI/4);
    testComputedExpressionEvaluation("asin8", "asin(0.8660254)", Math.PI/3);
    testComputedExpressionEvaluation("asin9", "asin(1)", Math.PI/2);
  }
  
  public void testAsinhMethod() throws Exception {
    testComputedExpressionEvaluation("asinh0", "asinh(-1234567.89)", -14.719378760740035);
    testComputedExpressionEvaluation("asinh1", "asinh(-2.5)", -1.6472311463710958);
    testComputedExpressionEvaluation("asinh2", "asinh(-1)", -0.8813735870195429);
    testComputedExpressionEvaluation("asinh3", "asinh(0)", 0);
    testComputedExpressionEvaluation("asinh4", "asinh(1)", 0.8813735870195429);
    testComputedExpressionEvaluation("asinh5", "asinh(2.5)", 1.6472311463710958);
    testComputedExpressionEvaluation("asinh6", "asinh(1234567.89)", 14.719378760740035);
  }
  
  public void testAtanMethod() throws Exception {
    testComputedExpressionEvaluation("atan0", "atan(-1.732050808)", -Math.PI/3);
    testComputedExpressionEvaluation("atan1", "atan(-1)", -Math.PI/4);
    testComputedExpressionEvaluation("atan3", "atan(-0.577350269)", -Math.PI/6);
    testComputedExpressionEvaluation("atan4", "atan(0)", 0);
    testComputedExpressionEvaluation("atan5", "atan(0.577350269)", Math.PI/6);
    testComputedExpressionEvaluation("atan6", "atan(1)", Math.PI/4);
    testComputedExpressionEvaluation("atan7", "atan(1.732050808)", Math.PI/3);
  }
  
  public void testAtanhMethod() throws Exception {
    testComputedExpressionEvaluation("atanh0", "atanh(-1)", Double.NEGATIVE_INFINITY);
    testComputedExpressionEvaluation("atanh1", "atanh(-0.5)", -0.5493061443340549);
    testComputedExpressionEvaluation("atanh2", "atanh(0)", 0);
    testComputedExpressionEvaluation("atanh3", "atanh(0.5)", 0.5493061443340549);
    testComputedExpressionEvaluation("atanh4", "atanh(1)", Double.POSITIVE_INFINITY);
  }
  
  public void testCeilMethod() throws Exception {
    testComputedExpressionEvaluation("ceil0", "ceil(0)", 0);
    testComputedExpressionEvaluation("ceil1", "ceil(0.1)", 1);
    testComputedExpressionEvaluation("ceil2", "ceil(0.9)", 1);
    testComputedExpressionEvaluation("ceil3", "ceil(25.2)", 26);
    testComputedExpressionEvaluation("ceil4", "ceil(-0.1)", 0);
    testComputedExpressionEvaluation("ceil5", "ceil(-0.9)", 0);
    testComputedExpressionEvaluation("ceil6", "ceil(-1.1)", -1);
  }
  
  public void testCosMethod() throws Exception {
    testComputedExpressionEvaluation("cos0", "cos(0)", 1);
    testComputedExpressionEvaluation("cos1", "cos(" + Math.PI/2 + ")", 0);
    testComputedExpressionEvaluation("cos2", "cos(" + -Math.PI/2 + ")", 0);
    testComputedExpressionEvaluation("cos3", "cos(" + Math.PI/4 + ")", 0.7071068);
    testComputedExpressionEvaluation("cos4", "cos(" + -Math.PI/4 + ")", 0.7071068);
    testComputedExpressionEvaluation("cos5", "cos(" + Math.PI*2/3 + ")",-0.5);
    testComputedExpressionEvaluation("cos6", "cos(" + -Math.PI*2/3 + ")", -0.5);
    testComputedExpressionEvaluation("cos7", "cos(" + Math.PI/6 + ")", 0.8660254);
    testComputedExpressionEvaluation("cos8", "cos(" + -Math.PI/6 + ")", 0.8660254);
  }
  
  public void testCoshMethod() throws Exception {
    testComputedExpressionEvaluation("cosh0", "cosh(0)", 1);
    testComputedExpressionEvaluation("cosh1", "cosh(-1)", 1.5430806348152437);
    testComputedExpressionEvaluation("cosh2", "cosh(1)", 1.5430806348152437);
    testComputedExpressionEvaluation("cosh3", "cosh(-0.5)", 1.1276259652063807);
    testComputedExpressionEvaluation("cosh4", "cosh(0.5)", 1.1276259652063807);
    testComputedExpressionEvaluation("cosh5", "cosh(-12.3456789)", 114982.09728671524);
    testComputedExpressionEvaluation("cosh6", "cosh(12.3456789)", 114982.09728671524);
  }
  
  public void testExpMethod() throws Exception {
    testComputedExpressionEvaluation("exp0", "exp(0)", 1);
    testComputedExpressionEvaluation("exp1", "exp(-1)", 0.36787944117);
    testComputedExpressionEvaluation("exp2", "exp(1)", 2.71828182846);
    testComputedExpressionEvaluation("exp3", "exp(-0.5)", 0.60653065971);
    testComputedExpressionEvaluation("exp4", "exp(0.5)", 1.6487212707);
    testComputedExpressionEvaluation("exp5", "exp(-12.3456789)", 0.0000043485);
    testComputedExpressionEvaluation("exp6", "exp(12.3456789)", 229964.194569);
  }
  
  public void testFloorMethod() throws Exception {
    testComputedExpressionEvaluation("floor0", "floor(0)", 0);
    testComputedExpressionEvaluation("floor1", "floor(0.1)", 0);
    testComputedExpressionEvaluation("floor2", "floor(0.9)", 0);
    testComputedExpressionEvaluation("floor3", "floor(25.2)", 25);
    testComputedExpressionEvaluation("floor4", "floor(-0.1)", -1);
    testComputedExpressionEvaluation("floor5", "floor(-0.9)", -1);
    testComputedExpressionEvaluation("floor6", "floor(-1.1)", -2);
  }
  
  public void testLnMethod() throws Exception {
    testComputedExpressionEvaluation("ln0", "ln(0)", Double.NEGATIVE_INFINITY);
    testComputedExpressionEvaluation("ln1", "ln(" + Math.E + ")", 1);
    testComputedExpressionEvaluation("ln2", "ln(-1)", Double.NaN);
    testComputedExpressionEvaluation("ln3", "ln(1)", 0);
    testComputedExpressionEvaluation("ln4", "ln(0.5)", -0.69314718056);
    testComputedExpressionEvaluation("ln5", "ln(12.3456789)", 2.51330611521);
  }
  
  public void testLog10Method() throws Exception {
    testComputedExpressionEvaluation("log100", "log10(0)", Double.NEGATIVE_INFINITY);
    testComputedExpressionEvaluation("log101", "log10(1)", 0);
    testComputedExpressionEvaluation("log102", "log10(-1)", Double.NaN);
    testComputedExpressionEvaluation("log103", "log10(0.5)", -0.3010299956639812);
    testComputedExpressionEvaluation("log104", "log10(12.3456789)", 1.0915149771692705);
  }
  
  public void testLognMethod() throws Exception {
    testComputedExpressionEvaluation("logn0", "logn(2, 0)", Double.NEGATIVE_INFINITY);
    testComputedExpressionEvaluation("logn1", "logn(2, 1)", 0);
    testComputedExpressionEvaluation("logn2", "logn(2, -1)", Double.NaN);
    testComputedExpressionEvaluation("logn3", "logn(2, 0.5)", -1);
    testComputedExpressionEvaluation("logn4", "logn(2, 12.3456789)", 3.6259342686489378);
    testComputedExpressionEvaluation("logn5", "logn(2.5, 0)", Double.NEGATIVE_INFINITY);
    testComputedExpressionEvaluation("logn6", "logn(2.5, 1)", 0);
    testComputedExpressionEvaluation("logn7", "logn(2.5, -1)", Double.NaN);
    testComputedExpressionEvaluation("logn8", "logn(2.5, 0.5)", -0.75647079736603);
    testComputedExpressionEvaluation("logn9", "logn(2.5, 12.3456789)", 2.7429133874016745);
  }
  
  public void testMaxMethod() throws Exception {
    testComputedExpressionEvaluation("max0", "max(0, 0)", 0);
    testComputedExpressionEvaluation("max1", "max(1, 0)", 1);
    testComputedExpressionEvaluation("max2", "max(0, -1)", 0);
    testComputedExpressionEvaluation("max3", "max(-1, 0)", 0);
    testComputedExpressionEvaluation("max4", "max(25, 23)", 25);
  }
  
  public void testMinMethod() throws Exception {
    testComputedExpressionEvaluation("min0", "min(0, 0)", 0);
    testComputedExpressionEvaluation("min1", "min(1, 0)", 0);
    testComputedExpressionEvaluation("min2", "min(0, -1)", -1);
    testComputedExpressionEvaluation("min3", "min(-1, 0)", -1);
    testComputedExpressionEvaluation("min4", "min(25, 23)", 23);
  }
  
  public void testPowMethod() throws Exception {
    testComputedExpressionEvaluation("pow0", "pow(0, 0)", 1);
    testComputedExpressionEvaluation("pow1", "pow(0.1, 2)", 0.01);
    testComputedExpressionEvaluation("pow2", "pow(0.9, -1)", 1.1111111111111112);
    testComputedExpressionEvaluation("pow3", "pow(2.2, -2.5)", 0.13929749224447147);
    testComputedExpressionEvaluation("pow4", "pow(5, 3)", 125);
    testComputedExpressionEvaluation("pow5", "pow(-0.9, 5)", -0.59049);
    testComputedExpressionEvaluation("pow6", "pow(-1.1, 2)", 1.21);
  }
  
  public void testSinMethod() throws Exception {
    testComputedExpressionEvaluation("sin0", "sin(0)", 0);
    testComputedExpressionEvaluation("sin1", "sin(" + Math.PI/2 + ")", 1);
    testComputedExpressionEvaluation("sin2", "sin(" + -Math.PI/2 + ")", -1);
    testComputedExpressionEvaluation("sin3", "sin(" + Math.PI/4 + ")", 0.7071068);
    testComputedExpressionEvaluation("sin4", "sin(" + -Math.PI/4 + ")", -0.7071068);
    testComputedExpressionEvaluation("sin5", "sin(" + Math.PI*2/3 + ")", 0.8660254);
    testComputedExpressionEvaluation("sin6", "sin(" + -Math.PI*2/3 + ")", -0.8660254);
    testComputedExpressionEvaluation("sin7", "sin(" + Math.PI/6 + ")", 0.5);
    testComputedExpressionEvaluation("sin8", "sin(" + -Math.PI/6 + ")", -0.5);
  }
  
  public void testSinhMethod() throws Exception {
    testComputedExpressionEvaluation("sinh0", "sinh(0)", 0);
    testComputedExpressionEvaluation("sinh1", "sinh(-1)", -1.1752011936438014);
    testComputedExpressionEvaluation("sinh2", "sinh(1)", 1.1752011936438014);
    testComputedExpressionEvaluation("sinh3", "sinh(-0.5)", -0.52109530549);
    testComputedExpressionEvaluation("sinh4", "sinh(0.5)", 0.52109530549);
    testComputedExpressionEvaluation("sinh5", "sinh(-12.3456789)", -114982.09728236674);
    testComputedExpressionEvaluation("sinh6", "sinh(12.3456789)", 114982.09728236674);
  }
  
  public void testSqrtMethod() throws Exception {
    testComputedExpressionEvaluation("sqrt0", "sqrt(0)", 0);
    testComputedExpressionEvaluation("sqrt1", "sqrt(-1)", Double.NaN);
    testComputedExpressionEvaluation("sqrt2", "sqrt(0.49)", 0.7);
    testComputedExpressionEvaluation("sqrt3", "sqrt(49)", 7);
  }
  
  public void testTanMethod() throws Exception {
    testComputedExpressionEvaluation("tan0", "tan(0)", 0);
    testComputedExpressionEvaluation("tan1", "tan(-1)", -1.55740772465);
    testComputedExpressionEvaluation("tan2", "tan(1)", 1.55740772465);
    testComputedExpressionEvaluation("tan3", "tan(-0.5)", -0.54630248984);
    testComputedExpressionEvaluation("tan4", "tan(0.5)", 0.54630248984);
    testComputedExpressionEvaluation("tan1", "tan(-1.3)", -3.60210244797);
    testComputedExpressionEvaluation("tan2", "tan(1.3)", 3.60210244797);
  }
  
  public void testTanhMethod() throws Exception {
    testComputedExpressionEvaluation("tanh0", "tanh(0)", 0);
    testComputedExpressionEvaluation("tanh1", "tanh(-1)", -0.76159415595);
    testComputedExpressionEvaluation("tanh2", "tanh(1)", 0.76159415595);
    testComputedExpressionEvaluation("tanh3", "tanh(-0.5)", -0.46211715726);
    testComputedExpressionEvaluation("tanh4", "tanh(0.5)", 0.46211715726);
    testComputedExpressionEvaluation("tanh5", "tanh(-12.3456789)", -0.99999999996);
    testComputedExpressionEvaluation("tanh6", "tanh(12.3456789)", 0.99999999996);
  }
  
  
}
