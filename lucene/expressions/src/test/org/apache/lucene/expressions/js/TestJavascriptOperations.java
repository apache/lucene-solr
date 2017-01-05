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

public class TestJavascriptOperations extends LuceneTestCase {
  private void assertEvaluatesTo(String expression, long expected) throws Exception {
    Expression evaluator = JavascriptCompiler.compile(expression);
    long actual = (long)evaluator.evaluate(null);
    assertEquals(expected, actual);
  }
  
  public void testNegationOperation() throws Exception {
    assertEvaluatesTo("-1", -1);
    assertEvaluatesTo("--1", 1);
    assertEvaluatesTo("-(-1)", 1);
    assertEvaluatesTo("-0", 0);
    assertEvaluatesTo("--0", 0);
  }
  
  public void testAddOperation() throws Exception {
    assertEvaluatesTo("1+1", 2);
    assertEvaluatesTo("1+0.5+0.5", 2);
    assertEvaluatesTo("5+10", 15);
    assertEvaluatesTo("1+1+2", 4);
    assertEvaluatesTo("(1+1)+2", 4);
    assertEvaluatesTo("1+(1+2)", 4);
    assertEvaluatesTo("0+1", 1);
    assertEvaluatesTo("1+0", 1);
    assertEvaluatesTo("0+0", 0);
  }
  
  public void testSubtractOperation() throws Exception {
    assertEvaluatesTo("1-1", 0);
    assertEvaluatesTo("5-10", -5);
    assertEvaluatesTo("1-0.5-0.5", 0);
    assertEvaluatesTo("1-1-2", -2);
    assertEvaluatesTo("(1-1)-2", -2);
    assertEvaluatesTo("1-(1-2)", 2);
    assertEvaluatesTo("0-1", -1);
    assertEvaluatesTo("1-0", 1);
    assertEvaluatesTo("0-0", 0);
  }
  
  public void testMultiplyOperation() throws Exception {
    assertEvaluatesTo("1*1", 1);
    assertEvaluatesTo("5*10", 50);
    assertEvaluatesTo("50*0.1", 5);
    assertEvaluatesTo("1*1*2", 2);
    assertEvaluatesTo("(1*1)*2", 2);
    assertEvaluatesTo("1*(1*2)", 2);
    assertEvaluatesTo("10*0", 0);
    assertEvaluatesTo("0*0", 0);
  }
  
  public void testDivisionOperation() throws Exception {
    assertEvaluatesTo("1*1", 1);
    assertEvaluatesTo("10/5", 2);
    assertEvaluatesTo("10/0.5", 20);
    assertEvaluatesTo("10/5/2", 1);
    assertEvaluatesTo("(27/9)/3", 1);
    assertEvaluatesTo("27/(9/3)", 9);
    assertEvaluatesTo("1/0", 9223372036854775807L);
  }
  
  public void testModuloOperation() throws Exception {
    assertEvaluatesTo("1%1", 0);
    assertEvaluatesTo("10%3", 1);
    assertEvaluatesTo("10%3%2", 1);
    assertEvaluatesTo("(27%10)%4", 3);
    assertEvaluatesTo("27%(9%5)", 3);
  }
  
  public void testLessThanOperation() throws Exception {
    assertEvaluatesTo("1 < 1", 0);
    assertEvaluatesTo("2 < 1", 0);
    assertEvaluatesTo("1 < 2", 1);
    assertEvaluatesTo("2 < 1 < 3", 1);
    assertEvaluatesTo("2 < (1 < 3)", 0);
    assertEvaluatesTo("(2 < 1) < 1", 1);
    assertEvaluatesTo("-1 < -2", 0);
    assertEvaluatesTo("-1 < 0", 1);
  }
  
  public void testLessThanEqualsOperation() throws Exception {
    assertEvaluatesTo("1 <= 1", 1);
    assertEvaluatesTo("2 <= 1", 0);
    assertEvaluatesTo("1 <= 2", 1);
    assertEvaluatesTo("1 <= 1 <= 0", 0);
    assertEvaluatesTo("-1 <= -1", 1);
    assertEvaluatesTo("-1 <= 0", 1);
    assertEvaluatesTo("-1 <= -2", 0);
    assertEvaluatesTo("-1 <= 0", 1);
  }
  
  public void testGreaterThanOperation() throws Exception {
    assertEvaluatesTo("1 > 1", 0);
    assertEvaluatesTo("2 > 1", 1);
    assertEvaluatesTo("1 > 2", 0);
    assertEvaluatesTo("2 > 1 > 3", 0);
    assertEvaluatesTo("2 > (1 > 3)", 1);
    assertEvaluatesTo("(2 > 1) > 1", 0);
    assertEvaluatesTo("-1 > -2", 1);
    assertEvaluatesTo("-1 > 0", 0);
  }
  
  public void testGreaterThanEqualsOperation() throws Exception {
    assertEvaluatesTo("1 >= 1", 1);
    assertEvaluatesTo("2 >= 1", 1);
    assertEvaluatesTo("1 >= 2", 0);
    assertEvaluatesTo("1 >= 1 >= 0", 1);
    assertEvaluatesTo("-1 >= -1", 1);
    assertEvaluatesTo("-1 >= 0", 0);
    assertEvaluatesTo("-1 >= -2", 1);
    assertEvaluatesTo("-1 >= 0", 0);
  }
  
  public void testEqualsOperation() throws Exception {
    assertEvaluatesTo("1 == 1", 1);
    assertEvaluatesTo("0 == 0", 1);
    assertEvaluatesTo("-1 == -1", 1);
    assertEvaluatesTo("1.1 == 1.1", 1);
    assertEvaluatesTo("0.9 == 0.9", 1);
    assertEvaluatesTo("-0 == 0", 1);
    assertEvaluatesTo("0 == 1", 0);
    assertEvaluatesTo("1 == 2", 0);
    assertEvaluatesTo("-1 == 1", 0);
    assertEvaluatesTo("-1 == 0", 0);
    assertEvaluatesTo("-2 == 1", 0);
    assertEvaluatesTo("-2 == -1", 0);
  }
  
  public void testNotEqualsOperation() throws Exception {
    assertEvaluatesTo("1 != 1", 0);
    assertEvaluatesTo("0 != 0", 0);
    assertEvaluatesTo("-1 != -1", 0);
    assertEvaluatesTo("1.1 != 1.1", 0);
    assertEvaluatesTo("0.9 != 0.9", 0);
    assertEvaluatesTo("-0 != 0", 0);
    assertEvaluatesTo("0 != 1", 1);
    assertEvaluatesTo("1 != 2", 1);
    assertEvaluatesTo("-1 != 1", 1);
    assertEvaluatesTo("-1 != 0", 1);
    assertEvaluatesTo("-2 != 1", 1);
    assertEvaluatesTo("-2 != -1", 1);
  }
  
  public void testBoolNotOperation() throws Exception {
    assertEvaluatesTo("!1", 0);
    assertEvaluatesTo("!!1", 1);
    assertEvaluatesTo("!0", 1);
    assertEvaluatesTo("!!0", 0);
    assertEvaluatesTo("!-1", 0);
    assertEvaluatesTo("!2", 0);
    assertEvaluatesTo("!-2", 0);
  }
  
  public void testBoolAndOperation() throws Exception {
    assertEvaluatesTo("1 && 1", 1);
    assertEvaluatesTo("1 && 0", 0);
    assertEvaluatesTo("0 && 1", 0);
    assertEvaluatesTo("0 && 0", 0);
    assertEvaluatesTo("-1 && -1", 1);
    assertEvaluatesTo("-1 && 0", 0);
    assertEvaluatesTo("0 && -1", 0);
    assertEvaluatesTo("-0 && -0", 0);
  }
  
  public void testBoolOrOperation() throws Exception {
    assertEvaluatesTo("1 || 1", 1);
    assertEvaluatesTo("1 || 0", 1);
    assertEvaluatesTo("0 || 1", 1);
    assertEvaluatesTo("0 || 0", 0);
    assertEvaluatesTo("-1 || -1", 1);
    assertEvaluatesTo("-1 || 0", 1);
    assertEvaluatesTo("0 || -1", 1);
    assertEvaluatesTo("-0 || -0", 0);
  }
  
  public void testConditionalOperation() throws Exception {
    assertEvaluatesTo("1 ? 2 : 3", 2);
    assertEvaluatesTo("-1 ? 2 : 3", 2);
    assertEvaluatesTo("0 ? 2 : 3", 3);
    assertEvaluatesTo("1 ? 2 ? 3 : 4 : 5", 3);
    assertEvaluatesTo("0 ? 2 ? 3 : 4 : 5", 5);
    assertEvaluatesTo("1 ? 0 ? 3 : 4 : 5", 4);
    assertEvaluatesTo("1 ? 2 : 3 ? 4 : 5", 2);
    assertEvaluatesTo("0 ? 2 : 3 ? 4 : 5", 4);
    assertEvaluatesTo("0 ? 2 : 0 ? 4 : 5", 5);
    assertEvaluatesTo("(1 ? 1 : 0) ? 3 : 4", 3);
    assertEvaluatesTo("(0 ? 1 : 0) ? 3 : 4", 4);
  }
  
  public void testBitShiftLeft() throws Exception {
    assertEvaluatesTo("1 << 1", 2);
    assertEvaluatesTo("2 << 1", 4);
    assertEvaluatesTo("-1 << 31", -2147483648);
    assertEvaluatesTo("3 << 5", 96);
    assertEvaluatesTo("-5 << 3", -40);
    assertEvaluatesTo("4195 << 7", 536960);
    assertEvaluatesTo("4195 << 66", 16780);
    assertEvaluatesTo("4195 << 6", 268480);
    assertEvaluatesTo("4195 << 70", 268480);
    assertEvaluatesTo("-4195 << 70", -268480);
    assertEvaluatesTo("-15 << 62", 4611686018427387904L);
  }
  
  public void testBitShiftRight() throws Exception {
    assertEvaluatesTo("1 >> 1", 0);
    assertEvaluatesTo("2 >> 1", 1);
    assertEvaluatesTo("-1 >> 5", -1);
    assertEvaluatesTo("-2 >> 30", -1);
    assertEvaluatesTo("-5 >> 1", -3);
    assertEvaluatesTo("536960 >> 7", 4195);
    assertEvaluatesTo("16780 >> 66", 4195);
    assertEvaluatesTo("268480 >> 6", 4195);
    assertEvaluatesTo("268480 >> 70", 4195);
    assertEvaluatesTo("-268480 >> 70", -4195);
    assertEvaluatesTo("-2147483646 >> 1", -1073741823);
  }
  
  public void testBitShiftRightUnsigned() throws Exception {
    assertEvaluatesTo("1 >>> 1", 0);
    assertEvaluatesTo("2 >>> 1", 1);
    assertEvaluatesTo("-1 >>> 37", 134217727);
    assertEvaluatesTo("-2 >>> 62", 3);
    assertEvaluatesTo("-5 >>> 33", 2147483647);
    assertEvaluatesTo("536960 >>> 7", 4195);
    assertEvaluatesTo("16780 >>> 66", 4195);
    assertEvaluatesTo("268480 >>> 6", 4195);
    assertEvaluatesTo("268480 >>> 70", 4195);
    assertEvaluatesTo("-268480 >>> 102", 67108863);
    assertEvaluatesTo("2147483648 >>> 1", 1073741824);
  }
  
  public void testBitwiseAnd() throws Exception {
    assertEvaluatesTo("4 & 4", 4);
    assertEvaluatesTo("3 & 2", 2);
    assertEvaluatesTo("7 & 3", 3);
    assertEvaluatesTo("-1 & -1", -1);
    assertEvaluatesTo("-1 & 25", 25);
    assertEvaluatesTo("3 & 7", 3);
    assertEvaluatesTo("0 & 1", 0);
    assertEvaluatesTo("1 & 0", 0);
  }
  
  public void testBitwiseOr() throws Exception {
    assertEvaluatesTo("4 | 4", 4);
    assertEvaluatesTo("5 | 2", 7);
    assertEvaluatesTo("7 | 3", 7);
    assertEvaluatesTo("-1 | -5", -1);
    assertEvaluatesTo("-1 | 25", -1);
    assertEvaluatesTo("-100 | 15", -97);
    assertEvaluatesTo("0 | 1", 1);
    assertEvaluatesTo("1 | 0", 1);
  }
  
  public void testBitwiseXor() throws Exception {
    assertEvaluatesTo("4 ^ 4", 0);
    assertEvaluatesTo("5 ^ 2", 7);
    assertEvaluatesTo("15 ^ 3", 12);
    assertEvaluatesTo("-1 ^ -5", 4);
    assertEvaluatesTo("-1 ^ 25", -26);
    assertEvaluatesTo("-100 ^ 15", -109);
    assertEvaluatesTo("0 ^ 1", 1);
    assertEvaluatesTo("1 ^ 0", 1);
    assertEvaluatesTo("0 ^ 0", 0);
  }
  
  public void testBitwiseNot() throws Exception {
    assertEvaluatesTo("~-5", 4);
    assertEvaluatesTo("~25", -26);
    assertEvaluatesTo("~0", -1);
    assertEvaluatesTo("~-1", 0);
  }
  
  public void testDecimalConst() throws Exception {
    assertEvaluatesTo("0", 0);
    assertEvaluatesTo("1", 1);
    assertEvaluatesTo("123456789", 123456789);
    assertEvaluatesTo("5.6E2", 560);
    assertEvaluatesTo("5.6E+2", 560);
    assertEvaluatesTo("500E-2", 5);
  }
  
  public void testHexConst() throws Exception {
    assertEvaluatesTo("0x0", 0);
    assertEvaluatesTo("0x1", 1);
    assertEvaluatesTo("0xF", 15);
    assertEvaluatesTo("0x1234ABCDEF", 78193085935L);
    assertEvaluatesTo("1 << 0x1", 1 << 0x1);
    assertEvaluatesTo("1 << 0xA", 1 << 0xA);
    assertEvaluatesTo("0x1 << 2", 0x1 << 2);
    assertEvaluatesTo("0xA << 2", 0xA << 2);
  }
  
  public void testHexConst2() throws Exception {
    assertEvaluatesTo("0X0", 0);
    assertEvaluatesTo("0X1", 1);
    assertEvaluatesTo("0XF", 15);
    assertEvaluatesTo("0X1234ABCDEF", 78193085935L);
  }
  
  public void testOctalConst() throws Exception {
    assertEvaluatesTo("00", 0);
    assertEvaluatesTo("01", 1);
    assertEvaluatesTo("010", 8);
    assertEvaluatesTo("0123456777", 21913087);
    assertEvaluatesTo("1 << 01", 1 << 01);
    assertEvaluatesTo("1 << 010", 1 << 010);
    assertEvaluatesTo("01 << 2", 01 << 2);
    assertEvaluatesTo("010 << 2", 010 << 2);

  }
}
