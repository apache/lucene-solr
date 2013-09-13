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

public class TestJavascriptOperations extends LuceneTestCase {
  private void testComputedExpression(String name, String expression, long expected) throws Exception {
    Expression evaluator = JavascriptCompiler.compile(expression);
    long actual = (long)evaluator.evaluate(0, null);
    assertEquals(name, expected, actual);
  }
  
  public void testNegationOperation() throws Exception {
    testComputedExpression("negate0", "-1", -1);
    testComputedExpression("negate1", "--1", 1);
    testComputedExpression("negate1", "-(-1)", 1);
    testComputedExpression("negate2", "-0", 0);
    testComputedExpression("negate3", "--0", 0);
  }
  
  public void testAddOperation() throws Exception {
    testComputedExpression("add0", "1+1", 2);
    testComputedExpression("add1", "1+0.5+0.5", 2);
    testComputedExpression("add2", "5+10", 15);
    testComputedExpression("add3", "1+1+2", 4);
    testComputedExpression("add4", "(1+1)+2", 4);
    testComputedExpression("add5", "1+(1+2)", 4);
    testComputedExpression("add6", "0+1", 1);
    testComputedExpression("add7", "1+0", 1);
    testComputedExpression("add8", "0+0", 0);
  }
  
  public void testSubtractOperation() throws Exception {
    testComputedExpression("subtract0", "1-1", 0);
    testComputedExpression("subtract1", "5-10", -5);
    testComputedExpression("subtract2", "1-0.5-0.5", 0);
    testComputedExpression("subtract3", "1-1-2", -2);
    testComputedExpression("subtract4", "(1-1)-2", -2);
    testComputedExpression("subtract5", "1-(1-2)", 2);
    testComputedExpression("subtract6", "0-1", -1);
    testComputedExpression("subtract6", "1-0", 1);
    testComputedExpression("subtract6", "0-0", 0);
  }
  
  public void testMultiplyOperation() throws Exception {
    testComputedExpression("multiply0", "1*1", 1);
    testComputedExpression("multiply1", "5*10", 50);
    testComputedExpression("multiply2", "50*0.1", 5);
    testComputedExpression("multiply4", "1*1*2", 2);
    testComputedExpression("multiply4", "(1*1)*2", 2);
    testComputedExpression("multiply5", "1*(1*2)", 2);
    testComputedExpression("multiply6", "10*0", 0);
    testComputedExpression("multiply6", "0*0", 0);
  }
  
  public void testDivisionOperation() throws Exception {
    testComputedExpression("division0", "1*1", 1);
    testComputedExpression("division1", "10/5", 2);
    testComputedExpression("division2", "10/0.5", 20);
    testComputedExpression("division3", "10/5/2", 1);
    testComputedExpression("division4", "(27/9)/3", 1);
    testComputedExpression("division5", "27/(9/3)", 9);
    testComputedExpression("division6", "1/0", 9223372036854775807L);
  }
  
  public void testModuloOperation() throws Exception {
    testComputedExpression("modulo0", "1%1", 0);
    testComputedExpression("modulo1", "10%3", 1);
    testComputedExpression("modulo2", "10%3%2", 1);
    testComputedExpression("modulo3", "(27%10)%4", 3);
    testComputedExpression("modulo4", "27%(9%5)", 3);
  }
  
  public void testLessThanOperation() throws Exception {
    testComputedExpression("lessthan0", "1 < 1", 0);
    testComputedExpression("lessthan1", "2 < 1", 0);
    testComputedExpression("lessthan2", "1 < 2", 1);
    testComputedExpression("lessthan3", "2 < 1 < 3", 1);
    testComputedExpression("lessthan4", "2 < (1 < 3)", 0);
    testComputedExpression("lessthan5", "(2 < 1) < 1", 1);
    testComputedExpression("lessthan6", "-1 < -2", 0);
    testComputedExpression("lessthan7", "-1 < 0", 1);
  }
  
  public void testLessThanEqualsOperation() throws Exception {
    testComputedExpression("lessthanequals0", "1 <= 1", 1);
    testComputedExpression("lessthanequals1", "2 <= 1", 0);
    testComputedExpression("lessthanequals2", "1 <= 2", 1);
    testComputedExpression("lessthanequals3", "1 <= 1 <= 0", 0);
    testComputedExpression("lessthanequals4", "-1 <= -1", 1);
    testComputedExpression("lessthanequals5", "-1 <= 0", 1);
    testComputedExpression("lessthanequals6", "-1 <= -2", 0);
    testComputedExpression("lessthanequals7", "-1 <= 0", 1);
  }
  
  public void testGreaterThanOperation() throws Exception {
    testComputedExpression("greaterthan0", "1 > 1", 0);
    testComputedExpression("greaterthan1", "2 > 1", 1);
    testComputedExpression("greaterthan2", "1 > 2", 0);
    testComputedExpression("greaterthan3", "2 > 1 > 3", 0);
    testComputedExpression("greaterthan4", "2 > (1 > 3)", 1);
    testComputedExpression("greaterthan5", "(2 > 1) > 1", 0);
    testComputedExpression("greaterthan6", "-1 > -2", 1);
    testComputedExpression("greaterthan7", "-1 > 0", 0);
  }
  
  public void testGreaterThanEqualsOperation() throws Exception {
    testComputedExpression("greaterthanequals0", "1 >= 1", 1);
    testComputedExpression("greaterthanequals1", "2 >= 1", 1);
    testComputedExpression("greaterthanequals2", "1 >= 2", 0);
    testComputedExpression("greaterthanequals3", "1 >= 1 >= 0", 1);
    testComputedExpression("greaterthanequals4", "-1 >= -1", 1);
    testComputedExpression("greaterthanequals5", "-1 >= 0", 0);
    testComputedExpression("greaterthanequals6", "-1 >= -2", 1);
    testComputedExpression("greaterthanequals7", "-1 >= 0", 0);
  }
  
  public void testEqualsOperation() throws Exception {
    testComputedExpression("equals0", "1 == 1", 1);
    testComputedExpression("equals1", "0 == 0", 1);
    testComputedExpression("equals2", "-1 == -1", 1);
    testComputedExpression("equals3", "1.1 == 1.1", 1);
    testComputedExpression("equals4", "0.9 == 0.9", 1);
    testComputedExpression("equals5", "-0 == 0", 1);
    testComputedExpression("equals6", "0 == 1", 0);
    testComputedExpression("equals7", "1 == 2", 0);
    testComputedExpression("equals8", "-1 == 1", 0);
    testComputedExpression("equals9", "-1 == 0", 0);
    testComputedExpression("equals10", "-2 == 1", 0);
    testComputedExpression("equals11", "-2 == -1", 0);
  }
  
  public void testNotEqualsOperation() throws Exception {
    testComputedExpression("notequals0", "1 != 1", 0);
    testComputedExpression("notequals1", "0 != 0", 0);
    testComputedExpression("notequals2", "-1 != -1", 0);
    testComputedExpression("notequals3", "1.1 != 1.1", 0);
    testComputedExpression("notequals4", "0.9 != 0.9", 0);
    testComputedExpression("notequals5", "-0 != 0", 0);
    testComputedExpression("notequals6", "0 != 1", 1);
    testComputedExpression("notequals7", "1 != 2", 1);
    testComputedExpression("notequals8", "-1 != 1", 1);
    testComputedExpression("notequals9", "-1 != 0", 1);
    testComputedExpression("notequals10", "-2 != 1", 1);
    testComputedExpression("notequals11", "-2 != -1", 1);
  }
  
  public void testBoolNotOperation() throws Exception {
    testComputedExpression("boolnot0", "!1", 0);
    testComputedExpression("boolnot0", "!!1", 1);
    testComputedExpression("boolnot0", "!0", 1);
    testComputedExpression("boolnot0", "!!0", 0);
    testComputedExpression("boolnot0", "!-1", 0);
    testComputedExpression("boolnot0", "!2", 0);
    testComputedExpression("boolnot0", "!-2", 0);
  }
  
  public void testBoolAndOperation() throws Exception {
    testComputedExpression("booland0", "1 && 1", 1);
    testComputedExpression("booland1", "1 && 0", 0);
    testComputedExpression("booland2", "0 && 1", 0);
    testComputedExpression("booland3", "0 && 0", 0);
    testComputedExpression("booland4", "-1 && -1", 1);
    testComputedExpression("booland5", "-1 && 0", 0);
    testComputedExpression("booland6", "0 && -1", 0);
    testComputedExpression("booland7", "-0 && -0", 0);
  }
  
  public void testBoolOrOperation() throws Exception {
    testComputedExpression("boolor0", "1 || 1", 1);
    testComputedExpression("boolor1", "1 || 0", 1);
    testComputedExpression("boolor2", "0 || 1", 1);
    testComputedExpression("boolor3", "0 || 0", 0);
    testComputedExpression("boolor4", "-1 || -1", 1);
    testComputedExpression("boolor5", "-1 || 0", 1);
    testComputedExpression("boolor6", "0 || -1", 1);
    testComputedExpression("boolor7", "-0 || -0", 0);
  }
  
  public void testConditionalOperation() throws Exception {
    testComputedExpression("conditional0", "1 ? 2 : 3", 2);
    testComputedExpression("conditional1", "-1 ? 2 : 3", 2);
    testComputedExpression("conditional2", "0 ? 2 : 3", 3);
    testComputedExpression("conditional3", "1 ? 2 ? 3 : 4 : 5", 3);
    testComputedExpression("conditional4", "0 ? 2 ? 3 : 4 : 5", 5);
    testComputedExpression("conditional5", "1 ? 0 ? 3 : 4 : 5", 4);
    testComputedExpression("conditional6", "1 ? 2 : 3 ? 4 : 5", 2);
    testComputedExpression("conditional7", "0 ? 2 : 3 ? 4 : 5", 4);
    testComputedExpression("conditional8", "0 ? 2 : 0 ? 4 : 5", 5);
    testComputedExpression("conditional9", "(1 ? 1 : 0) ? 3 : 4", 3);
    testComputedExpression("conditional10", "(0 ? 1 : 0) ? 3 : 4", 4);
  }
  
  public void testBitShiftLeft() throws Exception {
    testComputedExpression("bitshiftleft0", "1 << 1", 2);
    testComputedExpression("bitshiftleft1", "2 << 1", 4);
    testComputedExpression("bitshiftleft2", "-1 << 31", -2147483648);
    testComputedExpression("bitshiftleft3", "3 << 5", 96);
    testComputedExpression("bitshiftleft4", "-5 << 3", -40);
    testComputedExpression("bitshiftleft5", "4195 << 7", 536960);
    testComputedExpression("bitshiftleft6", "4195 << 66", 16780);
    testComputedExpression("bitshiftleft7", "4195 << 6", 268480);
    testComputedExpression("bitshiftleft8", "4195 << 70", 268480);
    testComputedExpression("bitshiftleft9", "-4195 << 70", -268480);
    testComputedExpression("bitshiftleft10", "-15 << 62", 4611686018427387904L);
  }
  
  public void testBitShiftRight() throws Exception {
    testComputedExpression("bitshiftright0", "1 >> 1", 0);
    testComputedExpression("bitshiftright1", "2 >> 1", 1);
    testComputedExpression("bitshiftright2", "-1 >> 5", -1);
    testComputedExpression("bitshiftright3", "-2 >> 30", -1);
    testComputedExpression("bitshiftright4", "-5 >> 1", -3);
    testComputedExpression("bitshiftright5", "536960 >> 7", 4195);
    testComputedExpression("bitshiftright6", "16780 >> 66", 4195);
    testComputedExpression("bitshiftright7", "268480 >> 6", 4195);
    testComputedExpression("bitshiftright8", "268480 >> 70", 4195);
    testComputedExpression("bitshiftright9", "-268480 >> 70", -4195);
    testComputedExpression("bitshiftright10", "-2147483646 >> 1", -1073741823);
  }
  
  public void testBitShiftRightUnsigned() throws Exception {
    testComputedExpression("bitshiftrightunsigned0", "1 >>> 1", 0);
    testComputedExpression("bitshiftrightunsigned1", "2 >>> 1", 1);
    testComputedExpression("bitshiftrightunsigned2", "-1 >>> 37", 134217727);
    testComputedExpression("bitshiftrightunsigned3", "-2 >>> 62", 3);
    testComputedExpression("bitshiftrightunsigned4", "-5 >>> 33", 2147483647);
    testComputedExpression("bitshiftrightunsigned5", "536960 >>> 7", 4195);
    testComputedExpression("bitshiftrightunsigned6", "16780 >>> 66", 4195);
    testComputedExpression("bitshiftrightunsigned7", "268480 >>> 6", 4195);
    testComputedExpression("bitshiftrightunsigned8", "268480 >>> 70", 4195);
    testComputedExpression("bitshiftrightunsigned9", "-268480 >>> 102", 67108863);
    testComputedExpression("bitshiftrightunsigned10", "2147483648 >>> 1", 1073741824);
  }
  
  public void testBitwiseAnd() throws Exception {
    testComputedExpression("bitwiseand0", "4 & 4", 4);
    testComputedExpression("bitwiseand1", "3 & 2", 2);
    testComputedExpression("bitwiseand2", "7 & 3", 3);
    testComputedExpression("bitwiseand3", "-1 & -1", -1);
    testComputedExpression("bitwiseand4", "-1 & 25", 25);
    testComputedExpression("bitwiseand5", "3 & 7", 3);
    testComputedExpression("bitwiseand6", "0 & 1", 0);
    testComputedExpression("bitwiseand7", "1 & 0", 0);
  }
  
  public void testBitwiseOr() throws Exception {
    testComputedExpression("bitwiseor0", "4 | 4", 4);
    testComputedExpression("bitwiseor1", "5 | 2", 7);
    testComputedExpression("bitwiseor2", "7 | 3", 7);
    testComputedExpression("bitwiseor3", "-1 | -5", -1);
    testComputedExpression("bitwiseor4", "-1 | 25", -1);
    testComputedExpression("bitwiseor5", "-100 | 15", -97);
    testComputedExpression("bitwiseor6", "0 | 1", 1);
    testComputedExpression("bitwiseor7", "1 | 0", 1);
  }
  
  public void testBitwiseXor() throws Exception {
    testComputedExpression("bitwisexor0", "4 ^ 4", 0);
    testComputedExpression("bitwisexor1", "5 ^ 2", 7);
    testComputedExpression("bitwisexor2", "15 ^ 3", 12);
    testComputedExpression("bitwisexor3", "-1 ^ -5", 4);
    testComputedExpression("bitwisexor4", "-1 ^ 25", -26);
    testComputedExpression("bitwisexor5", "-100 ^ 15", -109);
    testComputedExpression("bitwisexor6", "0 ^ 1", 1);
    testComputedExpression("bitwisexor7", "1 ^ 0", 1);
    testComputedExpression("bitwisexor8", "0 ^ 0", 0);
  }
  
  public void testBitwiseNot() throws Exception {
    testComputedExpression("bitwisenot0", "~-5", 4);
    testComputedExpression("bitwisenot1", "~25", -26);
    testComputedExpression("bitwisenot2", "~0", -1);
    testComputedExpression("bitwisenot3", "~-1", 0);
  }
  
  public void testDecimalConst() throws Exception {
    testComputedExpression("decimalconst0", "0", 0);
    testComputedExpression("decimalconst1", "1", 1);
    testComputedExpression("decimalconst2", "123456789", 123456789);
    testComputedExpression("decimalconst3", "5.6E2", 560);
  }
  
  public void testHexConst() throws Exception {
    testComputedExpression("hexconst0", "0x0", 0);
    testComputedExpression("hexconst1", "0x1", 1);
    testComputedExpression("hexconst2", "0xF", 15);
    testComputedExpression("hexconst3", "0x1234ABCDEF", 78193085935L);
  }
  
  public void testOctalConst() throws Exception {
    testComputedExpression("octalconst0", "00", 0);
    testComputedExpression("octalconst1", "01", 1);
    testComputedExpression("octalconst2", "010", 8);
    testComputedExpression("octalconst3", "0123456777", 21913087);
  }
}
