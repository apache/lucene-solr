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
package org.apache.lucene.expressions;


import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.LuceneTestCase;

public class TestExpressionSortField extends LuceneTestCase {
  
  public void testToString() throws Exception {
    Expression expr = JavascriptCompiler.compile("sqrt(_score) + ln(popularity)");
    
    SimpleBindings bindings = new SimpleBindings();    
    bindings.add(new SortField("_score", SortField.Type.SCORE));
    bindings.add(new SortField("popularity", SortField.Type.INT));
    
    SortField sf = expr.getSortField(bindings, true);
    assertEquals("<expr \"sqrt(_score) + ln(popularity)\">!", sf.toString());
  }
  
  public void testEquals() throws Exception {
    Expression expr = JavascriptCompiler.compile("sqrt(_score) + ln(popularity)");
    
    SimpleBindings bindings = new SimpleBindings();    
    bindings.add(new SortField("_score", SortField.Type.SCORE));
    bindings.add(new SortField("popularity", SortField.Type.INT));
    
    SimpleBindings otherBindings = new SimpleBindings();
    otherBindings.add(new SortField("_score", SortField.Type.LONG));
    otherBindings.add(new SortField("popularity", SortField.Type.INT));
    
    SortField sf1 = expr.getSortField(bindings, true);
    
    // different order
    SortField sf2 = expr.getSortField(bindings, false);
    assertFalse(sf1.equals(sf2));
    
    // different bindings
    sf2 = expr.getSortField(otherBindings, true);
    assertFalse(sf1.equals(sf2));
    
    // different expression
    Expression other = JavascriptCompiler.compile("popularity/2");
    sf2 = other.getSortField(bindings, true);
    assertFalse(sf1.equals(sf2));
    
    // null
    assertFalse(sf1.equals(null));
    
    // same instance:
    assertEquals(sf1, sf1);
  }
  
  public void testNeedsScores() throws Exception {
    SimpleBindings bindings = new SimpleBindings();
    // refers to score directly
    Expression exprA = JavascriptCompiler.compile("_score");
    // constant
    Expression exprB = JavascriptCompiler.compile("0");
    // field
    Expression exprC = JavascriptCompiler.compile("intfield");
    
    // score + constant
    Expression exprD = JavascriptCompiler.compile("_score + 0");
    // field + constant
    Expression exprE = JavascriptCompiler.compile("intfield + 0");
    
    // expression + constant (score ref'd)
    Expression exprF = JavascriptCompiler.compile("a + 0");
    // expression + constant
    Expression exprG = JavascriptCompiler.compile("e + 0");
    
    // several variables (score ref'd)
    Expression exprH = JavascriptCompiler.compile("b / c + e * g - sqrt(f)");
    // several variables
    Expression exprI = JavascriptCompiler.compile("b / c + e * g");
    
    bindings.add(new SortField("_score", SortField.Type.SCORE));
    bindings.add(new SortField("intfield", SortField.Type.INT));
    bindings.add("a", exprA);
    bindings.add("b", exprB);
    bindings.add("c", exprC);
    bindings.add("d", exprD);
    bindings.add("e", exprE);
    bindings.add("f", exprF);
    bindings.add("g", exprG);
    bindings.add("h", exprH);
    bindings.add("i", exprI);
    
    assertTrue(exprA.getSortField(bindings, true).needsScores());
    assertFalse(exprB.getSortField(bindings, true).needsScores());
    assertFalse(exprC.getSortField(bindings, true).needsScores());
    assertTrue(exprD.getSortField(bindings, true).needsScores());
    assertFalse(exprE.getSortField(bindings, true).needsScores());
    assertTrue(exprF.getSortField(bindings, true).needsScores());
    assertFalse(exprG.getSortField(bindings, true).needsScores());
    assertTrue(exprH.getSortField(bindings, true).needsScores());
    assertFalse(exprI.getSortField(bindings, false).needsScores());
  }
}
