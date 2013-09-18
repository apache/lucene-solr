package org.apache.lucene.expressions;

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
    bindings.add(new SortField("_score", SortField.Type.LONG));
    bindings.add(new SortField("popularity", SortField.Type.INT));
    
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
}
