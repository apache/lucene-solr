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
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.util.LuceneTestCase;

/** Tests validation of bindings */
public class TestExpressionValidation extends LuceneTestCase {

  public void testValidExternals() throws Exception {
    SimpleBindings bindings = new SimpleBindings();
    bindings.add("valid0", DoubleValuesSource.fromIntField("valid0"));
    bindings.add("valid1", DoubleValuesSource.fromIntField("valid1"));
    bindings.add("valid2", DoubleValuesSource.fromIntField("valid2"));
    bindings.add("_score", DoubleValuesSource.SCORES);
    bindings.add("valide0", JavascriptCompiler.compile("valid0 - valid1 + valid2 + _score"));
    bindings.validate();
    bindings.add("valide1", JavascriptCompiler.compile("valide0 + valid0"));
    bindings.validate();
    bindings.add("valide2", JavascriptCompiler.compile("valide0 * valide1"));
    bindings.validate();
  }
  
  public void testInvalidExternal() throws Exception {
    SimpleBindings bindings = new SimpleBindings();
    bindings.add("valid", DoubleValuesSource.fromIntField("valid"));
    bindings.add("invalid", JavascriptCompiler.compile("badreference"));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      bindings.validate();
    });
    assertTrue(expected.getMessage().contains("Invalid reference"));
  }
  
  public void testInvalidExternal2() throws Exception {
    SimpleBindings bindings = new SimpleBindings();
    bindings.add("valid", DoubleValuesSource.fromIntField("valid"));
    bindings.add("invalid", JavascriptCompiler.compile("valid + badreference"));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      bindings.validate();
    });
    assertTrue(expected.getMessage().contains("Invalid reference"));
  }
  
  public void testSelfRecursion() throws Exception {
    SimpleBindings bindings = new SimpleBindings();
    bindings.add("cycle0", JavascriptCompiler.compile("cycle0"));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      bindings.validate();
    });
    assertTrue(expected.getMessage().contains("Cycle detected"));
  }
  
  public void testCoRecursion() throws Exception {
    SimpleBindings bindings = new SimpleBindings();
    bindings.add("cycle0", JavascriptCompiler.compile("cycle1"));
    bindings.add("cycle1", JavascriptCompiler.compile("cycle0"));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      bindings.validate();
    });
    assertTrue(expected.getMessage().contains("Cycle detected"));
  }
  
  public void testCoRecursion2() throws Exception {
    SimpleBindings bindings = new SimpleBindings();
    bindings.add("cycle0", JavascriptCompiler.compile("cycle1"));
    bindings.add("cycle1", JavascriptCompiler.compile("cycle2"));
    bindings.add("cycle2", JavascriptCompiler.compile("cycle0"));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      bindings.validate();
    });
    assertTrue(expected.getMessage().contains("Cycle detected"));
  }
  
  public void testCoRecursion3() throws Exception {
    SimpleBindings bindings = new SimpleBindings();
    bindings.add("cycle0", JavascriptCompiler.compile("100"));
    bindings.add("cycle1", JavascriptCompiler.compile("cycle0 + cycle2"));
    bindings.add("cycle2", JavascriptCompiler.compile("cycle0 + cycle1"));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      bindings.validate();
    });
    assertTrue(expected.getMessage().contains("Cycle detected"));
  }
  
  public void testCoRecursion4() throws Exception {
    SimpleBindings bindings = new SimpleBindings();
    bindings.add("cycle0", JavascriptCompiler.compile("100"));
    bindings.add("cycle1", JavascriptCompiler.compile("100"));
    bindings.add("cycle2", JavascriptCompiler.compile("cycle1 + cycle0 + cycle3"));
    bindings.add("cycle3", JavascriptCompiler.compile("cycle0 + cycle1 + cycle2"));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      bindings.validate();
    });
    assertTrue(expected.getMessage().contains("Cycle detected"));
  }
}
