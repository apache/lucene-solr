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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.queries.function.ValueSource;

/**
 * Binds variable names in expressions to actual data.
 * <p>
 * These are typically DocValues fields/FieldCache, the document's 
 * relevance score, or other ValueSources.
 * 
 * @lucene.experimental
 */
public abstract class Bindings implements Iterable<String> {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected Bindings() {}
  
  /**
   * Returns a ValueSource bound to the variable name.
   */
  public abstract ValueSource getValueSource(String name);
  
  /** Returns an <code>Iterator</code> over the variable names in this binding */
  @Override
  public abstract Iterator<String> iterator();

  /** 
   * Traverses the graph of bindings, checking there are no cycles or missing references 
   * @throws IllegalArgumentException if the bindings is inconsistent 
   */
  public final void validate() {
    Set<String> marked = new HashSet<String>();
    Set<String> chain = new HashSet<String>();
    
    for (String name : this) {
      if (!marked.contains(name)) {
        chain.add(name);
        validate(name, marked, chain);
        chain.remove(name);
      }
    }
  }

  private void validate(String name, Set<String> marked, Set<String> chain) {        
    ValueSource vs = getValueSource(name);
    if (vs == null) {
      throw new IllegalArgumentException("Invalid reference '" + name + "'");
    }
    
    if (vs instanceof ExpressionValueSource) {
      Expression expr = ((ExpressionValueSource)vs).expression;
      for (String external : expr.externals) {
        if (chain.contains(external)) {
          throw new IllegalArgumentException("Recursion Error: Cycle detected originating in (" + external + ")");
        }
        if (!marked.contains(external)) {
          chain.add(external);
          validate(external, marked, chain);
          chain.remove(external);
        }
      }
    }
    
    marked.add(name);
  }
}
