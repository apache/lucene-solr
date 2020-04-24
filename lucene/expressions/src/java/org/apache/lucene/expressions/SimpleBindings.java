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


import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.SortField;

/**
 * Simple class that binds expression variable names to {@link DoubleValuesSource}s
 * or other {@link Expression}s.
 * <p>
 * Example usage:
 * <pre class="prettyprint">
 *   SimpleBindings bindings = new SimpleBindings();
 *   // document's text relevance score
 *   bindings.add("_score", DoubleValuesSource.SCORES);
 *   // integer NumericDocValues field
 *   bindings.add("popularity", DoubleValuesSource.fromIntField("popularity"));
 *   // another expression
 *   bindings.add("recency", myRecencyExpression);
 *   
 *   // create a sort field in reverse order
 *   Sort sort = new Sort(expr.getSortField(bindings, true));
 * </pre>
 * 
 * @lucene.experimental
 */
public final class SimpleBindings extends Bindings {

  private final Map<String, Function<Bindings, DoubleValuesSource>> map = new HashMap<>();

  /** Creates a new empty Bindings */
  public SimpleBindings() {}
  
  /** 
   * Adds a SortField to the bindings.
   *
   * @see DoubleValuesSource#fromIntField(String)
   * @see DoubleValuesSource#fromLongField(String)
   * @see DoubleValuesSource#fromFloatField(String)
   * @see DoubleValuesSource#fromDoubleField(String)
   * @see DoubleValuesSource#SCORES
   */
  @Deprecated
  public void add(SortField sortField) {
    map.put(sortField.getField(), bindings -> fromSortField(sortField));
  }

  /**
   * Bind a {@link DoubleValuesSource} directly to the given name.
   */
  public void add(String name, DoubleValuesSource source) { map.put(name, bindings -> source); }

  /** 
   * Adds an Expression to the bindings.
   * <p>
   * This can be used to reference expressions from other expressions. 
   */
  public void add(String name, Expression expression) {
    map.put(name, expression::getDoubleValuesSource);
  }

  private DoubleValuesSource fromSortField(SortField field) {
    switch(field.getType()) {
      case INT:
        return DoubleValuesSource.fromIntField(field.getField());
      case LONG:
        return DoubleValuesSource.fromLongField(field.getField());
      case FLOAT:
        return DoubleValuesSource.fromFloatField(field.getField());
      case DOUBLE:
        return DoubleValuesSource.fromDoubleField(field.getField());
      case SCORE:
        return DoubleValuesSource.SCORES;
      default:
        throw new UnsupportedOperationException();
    }
  }
  
  @Override
  public DoubleValuesSource getDoubleValuesSource(String name) {
    if (map.containsKey(name) == false) {
      throw new IllegalArgumentException("Invalid reference '" + name + "'");
    }
    return map.get(name).apply(this);
  }

  /**
   * Traverses the graph of bindings, checking there are no cycles or missing references
   * @throws IllegalArgumentException if the bindings is inconsistent
   */
  public void validate() {
    for (Map.Entry<String, Function<Bindings, DoubleValuesSource>> origin : map.entrySet()) {
      origin.getValue().apply(new CycleDetectionBindings(origin.getKey()));
    }
  }

  private class CycleDetectionBindings extends Bindings {

    private final Set<String> seenFields = new LinkedHashSet<>();

    CycleDetectionBindings(String current) {
      seenFields.add(current);
    }

    CycleDetectionBindings(Set<String> parents, String current) {
      seenFields.addAll(parents);
      seenFields.add(current);
    }

    @Override
    public DoubleValuesSource getDoubleValuesSource(String name) {
      if (seenFields.contains(name)) {
        throw new IllegalArgumentException("Recursion error: Cycle detected " + seenFields + "->" + name);
      }
      if (map.containsKey(name) == false) {
        throw new IllegalArgumentException("Invalid reference '" + name + "'");
      }
      return map.get(name).apply(new CycleDetectionBindings(seenFields, name));
    }
  }
}

