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
package org.apache.solr.schema;

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.SortField;

/**
 * Custom field wrapping an int, to test sorting via a custom comparator.
 */
public class WrappedIntPointField extends IntPointField {
  /** static helper for re-use in sibling trie class */
  public static SortField getSortField(final SortField superSort, final SchemaField field) {
    field.checkSortability();
    Expression expr = null;
    try {
      expr = JavascriptCompiler.compile(field.getName() + " % 3");
    } catch (Exception e) {
      throw new RuntimeException("impossible?", e);
    }
    SimpleBindings bindings = new SimpleBindings();
    bindings.add(superSort.getField(), fromSortField(superSort));
    return expr.getSortField(bindings, superSort.getReverse());
  }

  @Override
  public SortField getSortField(final SchemaField field, final boolean reverse) {
    return getSortField(super.getSortField(field, reverse), field);
  }

  private static DoubleValuesSource fromSortField(SortField field) {
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
}
