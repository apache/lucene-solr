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

import java.io.IOException;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;

/** A {@link SortField} which sorts documents by the evaluated value of an expression for each document */
class ExpressionSortField extends SortField {
  private final ExpressionValueSource source;

  ExpressionSortField(String name, ExpressionValueSource source, boolean reverse) {
    super(name, Type.CUSTOM, reverse);
    this.source = source;
  }
  
  @Override
  public FieldComparator<?> getComparator(final int numHits, final int sortPos) throws IOException {
    return new ExpressionComparator(source, numHits);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((source == null) ? 0 : source.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    ExpressionSortField other = (ExpressionSortField) obj;
    if (source == null) {
      if (other.source != null) return false;
    } else if (!source.equals(other.source)) return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    
    buffer.append("<expr \"");
    buffer.append(getField());
    buffer.append("\">");
    
    if (getReverse()) {
      buffer.append('!');
    }

    return buffer.toString();
  }

  @Override
  public boolean needsScores() {
    return source.needsScores();
  }
}