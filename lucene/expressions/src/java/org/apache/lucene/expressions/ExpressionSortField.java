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

import java.io.IOException;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;

/** A {@link SortField} which sorts documents by the evaluated value of an expression for each document */
class ExpressionSortField extends SortField {
  private final ValueSource source;

  ExpressionSortField(String name, ValueSource source, boolean reverse) {
    super(name, Type.CUSTOM, reverse);
    this.source = source;
  }
  
  @Override
  public FieldComparator<?> getComparator(final int numHits, final int sortPos) throws IOException {
    return new ExpressionComparator(source, numHits);
  }

  @Override
  public boolean needsScores() {
    return true; // TODO: maybe we can optimize by "figuring this out" somehow...
  }
}