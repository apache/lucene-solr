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

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.search.Scorer;

/**
 * A utility class to allow expressions to access the score as a {@link FunctionValues}.
 */
class ScoreFunctionValues extends DoubleDocValues {
  final Scorer scorer;

  ScoreFunctionValues(ValueSource parent, Scorer scorer) {
    super(parent);
    this.scorer = scorer;
  }
  
  @Override
  public double doubleVal(int document) {
    try {
      assert document == scorer.docID();
      return scorer.score();
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }
}
