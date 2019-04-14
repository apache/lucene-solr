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

package org.apache.lucene.luwak.termextractor.weights;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.luwak.termextractor.QueryTerm;

/**
 * Weight a set of terms in a given field by a scale factor
 */
public class FieldSpecificTermWeightNorm extends TermWeightNorm {

  private final String field;

  /**
   * Create a new FieldSpecificTermWeightNorm
   *
   * @param k     the scale factor
   * @param field the field in which to apply the norm
   * @param terms the terms to scale
   */
  public FieldSpecificTermWeightNorm(float k, String field, Set<String> terms) {
    super(k, terms);
    this.field = field;
  }

  /**
   * Create a new FieldSpecificTermWeightNorm
   *
   * @param k     the scale factor
   * @param field the field in which to apply the norm
   * @param terms the terms to scale
   */
  public FieldSpecificTermWeightNorm(float k, String field, String... terms) {
    this(k, field, new HashSet<>(Arrays.asList(terms)));
  }

  @Override
  public float norm(QueryTerm term) {
    if (term.term.field().equals(field))
      return super.norm(term);
    return 1;
  }
}
