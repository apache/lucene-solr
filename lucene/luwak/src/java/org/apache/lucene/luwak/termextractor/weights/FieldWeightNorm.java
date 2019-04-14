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
 * Weight a set of fields by a scale factor
 */
public class FieldWeightNorm extends WeightNorm {

  private final Set<String> fields;
  private final float k;

  /**
   * Create a new FieldWeightNorm
   *
   * @param k      the scale factor
   * @param fields the fields to scale
   */
  public FieldWeightNorm(float k, Set<String> fields) {
    this.fields = fields;
    this.k = k;
  }

  /**
   * Create a new FieldWeightNorm
   *
   * @param k      the scale factor
   * @param fields the fields to scale
   */
  public FieldWeightNorm(float k, String... fields) {
    this(k, new HashSet<>(Arrays.asList(fields)));
  }

  @Override
  public float norm(QueryTerm term) {
    if (this.fields.contains(term.term.field()))
      return k;
    return 1;
  }
}
