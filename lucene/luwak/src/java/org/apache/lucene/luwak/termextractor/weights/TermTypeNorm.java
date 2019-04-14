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

import org.apache.lucene.luwak.termextractor.QueryTerm;

/**
 * Weight a particular term type
 */
public class TermTypeNorm extends WeightNorm {

  private final float weight;
  private final String payload;
  private final QueryTerm.Type type;

  public TermTypeNorm(QueryTerm.Type type, float weight) {
    this(type, null, weight);
  }

  public TermTypeNorm(QueryTerm.Type type, String payload, float weight) {
    this.weight = weight;
    this.type = type;
    this.payload = payload;
  }

  @Override
  public float norm(QueryTerm term) {
    if (term.type == this.type &&
        (term.payload == null ? this.payload == null : term.payload.equals(this.payload)))
      return weight;
    return 1;
  }
}
