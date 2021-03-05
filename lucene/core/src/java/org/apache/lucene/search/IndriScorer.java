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
package org.apache.lucene.search;

import java.io.IOException;

/**
 * The Indri parent scorer that stores the boost so that IndriScorers can use the boost outside of
 * the term.
 */
public abstract class IndriScorer extends Scorer {

  private float boost;

  protected IndriScorer(Weight weight, float boost) {
    super(weight);
    this.boost = boost;
  }

  @Override
  public abstract DocIdSetIterator iterator();

  @Override
  public abstract float getMaxScore(int upTo) throws IOException;

  @Override
  public abstract float score() throws IOException;

  public abstract float smoothingScore(int docId) throws IOException;

  @Override
  public abstract int docID();

  public float getBoost() {
    return this.boost;
  }
}
