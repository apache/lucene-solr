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
package org.apache.lucene.search.similarities;

import org.apache.lucene.search.Explanation;

/**
 * Model of the information gain based on Laplace's law of succession.
 *
 * @lucene.experimental
 */
public class AfterEffectL extends AfterEffect {

  /** Sole constructor: parameter-free */
  public AfterEffectL() {}

  @Override
  public final double scoreTimes1pTfn(BasicStats stats) {
    return 1.0;
  }

  @Override
  public final Explanation explain(BasicStats stats, double tfn) {
    return Explanation.match(
        (float) (scoreTimes1pTfn(stats) / (1 + tfn)),
        getClass().getSimpleName() + ", computed as 1 / (tfn + 1) from:",
        Explanation.match((float) tfn, "tfn, normalized term frequency"));
  }

  @Override
  public String toString() {
    return "L";
  }
}
