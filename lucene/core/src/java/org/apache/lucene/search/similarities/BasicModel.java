package org.apache.lucene.search.similarities;

/**
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

import org.apache.lucene.search.Explanation;

/**
 * This class acts as the base class for the specific <em>basic model</em>
 * implementations in the DFR framework. Basic models compute the
 * <em>informative content Inf<sub>1</sub> = -log<sub>2</sub>Prob<sub>1</sub>
 * </em>.
 * 
 * @see DFRSimilarity
 * @lucene.experimental
 */
public abstract class BasicModel {
  /** Returns the informative content score. */
  public abstract float score(BasicStats stats, float tfn);
  
  /**
   * Returns an explanation for the score.
   * <p>Most basic models use the number of documents and the total term
   * frequency to compute Inf<sub>1</sub>. This method provides a generic
   * explanation for such models. Subclasses that use other statistics must
   * override this method.</p>
   */
  public Explanation explain(BasicStats stats, float tfn) {
    Explanation result = new Explanation();
    result.setDescription(getClass().getSimpleName() + ", computed from: ");
    result.setValue(score(stats, tfn));
    result.addDetail(new Explanation(tfn, "tfn"));
    result.addDetail(
        new Explanation(stats.getNumberOfDocuments(), "numberOfDocuments"));
    result.addDetail(
        new Explanation(stats.getTotalTermFreq(), "totalTermFreq"));
    return result;
  }
  
  /**
   * Subclasses must override this method to return the code of the
   * basic model formula. Refer to the original paper for the list. 
   */
  @Override
  public abstract String toString();
}
