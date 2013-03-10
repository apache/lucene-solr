package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.IntsRef;

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

/**
 * An {@link Aggregator} which updates the weight of a category according to the
 * scores of the documents it was found in.
 * 
 * @lucene.experimental
 */
public class ScoringAggregator implements Aggregator {

  private final float[] scoreArray;
  private final int hashCode;
  
  public ScoringAggregator(float[] counterArray) {
    this.scoreArray = counterArray;
    this.hashCode = scoreArray == null ? 0 : scoreArray.hashCode();
  }

  @Override
  public void aggregate(int docID, float score, IntsRef ordinals) throws IOException {
    for (int i = 0; i < ordinals.length; i++) {
      scoreArray[ordinals.ints[i]] += score;
    }
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    ScoringAggregator that = (ScoringAggregator) obj;
    return that.scoreArray == this.scoreArray;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean setNextReader(AtomicReaderContext context) throws IOException {
    return true;
  }
  
}
