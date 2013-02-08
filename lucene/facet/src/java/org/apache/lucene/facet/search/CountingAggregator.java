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
 * A CountingAggregator updates a counter array with the size of the whole
 * taxonomy, counting the number of times each category appears in the given set
 * of documents.
 * 
 * @lucene.experimental
 */
public class CountingAggregator implements Aggregator {

  protected int[] counterArray;
  
  public CountingAggregator(int[] counterArray) {
    this.counterArray = counterArray;
  }
  
  @Override
  public void aggregate(int docID, float score, IntsRef ordinals) throws IOException {
    for (int i = 0; i < ordinals.length; i++) {
      counterArray[ordinals.ints[i]]++;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    CountingAggregator that = (CountingAggregator) obj;
    return that.counterArray == this.counterArray;
  }

  @Override
  public int hashCode() {
    return counterArray == null ? 0 : counterArray.hashCode();
  }
  
  @Override
  public boolean setNextReader(AtomicReaderContext context) throws IOException {
    return true;
  }
  
}
