package org.apache.lucene.facet.complements;

import java.io.IOException;

import org.apache.lucene.facet.search.CountingAggregator;
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
 * A {@link CountingAggregator} used during complement counting.
 * 
 * @lucene.experimental
 */
public class ComplementCountingAggregator extends CountingAggregator {

  public ComplementCountingAggregator(int[] counterArray) {
    super(counterArray);
  }

  @Override
  public void aggregate(int docID, float score, IntsRef ordinals) throws IOException {
    for (int i = 0; i < ordinals.length; i++) {
      int ord = ordinals.ints[i];
      assert counterArray[ord] != 0 : "complement aggregation: count is about to become negative for ordinal " + ord;
      --counterArray[ord];
    }
  }

}
