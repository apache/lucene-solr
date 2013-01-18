package org.apache.lucene.search.spell;
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

import java.util.Comparator;


/**
 * Score first, then frequency
 *
 **/
public class SuggestWordScoreComparator implements Comparator<SuggestWord> {
  
  /**
   * Creates a new comparator that will compare by {@link SuggestWord#score},
   * then by {@link SuggestWord#freq}, then by {@link SuggestWord#string}.
   */
  public SuggestWordScoreComparator() {}

  @Override
  public int compare(SuggestWord first, SuggestWord second) {
    // first criteria: the distance
    if (first.score > second.score) {
      return 1;
    }
    if (first.score < second.score) {
      return -1;
    }

    // second criteria (if first criteria is equal): the popularity
    if (first.freq > second.freq) {
      return 1;
    }

    if (first.freq < second.freq) {
      return -1;
    }
    // third criteria: term text
    return second.string.compareTo(first.string);
  }
}
