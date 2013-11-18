package org.apache.lucene.facet.simple;

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

import org.apache.lucene.util.PriorityQueue;

// nocommit make value a double and merge with TopOrdCountValueQueue?

/** Keeps highest results, first by largest float value,
 *  then tie break by smallest ord. */
class TopOrdAndFloatQueue extends PriorityQueue<TopOrdAndFloatQueue.OrdAndValue> {

  public static final class OrdAndValue {
    int ord;
    float value;
  }

  public TopOrdAndFloatQueue(int topN) {
    super(topN, false);
  }

  @Override
  protected boolean lessThan(OrdAndValue a, OrdAndValue b) {
    if (a.value < b.value) {
      return true;
    } else if (a.value > b.value) {
      return false;
    } else {
      return a.ord > b.ord;
    }
  }
}
