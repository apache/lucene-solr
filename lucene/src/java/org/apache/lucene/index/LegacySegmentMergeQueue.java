package org.apache.lucene.index;

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

import java.io.IOException;
import org.apache.lucene.util.PriorityQueue;

final class LegacySegmentMergeQueue extends PriorityQueue<LegacySegmentMergeInfo> {
  LegacySegmentMergeQueue(int size) {
    initialize(size);
  }

  @Override
  protected final boolean lessThan(LegacySegmentMergeInfo a, LegacySegmentMergeInfo b) {
    int comparison = a.term.compareTo(b.term);
    if (comparison == 0)
      return a.base < b.base; 
    else
      return comparison < 0;
  }

  final void close() throws IOException {
    while (top() != null)
      ((LegacySegmentMergeInfo)pop()).close();
  }

}
