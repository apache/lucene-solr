package org.apache.lucene.search.spans;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

class SpanQueue extends PriorityQueue {
  public SpanQueue(int size) {
    initialize(size);
  }

  protected final boolean lessThan(Object o1, Object o2) {
    Spans spans1 = (Spans)o1;
    Spans spans2 = (Spans)o2;
    if (spans1.doc() == spans2.doc()) {
      if (spans1.start() == spans2.start()) {
        return spans1.end() < spans2.end();
      } else {
        return spans1.start() < spans2.start();
      }
    } else {
      return spans1.doc() < spans2.doc();
    }
  }
}
