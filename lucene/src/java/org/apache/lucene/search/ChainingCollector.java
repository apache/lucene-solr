package org.apache.lucene.search;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

/**
 * A {@link Collector} which allows chaining several {@link Collector}s in order
 * to process the matching documents emitted to {@link #collect(int)}. This
 * collector accepts a list of {@link Collector}s in its constructor, allowing
 * for some of them to be <code>null</code>. It optimizes away those
 * <code>null</code> collectors, so that they are not acessed during collection
 * time.
 * <p>
 * <b>NOTE:</b> if all the collectors passed to the constructor are null, then
 * {@link IllegalArgumentException} is thrown - it is useless to run the search
 * with 0 collectors.
 */
public class ChainingCollector extends Collector {

  private final Collector[] collectors;

  public ChainingCollector(Collector... collectors) {
    // For the user's convenience, we allow null collectors to be passed.
    // However, to improve performance, these null collectors are found
    // and dropped from the array we save for actual collection time.
    int n = 0;
    for (Collector c : collectors) {
      if (c != null) {
        n++;
      }
    }

    if (n == 0) {
      throw new IllegalArgumentException("At least 1 collector must not be null");
    } else if (n == collectors.length) {
      // No null collectors, can use the given list as is.
      this.collectors = collectors;
    } else {
      this.collectors = new Collector[n];
      n = 0;
      for (Collector c : collectors) {
        if (c != null) {
          this.collectors[n++] = c;
        }
      }
    }
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    for (Collector c : collectors) {
      if (!c.acceptsDocsOutOfOrder()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void collect(int doc) throws IOException {
    for (Collector c : collectors) {
      c.collect(doc);
    }
  }

  @Override
  public void setNextReader(IndexReader reader, int o) throws IOException {
    for (Collector c : collectors) {
      c.setNextReader(reader, o);
    }
  }

  @Override
  public void setScorer(Scorer s) throws IOException {
    for (Collector c : collectors) {
      c.setScorer(s);
    }
  }

}
