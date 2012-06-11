package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

/**
 * A {@link Collector} which allows running a search with several
 * {@link Collector}s. It offers a static {@link #wrap} method which accepts a
 * list of collectors and wraps them with {@link MultiCollector}, while
 * filtering out the <code>null</code> null ones.
 */
public class MultiCollector extends Collector {

  /**
   * Wraps a list of {@link Collector}s with a {@link MultiCollector}. This
   * method works as follows:
   * <ul>
   * <li>Filters out the <code>null</code> collectors, so they are not used
   * during search time.
   * <li>If the input contains 1 real collector (i.e. non-<code>null</code> ),
   * it is returned.
   * <li>Otherwise the method returns a {@link MultiCollector} which wraps the
   * non-<code>null</code> ones.
   * </ul>
   * 
   * @throws IllegalArgumentException
   *           if either 0 collectors were input, or all collectors are
   *           <code>null</code>.
   */
  public static Collector wrap(Collector... collectors) {
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
    } else if (n == 1) {
      // only 1 Collector - return it.
      Collector col = null;
      for (Collector c : collectors) {
        if (c != null) {
          col = c;
          break;
        }
      }
      return col;
    } else if (n == collectors.length) {
      return new MultiCollector(collectors);
    } else {
      Collector[] colls = new Collector[n];
      n = 0;
      for (Collector c : collectors) {
        if (c != null) {
          colls[n++] = c;
        }
      }
      return new MultiCollector(colls);
    }
  }
  
  private final Collector[] collectors;

  private MultiCollector(Collector... collectors) {
    this.collectors = collectors;
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
  public void setNextReader(AtomicReaderContext context) throws IOException {
    for (Collector c : collectors) {
      c.setNextReader(context);
    }
  }

  @Override
  public void setScorer(Scorer s) throws IOException {
    for (Collector c : collectors) {
      c.setScorer(s);
    }
  }

}
