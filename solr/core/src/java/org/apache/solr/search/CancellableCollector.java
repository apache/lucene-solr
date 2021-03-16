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
package org.apache.solr.search;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.solr.client.solrj.util.Cancellable;

/** Allows a query to be cancelled */
public class CancellableCollector implements Collector, Cancellable {

  /** Thrown when a query gets cancelled */
  public static class QueryCancelledException extends RuntimeException {}

  private final Collector collector;
  private final AtomicBoolean isQueryCancelled;

  public CancellableCollector(Collector collector) {
    Objects.requireNonNull(collector, "Internal collector not provided but wrapper collector accessed");

    this.collector = collector;
    this.isQueryCancelled = new AtomicBoolean();
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {

    if (isQueryCancelled.get()) {
      throw new QueryCancelledException();
    }

    return new FilterLeafCollector(collector.getLeafCollector(context)) {

      @Override
      public void collect(int doc) throws IOException {
        if (isQueryCancelled.get()) {
          throw new QueryCancelledException();
        }
        in.collect(doc);
      }
    };
  }

  @Override
  public ScoreMode scoreMode() {
    return collector.scoreMode();
  }

  @Override
  public void cancel() {
    isQueryCancelled.compareAndSet(false, true);
  }

  public Collector getInternalCollector() {
    return collector;
  }
}
