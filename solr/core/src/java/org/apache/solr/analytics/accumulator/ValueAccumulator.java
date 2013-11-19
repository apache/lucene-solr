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

package org.apache.solr.analytics.accumulator;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.solr.common.util.NamedList;

/**
 * Abstract Collector that manages all StatsCollectors, Expressions and Facets.
 */
public abstract class ValueAccumulator extends Collector {

  /**
   * @param context The context to read documents from.
   * @throws IOException if setting next reader fails
   */
  public abstract void setNextReader(AtomicReaderContext context) throws IOException;
  
  /**
   * Finalizes the statistics within each StatsCollector.
   * Must be called before <code>export()</code>.
   */
  public abstract void compute();
  public abstract NamedList<?> export();
  
  public void postProcess() throws IOException {
    // NOP
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return true;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    // NOP
  }
  
}
