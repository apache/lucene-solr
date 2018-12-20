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
package org.apache.lucene.search;


import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;

/**
 * Base {@link Collector} implementation that is used to collect all contexts.
 *
 * @lucene.experimental
 */
public abstract class SimpleCollector implements Collector, LeafCollector {

  @Override
  public final LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    doSetNextReader(context);
    return this;
  }

  /** This method is called before collecting <code>context</code>. */
  protected void doSetNextReader(LeafReaderContext context) throws IOException {}

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    // no-op by default
  }

  // redeclare methods so that javadocs are inherited on sub-classes

  @Override
  public abstract void collect(int doc) throws IOException;

}
