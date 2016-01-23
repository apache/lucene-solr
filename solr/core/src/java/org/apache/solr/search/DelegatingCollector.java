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

package org.apache.solr.search;


import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

import java.io.IOException;


/** A simple delegating collector where one can set the delegate after creation */
public class DelegatingCollector extends Collector {
  static int setLastDelegateCount; // for testing purposes only to determine the number of times a delegating collector chain was used

  protected Collector delegate;
  protected Scorer scorer;
  protected IndexReader.AtomicReaderContext context;
  protected int docBase;

  public Collector getDelegate() {
    return delegate;
  }

  public void setDelegate(Collector delegate) {
    this.delegate = delegate;
  }

  /** Sets the last delegate in a chain of DelegatingCollectors */
  public void setLastDelegate(Collector delegate) {
    DelegatingCollector ptr = this;
    for(; ptr.getDelegate() instanceof DelegatingCollector; ptr = (DelegatingCollector)ptr.getDelegate());
    ptr.setDelegate(delegate);
    setLastDelegateCount++;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
    delegate.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    delegate.collect(doc);
  }

  @Override
  public void setNextReader(IndexReader.AtomicReaderContext context) throws IOException {
    this.context = context;
    this.docBase = context.docBase;
    delegate.setNextReader(context);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return delegate.acceptsDocsOutOfOrder();
  }
}
