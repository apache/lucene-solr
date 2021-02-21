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
package org.apache.solr.core;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Gauge;

import java.lang.ref.WeakReference;
import java.util.concurrent.TimeUnit;

public abstract class SolrCoreGauge<T> implements Gauge<T> {
  private final WeakReference<SolrCore> solrCoreRef;

  public SolrCoreGauge(SolrCore solrCore) {
    this.solrCoreRef = new WeakReference<>(solrCore);
  }

  @Override public T getValue() {
    return getValue(this.solrCoreRef.get());
  }

  protected abstract T getValue(SolrCore solrCore);

  public abstract static class SolrCoreCachedGauge<T> extends CachedGauge<T> {
    private final WeakReference<SolrCore> solrCoreRef;

    public SolrCoreCachedGauge(SolrCore solrCore, long time, TimeUnit unit) {
      super(time, unit);
      this.solrCoreRef = new WeakReference<>(solrCore);
    }

    @Override protected T loadValue() {
      return getValue(this.solrCoreRef.get());
    }

    protected abstract T getValue(SolrCore solrCore);
  }
}
