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
