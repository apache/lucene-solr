package org.apache.solr.search;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Gauge;

import java.lang.ref.WeakReference;
import java.util.concurrent.TimeUnit;

public abstract class SolrIndexSearcherGauge<T> implements Gauge<T> {
  private final WeakReference<SolrIndexSearcher> solrIndexSearcherRef;

  public SolrIndexSearcherGauge(SolrIndexSearcher solrIndexSearcher) {
    this.solrIndexSearcherRef = new WeakReference<>(solrIndexSearcher);
  }

  @Override public T getValue() {
    return getValue(this.solrIndexSearcherRef.get());
  }

  protected abstract T getValue(SolrIndexSearcher solrIndexSearcher);


  public abstract static class SolrIndexSearcherCachedGauge<T> extends CachedGauge<T> {
    private final WeakReference<SolrIndexSearcher> solrIndexSearcherRef;

    public SolrIndexSearcherCachedGauge(SolrIndexSearcher solrIndexSearcher, long time, TimeUnit unit) {
      super(time, unit);
      this.solrIndexSearcherRef = new WeakReference<>(solrIndexSearcher);
    }

    @Override protected T loadValue() {
      return getValue(this.solrIndexSearcherRef.get());
    }

    protected abstract T getValue(SolrIndexSearcher solrIndexSearcher);
  }

}
