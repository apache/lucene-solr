package org.apache.solr.handler.dataimport;

import static org.hamcrest.CoreMatchers.nullValue;

import java.util.IdentityHashMap;
import java.util.Map;

import org.junit.Assert;

public class DestroyCountCache extends SortedMapBackedCache {
  static Map<DIHCache,DIHCache> destroyed = new IdentityHashMap<DIHCache,DIHCache>();
  
  @Override
  public void destroy() {
    super.destroy();
    Assert.assertThat(destroyed.put(this, this), nullValue());
  }
  
  public DestroyCountCache() {}
  
}