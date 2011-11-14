package org.apache.solr.handler.dataimport;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class DIHWriterBase implements DIHWriter {
  protected String keyFieldName;
  protected Set<Object> deltaKeys = null;
  
  @Override
  public void setDeltaKeys(Set<Map<String,Object>> passedInDeltaKeys) {
    deltaKeys = new HashSet<Object>();
    for (Map<String,Object> aMap : passedInDeltaKeys) {
      if (aMap.size() > 0) {
        Object key = null;
        if (keyFieldName != null) {
          key = aMap.get(keyFieldName);
        } else {
          key = aMap.entrySet().iterator().next();
        }
        if (key != null) {
          deltaKeys.add(key);
        }
      }
    }
  }
}
