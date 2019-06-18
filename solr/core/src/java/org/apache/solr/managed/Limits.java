package org.apache.solr.managed;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class Limits implements Iterable<Map.Entry<String, Limit>> {

  public static final Limit UNLIMITED = new Limit(Float.MIN_VALUE, Float.MAX_VALUE);

  private Map<String, Limit> values = new HashMap<>();

  public void setLimit(String key, Limit value) {
    if (value != null) {
      values.put(key, value);
    } else {
      values.remove(key);
    }
  }

  public void setLimitMax(String key, float max) {
    Limit limit = values.computeIfAbsent(key, k -> new Limit(Float.MIN_VALUE, max));
    if (limit.max == max) {
      return;
    } else {
      values.put(key, new Limit(limit.min, max));
    }
  }

  public void setLimitMin(String key, float min) {
    Limit limit = values.computeIfAbsent(key, k -> new Limit(min, Float.MAX_VALUE));
    if (limit.min == min) {
      return;
    } else {
      values.put(key, new Limit(min, limit.max));
    }
  }

  public Limit getLimit(String key) {
    return getLimit(key, UNLIMITED);
  }

  public Limit getLimit(String key, Limit defValue) {
    Limit value = values.get(key);
    if (value != null) {
      return value;
    } else {
      return defValue;
    }
  }

  public Set<String> getKeys() {
    return Collections.unmodifiableSet(values.keySet());
  }

  public void removeLimit(String key) {
    values.remove(key);
  }

  public Limits copy() {
    Limits cloned = new Limits();
    cloned.values.putAll(values);
    return cloned;
  }

  @Override
  public Iterator<Map.Entry<String, Limit>> iterator() {
    return Collections.unmodifiableMap(values).entrySet().iterator();
  }
}
